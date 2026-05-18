import os
import json
import logging
from pyflink.common import WatermarkStrategy, Types, Time
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings
from pyflink.datastream.functions import ProcessFunction
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema

from fraud_pipeline import PipelineConfig, RuleEngine
from fraud_pipeline.models import TransactionEvent

# --- CONFIG ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

def run_flink_production_job():
    # 1. Environment Setup
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    # Enable checkpointing for production reliability
    env.enable_checkpointing(30000) 
    
    t_env = StreamTableEnvironment.create(env, environment_settings=EnvironmentSettings.in_streaming_mode())
    
    # 2. Define Kafka Tables via DDL
    # Source: Transactions
    t_env.execute_sql(f"""
        CREATE TABLE tx_source (
            event_id STRING,
            txn_type STRING,
            amount DOUBLE,
            name_orig STRING,
            name_dest STRING,
            event_time TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'transaction_topic',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'flink-prod-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)

    # Source: Sender State
    t_env.execute_sql(f"""
        CREATE TABLE sender_source (
            event_id STRING,
            oldbalance_org DOUBLE,
            newbalance_orig DOUBLE,
            event_time TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sender_state_topic',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'format' = 'json'
        )
    """)

    # Source: Receiver State
    t_env.execute_sql(f"""
        CREATE TABLE receiver_source (
            event_id STRING,
            oldbalance_dest DOUBLE,
            newbalance_dest DOUBLE,
            event_time TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'receiver_state_topic',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'format' = 'json'
        )
    """)

    # Sink: Window Metrics
    t_env.execute_sql(f"""
        CREATE TABLE window_metrics_sink (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            total_amount DOUBLE,
            tx_count BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'metrics_windowed',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'format' = 'json'
        )
    """)

    # 3. Create StatementSet to run multiple sinks in ONE job
    statement_set = t_env.create_statement_set()

    # Flow A: Window Aggregation (5-min Tumbling)
    statement_set.add_insert_sql("""
        INSERT INTO window_metrics_sink
        SELECT 
            TUMBLE_START(event_time, INTERVAL '5' MINUTE),
            TUMBLE_END(event_time, INTERVAL '5' MINUTE),
            SUM(amount),
            COUNT(*)
        FROM tx_source
        GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE)
    """)

    # Flow B: 3-Way Join & Python Scoring
    joined_table = t_env.sql_query("""
        SELECT 
            T.event_id, T.event_time, T.txn_type, T.amount, T.name_orig, T.name_dest,
            S.oldbalance_org, S.newbalance_orig,
            R.oldbalance_dest, R.newbalance_dest
        FROM tx_source T
        JOIN sender_source S ON T.event_id = S.event_id 
            AND S.event_time BETWEEN T.event_time - INTERVAL '10' SECOND AND T.event_time + INTERVAL '10' SECOND
        JOIN receiver_source R ON T.event_id = R.event_id
            AND R.event_time BETWEEN T.event_time - INTERVAL '10' SECOND AND T.event_time + INTERVAL '10' SECOND
    """)

    # Convert to DataStream for complex Python rules
    joined_stream = t_env.to_append_stream(joined_table, Types.ROW([
        Types.STRING(), Types.SQL_TIMESTAMP(), Types.STRING(), Types.DOUBLE(), 
        Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.DOUBLE(), 
        Types.DOUBLE(), Types.DOUBLE()
    ]))

    class ProductionScorer(ProcessFunction):
        def open(self, ctx):
            import redis
            from cassandra.cluster import Cluster
            from fraud_pipeline import PipelineConfig, RuleEngine
            self.config = PipelineConfig()
            self.engine = RuleEngine(self.config)
            self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            self.session = Cluster([CASSANDRA_HOST]).connect("fraud_detection")

        def process_element(self, row, ctx):
            data = {
                "event_id": row[0], "event_time": str(row[1]), "txn_type": row[2],
                "amount": row[3], "name_orig": row[4], "name_dest": row[5],
                "oldbalance_org": row[6], "newbalance_orig": row[7],
                "oldbalance_dest": row[8], "newbalance_dest": row[9]
            }
            if self.redis_client.get(f"proc:{data['event_id']}"): return
            
            event = TransactionEvent(**data)
            decision = self.engine.evaluate(event)
            
            # Upsert into Cassandra
            self.session.execute(
                "INSERT INTO transactions (event_id, event_time, amount, is_fraud, risk_score) VALUES (%s, %s, %s, %s, %s)",
                (data['event_id'], data['event_time'], data['amount'], 1 if decision.is_alert else 0, decision.risk_score)
            )
            self.redis_client.setex(f"proc:{data['event_id']}", 3600, "1")
            
            if decision.is_alert:
                yield json.dumps(decision.__dict__)

    alerts_stream = joined_stream.process(ProductionScorer())

    # Define Alert Sink
    alert_sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("fraud_alerts")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .build()
    
    # Link DataStream alert to the same job context
    # Note: Using DataStream sink in StatementSet requires creating a Table from it or just executing normally
    # But Flink allows multiple DataStream sinks + Table sinks in ONE env.execute()
    alerts_stream.sink_to(alert_sink)

    logger.info("🚀 Production Flink Pipeline (StatementSet Unified) Starting...")
    # Execute the unified job
    statement_set.execute()

if __name__ == "__main__":
    run_flink_production_job()
