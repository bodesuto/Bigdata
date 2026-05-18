import os
import json
import logging
from pyflink.common import WatermarkStrategy, Types, Time
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings
from pyflink.table.expressions import col, lit
from pyflink.table.window import Tumble, Slide

from fraud_pipeline import PipelineConfig, RuleEngine
from fraud_pipeline.models import TransactionEvent

# --- CONFIG ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

def run_flink_mirror_job():
    # 1. Setup Table Environment (Best for Joins & Windows)
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    t_env = StreamTableEnvironment.create(env, environment_settings=EnvironmentSettings.in_streaming_mode())
    
    # 2. Define Kafka Sources via SQL (Cleaner for 3-way joins)
    # Source: Transactions
    t_env.execute_sql(f"""
        CREATE TABLE tx_source (
            event_id STRING,
            txn_type STRING,
            amount DOUBLE,
            name_orig STRING,
            name_dest STRING,
            event_time TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'transaction_topic',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'flink-mirror-group',
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
            WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
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
            WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'receiver_state_topic',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'format' = 'json'
        )
    """)

    # 3. Perform 3-WAY INTERVAL JOIN (Mirroring Spark's Join)
    # Join Tx + Sender + Receiver within a 30s window
    joined_table = t_env.sql_query("""
        SELECT 
            T.event_id, T.event_time, T.txn_type, T.amount, T.name_orig, T.name_dest,
            S.oldbalance_org, S.newbalance_orig,
            R.oldbalance_dest, R.newbalance_dest
        FROM tx_source T
        JOIN sender_source S ON T.event_id = S.event_id 
            AND S.event_time BETWEEN T.event_time - INTERVAL '30' SECOND AND T.event_time + INTERVAL '30' SECOND
        JOIN receiver_source R ON T.event_id = R.event_id
            AND R.event_time BETWEEN T.event_time - INTERVAL '30' SECOND AND T.event_time + INTERVAL '30' SECOND
    """)

    # 4. Perform WINDOW AGGREGATION (Mirroring Spark's Metrics)
    # Tumbling 5-min Window
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
    
    t_env.execute_sql("""
        INSERT INTO window_metrics_sink
        SELECT 
            TUMBLE_START(event_time, INTERVAL '5' MINUTE),
            TUMBLE_END(event_time, INTERVAL '5' MINUTE),
            SUM(amount),
            COUNT(*)
        FROM tx_source
        GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE)
    """)

    # 5. INTEGRATE PYTHON SCORING (Rule Engine + ML)
    # Convert joined table to DataStream to use Python logic
    joined_stream = t_env.to_append_stream(joined_table, Types.ROW([
        Types.STRING(), Types.SQL_TIMESTAMP(), Types.STRING(), Types.DOUBLE(), 
        Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.DOUBLE(), 
        Types.DOUBLE(), Types.DOUBLE()
    ]))

    class ScoringProcessFunction(ProcessFunction):
        def open(self, ctx):
            import redis
            from cassandra.cluster import Cluster
            from fraud_pipeline import PipelineConfig, RuleEngine
            self.config = PipelineConfig()
            self.engine = RuleEngine(self.config)
            self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            self.cluster = Cluster([CASSANDRA_HOST])
            self.session = self.cluster.connect("fraud_detection")

        def process_element(self, row, ctx):
            # Map Flink Row to Python Dict
            data = {
                "event_id": row[0], "event_time": str(row[1]), "txn_type": row[2],
                "amount": row[3], "name_orig": row[4], "name_dest": row[5],
                "oldbalance_org": row[6], "newbalance_orig": row[7],
                "oldbalance_dest": row[8], "newbalance_dest": row[9]
            }
            
            # Deduplication
            if self.redis_client.get(f"proc:{data['event_id']}"): return

            # Evaluate
            event = TransactionEvent(**data)
            decision = self.engine.evaluate(event)
            
            # Persistence (Mirror Spark Sinks)
            self.session.execute(
                "INSERT INTO transactions (event_id, event_time, amount, is_fraud, risk_score) VALUES (%s, %s, %s, %s, %s)",
                (data['event_id'], data['event_time'], data['amount'], 1 if decision.is_alert else 0, decision.risk_score)
            )
            self.redis_client.setex(f"proc:{data['event_id']}", 3600, "1")
            
            if decision.is_alert:
                yield json.dumps(decision.__dict__)

    alerts = joined_stream.process(ScoringProcessFunction())

    # 6. Kafka Alert Sink
    alert_sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("fraud_alerts")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .build()
    
    alerts.sink_to(alert_sink)

    logger.info("🚀 Flink Mirror Pipeline (Spark Logic 1:1) Starting...")
    env.execute("Flink Fraud Discovery Mirror")

if __name__ == "__main__":
    run_flink_mirror_job()
