FROM flink:1.17.1-scala_2.12

# Install python and pip
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

# Install PyFlink and other dependencies
RUN python3 -m pip install --no-cache-dir \
    apache-flink==1.17.1 \
    cassandra-driver \
    redis \
    kafka-python \
    numpy \
    joblib \
    scikit-learn \
    pandas==1.3.5

# Download connectors
ENV FLINK_LIB_DIR=/opt/flink/lib
RUN wget -P $FLINK_LIB_DIR https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar && \
    wget -P $FLINK_LIB_DIR https://repo1.maven.org/maven2/org/apache/flink/flink-connector-cassandra_2.12/1.16.1/flink-connector-cassandra_2.12-1.16.1.jar && \
    wget -P $FLINK_LIB_DIR https://repo1.maven.org/maven2/com/datastax/oss/java-driver-core/4.15.0/java-driver-core-4.15.0.jar

WORKDIR /opt/project

COPY fraud_pipeline /opt/project/fraud_pipeline
COPY model /opt/project/model
COPY flink-app /opt/project/flink-app

ENV PYTHONPATH=/opt/project

# Entrypoint script
COPY flink-app/entrypoint.sh /opt/project/entrypoint.sh
RUN chmod +x /opt/project/entrypoint.sh

ENTRYPOINT ["/opt/project/entrypoint.sh"]
