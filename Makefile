SPARK_VERSION=3.4.1

build-airflow:
	docker build -t apache/airflow-extend:2.7.1 .

run-services:
	docker compose up -d

run-spark:
	spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_VERSION},com.datastax.spark:spark-cassandra-connector_2.12:${SPARK_VERSION} --master spark://localhost:7077 spark_stream.py