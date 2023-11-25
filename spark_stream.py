import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


def create_keyspace(session):
    session.execute("""
        create keyspace if not exists spark_streams
        with replication = {'class': 'SimpleStrategy', 'replication_factor': '1' };
    """)
    print("Create keyspace successfully")


def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)
    print("Create table successfully")


def create_spark_connection():
    try:
        s_conn = SparkSession.builder \
            .appName("SparkDataStreaming") \
            .config("spark.cassandra.connection.host", "localhost") \
            .getOrCreate()
        # .config(
        #     "spark.jars.packages",
        #     "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
        #     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")

        return s_conn

    except Exception as e:
        logging.error(
            f"Couldn't create the spark session due to execption: {e}"
        )
        return None


def connect_to_kafka(spark_conn):
    spark_df = None

    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()

    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        cluster = Cluster(["localhost"])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(
            f"Couldn't create the cassandra session due to execption: {e}"
        )
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("cast(value as string)") \
        .select(from_json(col("value"), schema).alias("data")).select("data.*")

    return sel


if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            streaming_query = (
                selection_df.writeStream
                .format("org.apache.spark.sql.cassandra")
                .option("checkpointLocation", "/tmp/checkpoint")
                .option("keyspace", "spark_streams")
                .option("table", "created_users")
                .start()
            )

            streaming_query.awaitTermination()
