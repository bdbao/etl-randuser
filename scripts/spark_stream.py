import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def create_cassandra_connection():
    """Establish connection to Cassandra."""
    try:
        return Cluster(['localhost']).connect()
    except Exception as e:
        logging.error(f"Could not create Cassandra connection: {e}")
        return None

def setup_cassandra(session):
    """Create keyspace and table in Cassandra."""
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """
    )
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
            dob TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT);
    """)
    print("Cassandra setup completed!")

def create_spark_session():
    """Initialize Spark session."""
    try:
        spark = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1," \
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        print("Spark session created successfully!")
        return spark
    except Exception as e:
        logging.error(f"Failed to create Spark session: {e}")
        return None

def read_kafka_stream(spark):
    """Read streaming data from Kafka topic."""
    try:
        return spark.readStream.format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .option('failOnDataLoss', 'false') \
            .load()
    except Exception as e:
        logging.warning(f"Failed to create Kafka stream: {e}")
        return None

def parse_kafka_data(kafka_df):
    """Parse Kafka JSON messages into structured DataFrame."""
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("dob", StringType(), True),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])
    return kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')) \
        .select("data.*")

def main():
    spark = create_spark_session()
    if not spark:
        return
    
    kafka_df = read_kafka_stream(spark)
    if kafka_df is None:
        return
    
    parsed_df = parse_kafka_data(kafka_df)
    session = create_cassandra_connection()
    if session:
        setup_cassandra(session)
        print("Starting data stream...")
        query = parsed_df.writeStream.format("org.apache.spark.sql.cassandra") \
            .option('checkpointLocation', './tmp/checkpoint') \
            .option('keyspace', 'spark_streams') \
            .option('table', 'created_users') \
            .start()
        query.awaitTermination()

if __name__ == "__main__":
    main()
