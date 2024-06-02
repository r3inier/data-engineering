import logging
from datetime import datetime

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

def create_table(session):
    # complete query
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            email TEXT,
            username TEXT,
            country TEXT,
            state TEXT,
            dob TEXT,
            age INT,
            picture TEXT);
        """)
    
    print("Table created successfully!")

def insert_data(session, **kwargs):
    # to finish
    id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    email = kwargs.get('email')
    username = kwargs.get('username')
    country = kwargs.get('country')
    state = kwargs.get('state')
    dob = kwargs.get('dob')
    age = kwargs.get('age')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name,
            gender, email, username, country, state, dob, age, picture)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (id, first_name, last_name, gender, email, username, country, state, dob, age, picture))
        print("Data inserted for {first_name} {last_name}")
    except Exception as e:
        logging.error(f"Could not insert data due to {e}")

    return None

def create_spark_connection():
    # Creating Spark session with relevant Kafka and Cassandra config 
    s_conn = None

    try:
        s_conn = SparkSession.builder \
        .appName('SparkDataStreaming') \
        .config('spark.jars.packages',  "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,"
                                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config('spark.cassandra.connection.host', 'localhost') \
        .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        print("Spark connection created successfully")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception: {e}")
    
    return s_conn

def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'], connect_timeout=20)

        cassandra_sess = cluster.connect()
        print("Cassandra connection created successfully")
        return cassandra_sess
    except Exception as e:
        logging.error(f"Couldn't create the cassandra connection due to exception: {e}")
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
        print("Kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}")

    return spark_df

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("country", StringType(), False),
        StructField("state", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("age", IntegerType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    
    print(sel)

    return sel

if __name__ == "__main__":
    spark_conn = create_spark_connection()
    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        cass_sess = create_cassandra_connection()
        if cass_sess is not None:
            create_keyspace(cass_sess)
            create_table(cass_sess)

            print("Streaming is starting...")

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())
            
            streaming_query.awaitTermination()