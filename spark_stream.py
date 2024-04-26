import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

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
            picture TEXT
        )
        """)

def insert_data(session, **kwargs):
    # to finish

    # id = kwargs.get('id')
    # first_name = kwargs.get('first_name')
    # last_name = kwargs.get('last_name')
    # gender = kwargs.get('gender')
    # email = kwargs.get('email')
    # username = kwargs.get('username')
    # country = kwargs.get('country')
    # state = kwargs.get('state')
    # dob = kwargs.get('dob')
    # age = kwargs.get('age')
    # picture = kwargs.get('picture')

    # try:
    #     session.execute("""
    #         INSERT INTO spark_streams.created_users(id, first_name, last_name,
    #         gender, email, username, country, state, dob, age, picture)
    #         VALUES ()
    #     """)
    return None

def create_spark_connection():
    # Creating Spark session with relevant Kafka and Cassandra config 
    s_conn = None

    try:
        s_conn = SparkSession.builder \
        .appName('SparkDataStreaming') \
        .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.13:3.41, \
        org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1') \
        .config('spark.cassandra.connection.host', 'broker') \
        .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception: {e}")

def create_cassandra_connection():
    session = None
    try:
        cluster = Cluster(['localhost'])

        session = cluster.connect()
    except:
        logging.error(f"Couldn't create the spark session due to exception: {e}")
        return None

if __name__ == "__main__":
    spark_conn = create_spark_connection()
    if spark_conn is not None:
        cass_conn = create_cassandra_connection()

        if cass_conn is not None:
            create_keyspace(cass_conn)