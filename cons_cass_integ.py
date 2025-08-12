import time
from kafka import KafkaConsumer
import json
import sys
import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Cassandra Configuration ---
# 'cassandra' is the service name defined in your docker-compose.yml
CASSANDRA_HOSTS = ['localhost']
CASSANDRA_PORT = 9042
CASSANDRA_USERNAME = 'cassandra'
CASSANDRA_PASSWORD = 'cassandra'
KEYSPACE_NAME = 'mimic_data' # Ensure this matches your keyspace name

# Define the mapping from Kafka Topic Name to Cassandra Table Name
# This dictionary also helps in extracting the 'Key' (sensor_id) for Cassandra
TOPIC_TO_TABLE_MAPPING = {
    'sensor_1_raw_data': {'table': 'data_sensor1', 'sensor_id_for_cassandra': 'sensor_1'},
    'sensor_2_raw_data': {'table': 'data_sensor2', 'sensor_id_for_cassandra': 'sensor_2'},
    'sensor_3_raw_data': {'table': 'data_sensor3', 'sensor_id_for_cassandra': 'sensor_3'},
}
ALL_KAFKA_TOPICS = list(TOPIC_TO_TABLE_MAPPING.keys()) # Get all topics to subscribe to

# Global Cassandra session and cluster objects
cluster = None
session = None
# Dictionary to hold prepared statements, keyed by table name
prepared_statements = {}

def setup_cassandra_connection():
    global cluster, session, prepared_statements
    auth_provider = PlainTextAuthProvider(username=CASSANDRA_USERNAME, password=CASSANDRA_PASSWORD)

    max_retries = 10
    retry_delay_seconds = 10

    for i in range(max_retries):
        try:
            logging.info(f"Attempting to connect to Cassandra (Attempt {i+1}/{max_retries})...")
            cluster = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT, auth_provider=auth_provider)
            session = cluster.connect()
            session.set_keyspace(KEYSPACE_NAME)
            logging.info(f"Successfully connected to Cassandra cluster: {CASSANDRA_HOSTS}, Keyspace: {KEYSPACE_NAME}")

            # Prepare INSERT statements for EACH table
            for topic_name, config in TOPIC_TO_TABLE_MAPPING.items():
                table_name = config['table']
                # The column order here (Key, Timestamp, output_data) MUST match your table schema
                INSERT_CQL = f"""
                INSERT INTO {table_name} (key, timestamp, output_data)
                VALUES (?, ?, ?);
                """
                prepared_statements[table_name] = session.prepare(INSERT_CQL)
                logging.info(f"Prepared INSERT statement for table '{table_name}'.")

            return True # Connection and preparation successful
        except Exception as e:
            logging.warning(f"Failed to connect or prepare Cassandra: {e}. Retrying in {retry_delay_seconds} seconds...")
            time.sleep(retry_delay_seconds)
    logging.error("Exceeded max retries. Could not connect to Cassandra.")
    return False

# --- Kafka Configuration ---
# KAFKA_BROKERS = ['localhost:9092'] # Use this if your consumer runs on HOST
# If your consumer runs IN A DOCKER CONTAINER within the same network:
KAFKA_BROKERS = ['localhost:9092'] # 'broker' is the service name, 29092 is the internal listener port

# --- Consumer Setup ---
consumer = KafkaConsumer(
    *ALL_KAFKA_TOPICS, # Subscribe to all relevant topics
    bootstrap_servers=KAFKA_BROKERS,
    group_id='raw_data_processing_group', # Ensure this is unique if you have multiple consumer instances
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: x.decode('utf-8') # Decode message value from bytes to UTF-8 string
)

# --- Processing and Insertion Function ---
def process_message_and_insert(message):
    """
    Processes a Kafka message and inserts relevant data into the correct Cassandra table.
    Assumes message.value is a string in "TIMESTAMP|HEX_DATA" format.
    """
    try:
        # Get the configuration for the current Kafka topic
        topic_config = TOPIC_TO_TABLE_MAPPING.get(message.topic)
        if not topic_config:
            logging.warning(f"No Cassandra table mapping found for Kafka topic: {message.topic}. Skipping message.")
            return

        target_table = topic_config['table']
        # This is the 'Key' for your Cassandra table's primary key
        sensor_id_for_cassandra = topic_config['sensor_id_for_cassandra']

        message_parts = message.value.split('|', 1)

        if len(message_parts) == 2:
            timestamp_str = message_parts[0]
            raw_hex_data = message_parts[1]

            # Convert timestamp from epoch string (milliseconds) to datetime object
            timestamp_ms = int(timestamp_str)
            event_timestamp = datetime.datetime.fromtimestamp(timestamp_ms / 1000, tz=datetime.timezone.utc)
            
            # Data to insert into Cassandra. Order MUST match the prepared statement:
            # (key, timestamp, output_data)
            data_to_insert = (
                sensor_id_for_cassandra, # This is the 'Key' column in Cassandra
                # event_timestamp,         # This is the 'Timestamp' column
                timestamp_ms,
                raw_hex_data             # This is the 'output_data' column
            )

            # Get the prepared statement for the target table
            prepared_stmt = prepared_statements.get(target_table)
            if not prepared_stmt:
                logging.error(f"Prepared statement not found for table '{target_table}'. This indicates a setup error.")
                return

            # Execute the prepared statement
            session.execute(prepared_stmt, data_to_insert)
            logging.info(f"Inserted into {target_table}: Key='{sensor_id_for_cassandra}', Time='{event_timestamp}', Data='{raw_hex_data[:20]}...' (from topic {message.topic})")
        else:
            logging.warning(f"Skipping malformed message from topic {message.topic}: {message.value}")

    except ValueError as ve:
        logging.error(f"Data conversion error (e.g., timestamp): {ve}. Message: {message.value}. Topic: {message.topic}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during Cassandra insertion: {e}. Message: {message.value}. Topic: {message.topic}")

# --- Main Consumption Loop ---
if __name__ == '__main__':
    # Initialize Cassandra connection and prepare statements
    if not setup_cassandra_connection():
        logging.error("Exiting as Cassandra connection could not be established.")
        sys.exit(1)

    logging.info(f"Starting Kafka Consumer on brokers: '{KAFKA_BROKERS}' for topics: {ALL_KAFKA_TOPICS}")
    logging.info(f"Using consumer group ID: 'raw_data_processing_group'")

    try:
        for message in consumer:
            process_message_and_insert(message)

    except KeyboardInterrupt:
        logging.info("\nStopping consumer.")
    except Exception as e:
        logging.error(f"An unexpected error occurred in the main consumer loop: {e}")
    finally:
        consumer.close()
        logging.info("Kafka Consumer closed.")
        if session:
            session.shutdown()
            logging.info("Cassandra Session closed.")
        if cluster:
            cluster.shutdown()
            logging.info("Cassandra Cluster connection closed.")
        logging.info("Consumer stopped.")