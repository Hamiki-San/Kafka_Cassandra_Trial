import time
from kafka import KafkaConsumer
import sys
import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime, timezone
import pytz

# To observe the time of data consumed, prompt out the consumed time.
LOCAL_TIMEZONE = pytz.timezone('Europe/Berlin') # Paderborn timezone

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Cassandra Configuration ---
CASSANDRA_HOSTS = ['localhost']
CASSANDRA_PORT = 9042
CASSANDRA_USERNAME = 'cassandra'
CASSANDRA_PASSWORD = 'cassandra'
KEYSPACE_NAME = 'sensor_data'

# Define the mapping from Kafka Topic Name to Cassandra Table Name
TOPIC_TO_TABLE_MAPPING = {
    'blickfeld': {'table': 'blickfeld', 'data_type': 'pcd_data', 'sensor_id_for_cassandra': 'blickfeld'},
    'helios1': {'table': 'helios1', 'data_type': 'pcd_data', 'sensor_id_for_cassandra': 'helios1'},
    'helios2': {'table': 'helios2', 'data_type': 'pcd_data', 'sensor_id_for_cassandra': 'helios2'},
    'lupus': {'table': 'lupus', 'data_type': 'image', 'sensor_id_for_cassandra': 'lupus'},
    'dahua': {'table': 'dahua', 'data_type': 'image', 'sensor_id_for_cassandra': 'dahua'},
    'zedx_top': {'table': 'zedx_top', 'data_type': 'image', 'sensor_id_for_cassandra': 'zedx_top'},
    'zedx_bottom': {'table': 'zedx_bottom', 'data_type': 'image', 'sensor_id_for_cassandra': 'zedx_bottom'},
    'zedx_l': {'table': 'zedx_l', 'data_type': 'image', 'sensor_id_for_cassandra': 'zedx_l'},
    'zedx_r': {'table': 'zedx_r', 'data_type': 'image', 'sensor_id_for_cassandra': 'zedx_r'},
}
ALL_KAFKA_TOPICS = list(TOPIC_TO_TABLE_MAPPING.keys())

cluster = None
session = None
prepared_statements = {}

def setup_cassandra_connection():
    global cluster, session, prepared_statements
    auth_provider = PlainTextAuthProvider(username=CASSANDRA_USERNAME, password=CASSANDRA_PASSWORD)

    max_retries = 10
    retry_delay_seconds = 5

    for i in range(max_retries):
        try:
            logging.info(f"Attempting to connect to Cassandra (Attempt {i+1}/{max_retries})...")
            cluster = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT, auth_provider=auth_provider)
            session = cluster.connect()
            session.set_keyspace(KEYSPACE_NAME)
            logging.info(f"Successfully connected to Cassandra cluster: {CASSANDRA_HOSTS}, Keyspace: {KEYSPACE_NAME}")

            for topic_name, config in TOPIC_TO_TABLE_MAPPING.items():
                table_name = config['table']
                data_type = config['data_type']

                INSERT_CQL = f"""
                INSERT INTO {table_name} (sensor_id, timestamp, {data_type})
                VALUES (?, ?, ?);
                """
                prepared_statements[table_name] = session.prepare(INSERT_CQL)
                logging.info(f"Prepared INSERT statement for table '{table_name}' using column '{data_type}'.")
            
            return True
        except Exception as e:
            logging.warning(f"Failed to connect or prepare Cassandra: {e}. Retrying in {retry_delay_seconds} seconds...")
            time.sleep(retry_delay_seconds)
    logging.error("Exceeded max retries. Could not connect to Cassandra.")
    return False

# --- Kafka Configuration ---
KAFKA_BROKERS = ['localhost:9092']

# --- Consumer Setup ---
consumer = KafkaConsumer(
    *ALL_KAFKA_TOPICS,
    bootstrap_servers=KAFKA_BROKERS,
    group_id='raw_data_processing_group',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: x.decode('utf-8')
)

# --- Processing and Insertion Function ---
def process_message_and_insert(message):
    """
    Processes a Kafka message and inserts relevant data into the correct Cassandra table and column.
    Handles different timestamp formats from producers.
    """
    try:
        topic_config = TOPIC_TO_TABLE_MAPPING.get(message.topic)
        if not topic_config:
            logging.warning(f"No Cassandra table mapping found for Kafka topic: {message.topic}. Skipping message.")
            return

        target_table = topic_config['table']
        sensor_id_for_cassandra = topic_config['sensor_id_for_cassandra']
        data_type_column = topic_config['data_type']

        timestamp_str = None
        raw_data = message.value

        # Extract timestamp string from message header
        for line in message.value.split('\n'):
            if line.startswith('# Event Created:'):
                timestamp_str = line.split('# Event Created:')[1].strip()
                break
        
        if timestamp_str is None:
            logging.warning(f"Skipping message from topic {message.topic} as no timestamp line was found.")
            return

        # Attempt to parse timestamp
        event_timestamp_utc = None
        
        # --- NEW LOGIC FOR TIMESTAMP PARSING ---
        # 1. Try to parse with explicit timezone (%z) first (for .pcd data or well-formatted local)
        try:
            timestamp_parsed_aware = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f%z')
            event_timestamp_utc = timestamp_parsed_aware.astimezone(timezone.utc)
            # If successfully parsed with %z, and the offset implies it was local,
            # this correctly converts it to UTC. If it was already UTC, it remains UTC.
            
        except ValueError:
            # 2. If parsing with %z fails, it's likely a naive timestamp (from .img producer)
            # Assume naive timestamp from .img producer is *intended* to be in LOCAL_TIMEZONE
            try:
                timestamp_parsed_naive = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f%z')
                # Localize the naive timestamp to LOCAL_TIMEZONE *before* converting to UTC
                event_timestamp_utc = LOCAL_TIMEZONE.localize(timestamp_parsed_naive).astimezone(timezone.utc)
                logging.info(f"Parsed naive timestamp from topic {message.topic}, localized to {LOCAL_TIMEZONE}: {timestamp_str}")
            except ValueError as ve:
                logging.error(f"Failed to parse timestamp '{timestamp_str}' as aware or naive: {ve}. Skipping message.")
                return
        # --- END NEW LOGIC ---

        if event_timestamp_utc is None:
            logging.error(f"Could not extract a valid timestamp from message value: {message.value}. Skipping.")
            return

        # Convert to local timezone for logging display ONLY
        event_timestamp_local_display = event_timestamp_utc.astimezone(LOCAL_TIMEZONE)
        
        prepared_stmt = prepared_statements.get(target_table)
        if not prepared_stmt:
            logging.error(f"Prepared statement not found for table '{target_table}'. This indicates a setup error.")
            return

        # Store UTC timestamp in Cassandra (recommended practice)
        data_to_insert = (
            sensor_id_for_cassandra,
            #event_timestamp_utc, # Store UTC timestamp in Cassandra
            event_timestamp_local_display,
            raw_data
        )

        session.execute(prepared_stmt, data_to_insert)
        logging.info(
            f"Inserted into {target_table} (column='{data_type_column}'): "
            f"Key='{sensor_id_for_cassandra}', "
            f"Time='{event_timestamp_local_display}', " # Display local time
            f"Offset={message.offset}"
        )

    except Exception as e:
        logging.error(f"An unexpected error occurred during Cassandra insertion: {e}. Message: {message.value}. Topic: {message.topic}")

# --- Main Consumption Loop ---
if __name__ == '__main__':
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