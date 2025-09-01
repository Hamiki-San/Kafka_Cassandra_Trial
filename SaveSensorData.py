import time
from kafka import KafkaConsumer
import sys
import logging
import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime, timezone
import pytz

# To observe the time of data consumed, prompt out the consumed time.
LOCAL_TIMEZONE = pytz.timezone('Europe/Berlin') # Paderborn timezone

# --- File Storage Configuration ---
# ADJUST: Define the base directory where 'pcd_data' and 'img_data' folders will be created.
# Example 1: To save in the same directory as the script:
BASE_DATA_DIR = os.getcwd()

# Example 2: To save in a specific absolute path (e.g., on Windows):
# BASE_DATA_DIR = "C:\\SensorData"
# Make sure this directory exists or the script has permissions to create it.

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Cassandra Configuration ---
CASSANDRA_HOSTS = ['localhost'] # ADJUST: Your Cassandra host(s) if not localhost
CASSANDRA_PORT = 9042
CASSANDRA_USERNAME = 'cassandra' # ADJUST: Your Cassandra username
CASSANDRA_PASSWORD = 'cassandra' # ADJUST: Your Cassandra password
KEYSPACE_NAME = 'sensor_data' # ADJUST: Your Cassandra keyspace name

# Define the mapping from Kafka Topic Name to Cassandra Table Name and data column
TOPIC_TO_TABLE_MAPPING = {
    'blickfeld': {'table': 'blickfeld', 'data_column': 'payload', 'sensor_id_for_cassandra': 'blickfeld'},
    'helios_1': {'table': 'helios_1', 'data_column': 'payload', 'sensor_id_for_cassandra': 'helios_1'},
    'helios_2': {'table': 'helios_2', 'data_column': 'payload', 'sensor_id_for_cassandra': 'helios_2'},
    'lupus': {'table': 'lupus', 'data_column': 'payload', 'sensor_id_for_cassandra': 'lupus'},
    'dahua': {'table': 'dahua', 'data_column': 'payload', 'sensor_id_for_cassandra': 'dahua'},
    'zedx_top': {'table': 'zedx_top', 'data_column': 'payload', 'sensor_id_for_cassandra': 'zedx_top'},
    'zedx_bottom': {'table': 'zedx_bottom', 'data_column': 'payload', 'sensor_id_for_cassandra': 'zedx_bottom'},
    'zedx_left': {'table': 'zedx_left', 'data_column': 'payload', 'sensor_id_for_cassandra': 'zedx_left'},
    'zedx_right': {'table': 'zedx_right', 'data_column': 'payload', 'sensor_id_for_cassandra': 'zedx_right'},
    'webcam-stream': {'table': 'webcam', 'data_column': 'payload', 'sensor_id_for_cassandra': 'webcam-stream'},
    'flightradar-data': {'table': 'flight_raw_data', 'data_column': 'payload', 'sensor_id_for_cassandra': 'flightradar'},
}

# Add all topics to the list
ALL_KAFKA_TOPICS = list(TOPIC_TO_TABLE_MAPPING.keys())

cluster = None
session = None
prepared_statements = {} # Dictionary to store prepared statements, keyed by table name

# --- Kafka Configuration ---
KAFKA_BROKERS = ['localhost:9092'] # ADJUST: Your Kafka broker addresses if not localhost:9092

# --- Consumer Class ---
class KafkaCassandraConsumer:
    def __init__(self, brokers: str, topics: list, cassandra_host: str = "127.0.0.1"):
        self.brokers = brokers
        self.topics = topics
        self.cassandra_host = cassandra_host
        self.consumer = None
        self.cassandra_session = None
        self.prepared_statements = {}
        self.last_saved_timestamp = {} # Dictionary to track the last saved timestamp for each sensor

    def setup_kafka_consumer(self):
        """Initializes and returns a Kafka consumer instance."""
        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.brokers,
                group_id='raw_data_processing_group',
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda x: x # Keep raw binary format for image/pcd, decode others later
            )
            logging.info("🟢 Successfully connected to Kafka consumer.")
            return True
        except Exception as e:
            logging.error(f"❌ Failed to connect to Kafka consumer: {e}")
            return False

    def setup_cassandra_connection(self):
        """Sets up connection to Cassandra and prepares statements."""
        global cluster, session # Use global variables for consistent access
        auth_provider = PlainTextAuthProvider(username=CASSANDRA_USERNAME, password=CASSANDRA_PASSWORD)

        max_retries = 10
        retry_delay_seconds = 5

        for i in range(max_retries):
            try:
                logging.info(f"Attempting to connect to Cassandra (Attempt {i+1}/{max_retries})...")
                cluster = Cluster([self.cassandra_host], port=CASSANDRA_PORT, auth_provider=auth_provider)
                session = cluster.connect()
                session.set_keyspace(KEYSPACE_NAME)
                self.cassandra_session = session # Assign to class instance
                logging.info(f"Successfully connected to Cassandra cluster: {self.cassandra_host}, Keyspace: {KEYSPACE_NAME}")

                self.prepare_statements() # Prepare all statements once connected
                return True
            except Exception as e:
                logging.warning(f"Failed to connect or prepare Cassandra: {e}. Retrying in {retry_delay_seconds} seconds...")
                time.sleep(retry_delay_seconds)
        logging.error("Exceeded max retries. Could not connect to Cassandra.")
        return False

    def prepare_statements(self):
        """Prepare CQL insert statements based on TOPIC_TO_TABLE_MAPPING."""
        for topic_name, config in TOPIC_TO_TABLE_MAPPING.items():
            table_name = config['table']
            data_column = config['data_column']

            # MODIFIED: Changed 'sensor_id' to 'topic' to match the new schema
            INSERT_CQL = f"""
            INSERT INTO {table_name} (topic, event_created, {data_column})
            VALUES (?, ?, ?);
            """
            self.prepared_statements[table_name] = self.cassandra_session.prepare(INSERT_CQL)
            logging.info(f"Prepared INSERT statement for table '{table_name}' using column '{data_column}'.")

    def consume_and_store_data(self) -> None:
        """Consume messages from Kafka and store them in Cassandra."""
        if not self.setup_cassandra_connection():
            logging.error("Exiting as Cassandra connection could not be established.")
            sys.exit(1)

        if not self.setup_kafka_consumer():
            logging.error("Exiting as Kafka consumer could not be established.")
            sys.exit(1)
        
        logging.info(f"Starting Kafka consumer on brokers: '{self.brokers}' for topics: {self.topics}")

        try:
            for message in self.consumer:
                topic = message.topic
                payload = message.value # Raw bytes

                timestamp_dt_utc_aware = datetime.fromtimestamp(message.timestamp / 1000.0, tz=timezone.utc)
                timestamp_dt_naive_local = timestamp_dt_utc_aware.astimezone(LOCAL_TIMEZONE).replace(tzinfo=None)

                timestamp_for_storage = timestamp_dt_naive_local
                formatted_timestamp = timestamp_dt_naive_local.strftime('%Y-%m-%d_%H-%M-%S-%f')

                topic_config = TOPIC_TO_TABLE_MAPPING.get(topic)
                if not topic_config:
                    logging.warning(f"No Cassandra table mapping found for Kafka topic: {topic}. Skipping message.")
                    continue

                target_table = topic_config['table']
                sensor_id_for_cassandra = topic_config['sensor_id_for_cassandra']

                prepared_stmt = self.prepared_statements.get(target_table)
                if not prepared_stmt:
                    logging.error(f"Prepared statement not found for table '{target_table}'. This indicates a setup error.")
                    continue

                if topic in ["blickfeld", "helios_1", "helios_2"]:
                    # PCD Data - Store as a file and insert file path
                    file_path = os.path.join(BASE_DATA_DIR, "pcd_data", sensor_id_for_cassandra, f"{sensor_id_for_cassandra}_{formatted_timestamp}.pcd")
                    self.save_file(file_path, payload)

                    if not self.cassandra_session.is_shutdown:
                        # MODIFIED: Using 'topic' directly in the execute call
                        self.cassandra_session.execute(prepared_stmt, (topic, timestamp_for_storage, file_path))
                        logging.info(f"Stored PCD file: {file_path}")

                elif topic in ["lupus", "dahua", "zedx_left", "zedx_right", "zedx_top", "zedx_bottom", "webcam-stream"]:
                    last_timestamp = self.last_saved_timestamp.get(sensor_id_for_cassandra, None)

                    if last_timestamp is None or (timestamp_for_storage - last_timestamp).total_seconds() >= 0.5:
                        # Image Data - Store as a file and insert file path
                        img_path = os.path.join(BASE_DATA_DIR, "img_data", sensor_id_for_cassandra, f"{sensor_id_for_cassandra}_{formatted_timestamp}.jpg")
                        self.save_file(img_path, payload)

                        if not self.cassandra_session.is_shutdown:
                            # MODIFIED: Using 'topic' directly in the execute call
                            self.cassandra_session.execute(prepared_stmt, (topic, timestamp_for_storage, img_path))
                            logging.info(f"Stored Image Data for {sensor_id_for_cassandra}: {img_path}")

                        self.last_saved_timestamp[sensor_id_for_cassandra] = timestamp_for_storage
                    else:
                        logging.info(f"Skipping image for {sensor_id_for_cassandra} at {timestamp_for_storage}, within 0.5s window.")
                
                elif topic == "flightradar-data":
                    # Text/JSON Data - Decode and insert payload directly
                    try:
                        decoded_payload = payload.decode('utf-8')
                        if not self.cassandra_session.is_shutdown:
                            # MODIFIED: Using 'topic' directly in the execute call
                            self.cassandra_session.execute(prepared_stmt, (topic, timestamp_for_storage, decoded_payload))
                            logging.info(f"Stored Text Data for {sensor_id_for_cassandra}: {decoded_payload[:50]}...")
                    except UnicodeDecodeError:
                        logging.error(f"Failed to decode payload for topic {topic}. Skipping.")

                else:
                    logging.warning(f"Received message from unhandled topic: {topic}. Skipping.")

                self.consumer.commit()

        except KeyboardInterrupt:
            logging.info("\nConsumer interrupted by user.")
        except Exception as e:
            logging.error(f"An unexpected error occurred in the main consumer loop: {e}")
        finally:
            self.close()

    def save_file(self, file_path, payload):
        """Save the payload (binary data) to a file."""
        try:
            directory = os.path.dirname(file_path)
            if not os.path.exists(directory):
                os.makedirs(directory)

            with open(file_path, "wb") as file:
                file.write(payload)
            logging.debug(f"File saved: {file_path}")
        except Exception as e:
            logging.error(f"Error writing file {file_path}: {e}")

    def close(self) -> None:
        """Close Kafka consumer and Cassandra connection."""
        if self.consumer:
            self.consumer.close()
            logging.info("Kafka consumer closed.")
        if self.cassandra_session:
            self.cassandra_session.shutdown()
            logging.info("Cassandra Session closed.")
        if cluster:
            cluster.shutdown()
            logging.info("Cassandra Cluster connection closed.")
        logging.info("Consumer stopped.")

if __name__ == "__main__":
    kafka_brokers = "localhost:9092" # ADJUST: Your Kafka broker address
    cassandra_host_ip = "localhost" # ADJUST: Your Cassandra host IP

    kafka_topics = [
        "zedx_top", "zedx_bottom", "zedx_left", "zedx_right",
        "lupus", "dahua",
        "blickfeld", "helios_1", "helios_2",
        "webcam-stream",
        "flightradar-data"
    ]

    consumer_app = KafkaCassandraConsumer(kafka_brokers, kafka_topics, cassandra_host=cassandra_host_ip)
    consumer_app.consume_and_store_data()
