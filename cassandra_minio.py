import time
from kafka import KafkaConsumer
import sys
import logging
import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime, timezone
import pytz
import io
import json
from minio import Minio
import uuid

# --- Import all configurations from the new consumer_config.py file ---
import consumer_config

# To observe the time of data consumed, prompt out the consumed time.
LOCAL_TIMEZONE = pytz.timezone('Europe/Berlin') # Paderborn timezone

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Global variables for Cassandra connection (if needed) ---
cluster = None
session = None
prepared_statements = {}

# --- Consumer Class ---
class KafkaCassandraConsumer:
    def __init__(self, brokers: str, topics: list, cassandra_host: str = "127.0.0.1"):
        self.brokers = brokers
        self.topics = topics
        self.cassandra_host = cassandra_host
        self.consumer = None
        self.cassandra_session = None
        self.prepared_statements = {}
        self.last_saved_timestamp = {}
        self.minio_client = None

        # Define policies here
        self.PCD_BUCKET_POLICY = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": ["*"]},
                "Action": ["s3:GetObject"],
                "Resource": [f"arn:aws:s3:::{consumer_config.MINIO_PCD_BUCKET}/*"]
            }]
        }

        self.IMAGE_BUCKET_POLICY = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": ["*"]},
                "Action": ["s3:GetObject"],
                "Resource": [f"arn:aws:s3:::{consumer_config.MINIO_IMAGE_BUCKET}/*"]
            }]
        }

    def setup_kafka_consumer(self):
        """Initializes and returns a Kafka consumer instance."""
        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.brokers,
                group_id='raw_data_processing_group',
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda x: x
            )
            logging.info("🟢 Successfully connected to Kafka consumer.")
            return True
        except Exception as e:
            logging.error(f"❌ Failed to connect to Kafka consumer: {e}")
            return False

    def setup_cassandra_connection(self):
        """Sets up connection to Cassandra and prepares statements."""
        global cluster, session
        auth_provider = PlainTextAuthProvider(username=consumer_config.CASSANDRA_USERNAME, password=consumer_config.CASSANDRA_PASSWORD)

        max_retries = 10
        retry_delay_seconds = 5

        for i in range(max_retries):
            try:
                logging.info(f"Attempting to connect to Cassandra (Attempt {i+1}/{max_retries})...")
                cluster = Cluster([self.cassandra_host], port=consumer_config.CASSANDRA_PORT, auth_provider=auth_provider)
                session = cluster.connect()
                session.set_keyspace(consumer_config.KEYSPACE_NAME)
                self.cassandra_session = session
                logging.info(f"Successfully connected to Cassandra cluster: {self.cassandra_host}, Keyspace: {consumer_config.KEYSPACE_NAME}")

                self.prepare_statements()
                return True
            except Exception as e:
                logging.warning(f"Failed to connect or prepare Cassandra: {e}. Retrying in {retry_delay_seconds} seconds...")
                time.sleep(retry_delay_seconds)
        logging.error("Exceeded max retries. Could not connect to Cassandra.")
        return False
        
    def setup_minio_connection(self):
        """Initializes and returns a MinIO client instance."""
        try:
            self.minio_client = Minio(
                consumer_config.MINIO_ENDPOINT,
                access_key=consumer_config.MINIO_ACCESS_KEY,
                secret_key=consumer_config.MINIO_SECRET_KEY,
                secure=consumer_config.MINIO_SECURE
            )
            if not self.minio_client.bucket_exists(consumer_config.MINIO_PCD_BUCKET):
                self.minio_client.make_bucket(consumer_config.MINIO_PCD_BUCKET)
                logging.info(f"Created MinIO bucket: '{consumer_config.MINIO_PCD_BUCKET}'")
            self.set_bucket_policy(consumer_config.MINIO_PCD_BUCKET, self.PCD_BUCKET_POLICY)

            if not self.minio_client.bucket_exists(consumer_config.MINIO_IMAGE_BUCKET):
                self.minio_client.make_bucket(consumer_config.MINIO_IMAGE_BUCKET)
                logging.info(f"Created MinIO bucket: '{consumer_config.MINIO_IMAGE_BUCKET}'")
            self.set_bucket_policy(consumer_config.MINIO_IMAGE_BUCKET, self.IMAGE_BUCKET_POLICY)
            
            logging.info("🟢 Successfully connected to MinIO.")
            return True
        except Exception as e:
            logging.error(f"❌ Failed to connect to MinIO: {e}")
            return False
            
    def set_bucket_policy(self, bucket_name, policy):
        """Sets a public read-only policy for a specified bucket."""
        try:
            policy_json = json.dumps(policy)
            self.minio_client.set_bucket_policy(bucket_name, policy_json)
            logging.info(f"✅ Successfully set public policy for bucket: '{bucket_name}'")
        except Exception as e:
            logging.error(f"❌ Error setting policy for bucket '{bucket_name}': {e}")

    def prepare_statements(self):
        """Prepare CQL insert statements based on TOPIC_TO_TABLE_MAPPING."""
        for topic_name, config_map in consumer_config.TOPIC_TO_TABLE_MAPPING.items():
            table_name = config_map['table']
            data_column = config_map['data_column']

            INSERT_CQL = f"""
            INSERT INTO {consumer_config.KEYSPACE_NAME}.{table_name} (sensor_id, event_created, {data_column})
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

        if not self.setup_minio_connection():
            logging.error("Exiting as MinIO connection could not be established.")
            sys.exit(1)
        
        logging.info(f"Starting Kafka consumer on brokers: '{self.brokers}' for topics: {self.topics}")

        try:
            for message in self.consumer:
                topic = message.topic
                payload = message.value

                timestamp_dt_utc_aware = datetime.fromtimestamp(message.timestamp / 1000.0, tz=timezone.utc)
                timestamp_dt_naive_local = timestamp_dt_utc_aware.astimezone(LOCAL_TIMEZONE).replace(tzinfo=None)
                timestamp_for_storage = timestamp_dt_naive_local
                formatted_timestamp = timestamp_dt_naive_local.strftime('%Y-%m-%d_%H-%M-%S-%f')

                topic_config = consumer_config.TOPIC_TO_TABLE_MAPPING.get(topic)
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
                    object_name = f"{sensor_id_for_cassandra}/{formatted_timestamp}.pcd"
                    minio_uri = self.upload_to_minio(consumer_config.MINIO_PCD_BUCKET, object_name, payload)

                    if minio_uri and not self.cassandra_session.is_shutdown:
                        self.cassandra_session.execute(prepared_stmt, (sensor_id_for_cassandra, timestamp_for_storage, minio_uri))
                        logging.info(f"Stored PCD object URI: {minio_uri}")
                
                elif topic in ["lupus", "dahua", "zedx_left", "zedx_right", "zedx_top", "zedx_bottom"]:
                    last_timestamp = self.last_saved_timestamp.get(sensor_id_for_cassandra, None)

                    if last_timestamp is None or (timestamp_for_storage - last_timestamp).total_seconds() >= 0.5:
                        object_name = f"{sensor_id_for_cassandra}/{formatted_timestamp}.jpg"
                        minio_uri = self.upload_to_minio(consumer_config.MINIO_IMAGE_BUCKET, object_name, payload)

                        if minio_uri and not self.cassandra_session.is_shutdown:
                            self.cassandra_session.execute(prepared_stmt, (sensor_id_for_cassandra, timestamp_for_storage, minio_uri))
                            logging.info(f"Stored Image Data for {sensor_id_for_cassandra}: {minio_uri}")

                        self.last_saved_timestamp[sensor_id_for_cassandra] = timestamp_for_storage
                    else:
                        logging.info(f"Skipping image for {sensor_id_for_cassandra} at {timestamp_for_storage}, within 0.5s window.")
                
                elif topic == "flightradar-data":
                    try:
                        # Decode the payload to a string
                        json_payload_str = payload.decode('utf-8')
                        
                        # Use the prepared statement from `self.prepared_statements`
                        prepared_stmt = self.prepared_statements.get(target_table)
                        
                        # Pass the correct arguments to the execute call
                        self.cassandra_session.execute(
                            prepared_stmt, 
                            (
                                # You must provide the data in the order expected by the prepared statement
                                # which is (sensor_id, event_created, payload)
                                sensor_id_for_cassandra,
                                timestamp_for_storage,
                                json_payload_str
                            )
                        )
                        logging.info(f"Stored full JSON payload for flightradar-data.")
                    except (json.JSONDecodeError, UnicodeDecodeError, Exception) as e:
                        logging.error(f"Failed to process FlightRadar24 message: {e}")

                else:
                    logging.warning(f"Received message from unhandled topic: {topic}. Skipping.")

                self.consumer.commit()

        except KeyboardInterrupt:
            logging.info("\nConsumer interrupted by user.")
        except Exception as e:
            logging.error(f"An unexpected error occurred in the main consumer loop: {e}")
        finally:
            self.close()

    def upload_to_minio(self, bucket_name, object_name, data):
        """Uploads binary data to MinIO and returns the object's URI."""
        try:
            self.minio_client.put_object(
                bucket_name,
                object_name,
                io.BytesIO(data),
                length=len(data)
            )
            uri = f"http://{consumer_config.MINIO_ENDPOINT}/{bucket_name}/{object_name}"
            logging.debug(f"Object uploaded: {uri}")
            return uri
        except Exception as e:
            logging.error(f"Error uploading object to MinIO: {e}")
            return None
    
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
    consumer_app = KafkaCassandraConsumer(consumer_config.KAFKA_BROKERS, consumer_config.ALL_KAFKA_TOPICS, cassandra_host=consumer_config.CASSANDRA_HOSTS[0])
    consumer_app.consume_and_store_data()