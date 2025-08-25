# --- Kafka Configuration ---
KAFKA_BROKERS = ['localhost:9092']

# --- Cassandra Configuration ---
CASSANDRA_HOSTS = ['localhost']
CASSANDRA_PORT = 9042
CASSANDRA_USERNAME = 'cassandra'
CASSANDRA_PASSWORD = 'cassandra'
KEYSPACE_NAME = 'sensor_data'

# --- MinIO Configuration ---
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "fastgate"
MINIO_SECRET_KEY = "fastgate"
MINIO_SECURE = False
MINIO_PCD_BUCKET = "pcd-data"
MINIO_IMAGE_BUCKET = "image-data"

# --- Sensor to Table Mapping ---
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
}

# --- Automatically generated list of Kafka topics ---
ALL_KAFKA_TOPICS = list(TOPIC_TO_TABLE_MAPPING.keys())