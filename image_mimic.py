import time
import random
import signal
import sys
import os
from datetime import datetime, timezone
import pytz

from kafka import KafkaProducer

LOCAL_TIMEZONE = pytz.timezone('Europe/Berlin')

# --- Kafka Configuration ---
KAFKA_BROKERS = ['localhost:9092']
KAFKA_PRODUCER = None

def get_kafka_producer():
    """Initializes and returns a Kafka producer instance."""
    global KAFKA_PRODUCER
    if KAFKA_PRODUCER is None:
        try:
            KAFKA_PRODUCER = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                retries=5,
                value_serializer=lambda v: v.encode('utf-8')
            )
            print("🟢 Successfully connected to Kafka producer.")
        except Exception as e:
            print(f"❌ Failed to connect to Kafka producer: {e}")
            sys.exit(1)
    return KAFKA_PRODUCER

def publish_kafka_message(topic, value):
    """
    Publishes a single message to a Kafka topic and prints its content to the terminal.
    """
    producer = get_kafka_producer()
    try:
        # Print the message to the terminal for troubleshooting
        print("-" * 50)
        print(f"Sending message to topic '{topic}':\n{value}")
        print("-" * 50)
        
        # Publish the message to Kafka
        producer.send(topic, value)
        producer.flush()
        print(f"✅ Published message successfully.")
        return True
    except Exception as e:
        print(f"❌ Failed to publish message to topic '{topic}': {e}")
        return False

# --- Dummy Data Generation Class ---
class DummyDataProducer:
    """
    Generates dummy data (timestamp + random hex string) and sends it to Kafka.
    """
    def __init__(self, topics, data_size_bytes=640*480*3):
        self.running = True
        self.topics = topics
        self.data_size_bytes = data_size_bytes
        signal.signal(signal.SIGINT, self._stop)
        signal.signal(signal.SIGTERM, self._stop)

    def _stop(self, signum, frame):
        """Signal handler for graceful shutdown."""
        print(f"\nSignal {signum} received. Stopping gracefully...")
        self.running = False
        if KAFKA_PRODUCER:
            KAFKA_PRODUCER.close()
            print("Kafka producer closed.")

    def _generate_dummy_data(self):
        """Generates a message with a timestamp and random hex string."""
        # Generate a timestamp string in the required format
        timestamp_str = datetime.now(LOCAL_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S.%f%z")
        timestamp_line = f"# Event Created: {timestamp_str}\n"
        
        # Generate a large random hex string
        random_bytes = os.urandom(self.data_size_bytes)
        hex_data = random_bytes.hex()
        
        # Combine the timestamp and the hex data
        message = timestamp_line + hex_data[:50]
        
        return message

    def run(self):
        """Main loop to generate and publish data."""
        print(f"Starting dummy data producer. Press Ctrl+C to stop.")
        while self.running:
            try:
                message = self._generate_dummy_data()
                
                # Randomly select a topic and publish
                topic = random.choice(self.topics)
                publish_kafka_message(topic, message)

            except Exception as e:
                print(f"❌ An error occurred: {e}")
            
            # Sleep for a bit to control the publishing rate
            time.sleep(1) # Publish one message every second


if __name__ == "__main__":
    topics_to_publish = ['lupus', 'dahua', 'zedx_top', 'zedx_bottom', 'zedx_left', 'zedx_right']
    producer = DummyDataProducer(topics_to_publish)
    producer.run()