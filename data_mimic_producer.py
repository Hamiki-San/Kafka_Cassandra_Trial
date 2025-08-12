import time
import secrets
import random
from kafka import KafkaProducer # Import KafkaProducer from kafka

# --- Configuration ---
KAFKA_BROKERS = ['localhost:9092'] # kafka-python expects a list of brokers
# KAFKA_TOPIC = 'raw_data'
HEX_DATA_BYTES = 512 # This means 512 bytes, which is 1024 hex characters

# --- Producer Setup ---
# KafkaProducer handles connections to Kafka brokers.
# value_serializer converts your message (e.g., string) to bytes.
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: v.encode('utf-8') # Automatically encode messages to utf-8 bytes
)

# --- Main Production Loop ---
if __name__ == '__main__':
    print(f"Producing to topic on '{KAFKA_BROKERS}' using kafka-python")
    try:
        while True:
            timestamp_ms = int(time.time() * 1000) # Current timestamp in milliseconds
            raw_hex_data = secrets.token_hex(HEX_DATA_BYTES)

            sensor_id = f"sensor_{random.randint(1, 3)}"
            dynamic_topic = f"{sensor_id}_raw_data"

            # Create a simple string message: "TIMESTAMP|HEX_DATA"
            message_value = f"{timestamp_ms}|{raw_hex_data}"

            # Send the message. It's asynchronous by default.
            # .get(timeout=10) can be used for synchronous sends, but we'll omit for simplicity.
            future = producer.send(
                topic=dynamic_topic,
                key=sensor_id.encode('utf-8'), # Key can also be bytes
                value=message_value # value is now automatically encoded by value_serializer
            )

            print(f"Sent: {timestamp_ms} | {raw_hex_data[:50]}... (length {len(raw_hex_data)})") # Show first 20 chars
            time.sleep(1) # Send every 1 second

    except KeyboardInterrupt:
        print("\nStopping producer.")
    finally:
        # Block until all async messages are sent
        producer.flush()
        print("Producer stopped.")