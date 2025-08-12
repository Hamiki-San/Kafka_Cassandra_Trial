import time
from kafka import KafkaConsumer # Import KafkaConsumer
import json # You'll need this if your producer sends JSON, otherwise simple split
import sys

# --- Configuration ---
KAFKA_BROKERS = ['localhost:9092'] # Must match your Kafka setup
# KAFKA_TOPIC = 'raw_data' # Must match the topic your producer sends to
CONSUMER_GROUP_ID = 'raw_data_processing_group' # A unique group ID for this consumer instance

# --- Consumer Setup ---
# KafkaConsumer handles connections and consuming messages.
# value_deserializer converts incoming bytes back to a usable format (e.g., string).
consumer = KafkaConsumer(
    'sensor_1_raw_data',
    'sensor_2_raw_data',
    'sensor_3_raw_data',
    bootstrap_servers=KAFKA_BROKERS,
    group_id=CONSUMER_GROUP_ID,
    # auto_offset_reset='earliest' will start reading from the beginning of the topic
    # if no offset is committed for this consumer group. 'latest' (default) starts from new messages.
    auto_offset_reset='earliest',
    enable_auto_commit=True, # Automatically commit offsets to Kafka
    value_deserializer=lambda x: x.decode('utf-8') # Decode message value from bytes to UTF-8 string
)

# --- Main Consumption Loop ---
if __name__ == '__main__':
    print(f"Starting Kafka Consumer on brokers: '{KAFKA_BROKERS}'")
    print(f"Using consumer group ID: '{CONSUMER_GROUP_ID}'")

    try:
        # Iterate over messages as they arrive
        for message in consumer:
            # The 'message' object has attributes like .topic, .partition, .offset, .key, .value

            # Our producer sends messages in "TIMESTAMP|HEX_DATA" format.
            # So, we split the string value.
            message_parts = message.value.split('|', 1) # Split only on the first '|'

            if len(message_parts) == 2:
                timestamp_str = message_parts[0]
                raw_hex_data = message_parts[1]

                try:
                    timestamp_ms = int(timestamp_str)
                    print(f"Consumed message: Key='{message.key.decode('utf-8') if message.key else 'None'}', "
                          f"Timestamp={timestamp_ms}, Raw_Data_Length={len(raw_hex_data)}, "
                          f"Offset={message.offset}")
                    # Here you can process the timestamp_ms and raw_hex_data
                    # For example, store it in your Cassandra database.

                except ValueError:
                    print(f"Error parsing timestamp: {timestamp_str}")
                except Exception as e:
                    print(f"Error processing message: {e}")
            else:
                print(f"Skipping malformed message: {message.value}")

    except KeyboardInterrupt:
        print("\nStopping consumer.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        consumer.close() # Ensure the consumer closes connections cleanly
        print("Consumer stopped.")