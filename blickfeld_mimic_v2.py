import time
import secrets # Not used in this version, but kept for context.
import random
import signal # Not used in this version, but kept for context.
import datetime
from kafka import KafkaProducer # Import KafkaProducer from kafka
import pytz

LOCAL_TIMEZONE = pytz.timezone('Europe/Berlin')

# --- Configuration ---
KAFKA_BROKERS = ['localhost:9092'] # kafka-python expects a list of brokers
# This dictionary maps sensor IDs to Kafka topics.
# Since your code uses the sensor_id as the topic, we will use a set of valid IDs.
SENSOR_IDS = ['blickfeld', 'helios_1', 'helios_2']

# --- Dummy Data Generator ---
def generate_dummy_frame(num_points=1000):
    """
    Generates a list of dummy [x, y, z] points.
    Each coordinate is a random float between 0 and 100.
    """
    print(f"Generating {num_points} dummy points for the PCD header...")
    dummy_points = []
    for _ in range(num_points):
        x = round(random.uniform(0.0, 100.0), 3) 
        y = round(random.uniform(0.0, 100.0), 3) 
        z = round(random.uniform(0.0, 100.0), 3) 
        dummy_points.append([x, y, z])
    return dummy_points

# --- Generate point data once before the loop ---
# This is where the missing function is now called.
# The points remain constant for each message in the loop.
points = generate_dummy_frame(num_points=500)

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
            # Generate a new timestamp for each message
            timestamp_ms = int(time.time() * 1000)
            event_timestamp = datetime.datetime.fromtimestamp(timestamp_ms / 1000, tz=datetime.timezone.utc)
            localTime = event_timestamp.astimezone(LOCAL_TIMEZONE)
            
            # Use the pre-generated points to build the PCD header and body
            header = (
                f"# .PCD v0.7 - Point Cloud Data file format\n"
                f"# Event Created: {localTime}\n" # Using milliseconds for better resolution
                "VERSION 0.7\nFIELDS x y z\nSIZE 4 4 4\n"
                "TYPE F F F\nCOUNT 1 1 1\n"
                f"WIDTH {len(points)}\nHEIGHT 1\n"
                "VIEWPOINT 0 0 0 1 0 0 0\n"
                f"POINTS {len(points)}\nDATA ascii\n"
            )
            body = "\n".join(f"{x} {y} {z}" for x, y, z in points)
            message = header + body

            # Randomly select a sensor ID to create a dynamic topic
            sensor_id = random.choice(SENSOR_IDS)
            dynamic_topic = f"{sensor_id}"

            # Create a simple string message: "TIMESTAMP|HEX_DATA"
            # It's good practice to send the timestamp as a number to a consumer
            message_value = f"{message}"
            
            # Send the message.
            future = producer.send(
                topic=dynamic_topic,
                key=sensor_id.encode('utf-8'), # Key can also be bytes
                value=message_value # value is now automatically encoded by value_serializer
            )

            print(f"Sent message to topic '{dynamic_topic}' with key '{sensor_id}'.") 
            time.sleep(0.5) # Send every 0.5 seconds

    except KeyboardInterrupt:
        print("\nStopping producer.")
    finally:
        # Block until all async messages are sent
        producer.flush()
        print("Producer stopped.")
