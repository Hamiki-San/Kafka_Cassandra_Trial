# Import necessary libraries
import cv2
import time
from kafka import KafkaProducer
import logging

# Set up logging for better visibility of what's happening
logging.basicConfig(level=logging.INFO)

# --- Configuration ---
# Set the Kafka broker address
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Update if your Kafka broker is on a different host
# Set the Kafka topic to publish messages to
KAFKA_TOPIC = 'webcam-stream'
# Set the desired frame rate in frames per second (Hz)
FRAME_RATE = 2 # 2 frames per second
INTERVAL = 1 / FRAME_RATE # Time to wait between frames

# --- Initialize Kafka Producer ---
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        # A simple serializer to ensure the data is sent as bytes
        value_serializer=lambda x: x
    )
    logging.info("Kafka producer initialized successfully.")
except Exception as e:
    logging.error(f"Failed to initialize Kafka producer: {e}")
    exit()

# --- Initialize Webcam ---
# cv2.VideoCapture(0) accesses the default webcam.
# If you have multiple webcams, you might need to change the number (e.g., 1, 2, etc.).
cap = cv2.VideoCapture(0)
if not cap.isOpened():
    logging.error("Error: Could not open webcam.")
    exit()
else:
    logging.info("Webcam opened successfully.")

# --- Stream the Frames ---
try:
    while True:
        # Capture frame-by-frame
        ret, frame = cap.read()
        
        # Check if the frame was captured successfully
        if not ret:
            logging.warning("Failed to capture frame from webcam. Retrying...")
            continue
            
        # Encode the frame into a JPEG format.
        # This is a good choice for images as it provides a good balance of size and quality.
        _, buffer = cv2.imencode('.jpg', frame)
        
        # Convert the buffer to a bytes object
        image_bytes = buffer.tobytes()
        
        # Log the size of the image being sent
        logging.info(f"Sending frame of size {len(image_bytes)} bytes to topic '{KAFKA_TOPIC}'...")

        # Send the frame to the Kafka topic
        producer.send(KAFKA_TOPIC, value=image_bytes)
        
        # To ensure the message is actually sent before the next iteration,
        # we can flush the producer. This is useful for high-throughput scenarios.
        producer.flush()

        # Enforce the desired frame rate by waiting for the remaining time
        time.sleep(INTERVAL)

        # Break the loop if 'q' key is pressed
        # Note: This requires a window to be displayed.
        # If you're running this headless, you may want a different exit condition.
        # Here we'll just check for a 'q' key press
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break

except KeyboardInterrupt:
    logging.info("Streaming stopped by user.")
except Exception as e:
    logging.error(f"An error occurred during streaming: {e}")

finally:
    # Release the webcam and close the producer
    logging.info("Releasing webcam and closing producer...")
    cap.release()
    producer.close()
    cv2.destroyAllWindows()
    logging.info("Cleanup complete. Exiting.")
