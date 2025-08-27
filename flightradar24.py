import time
import json
import logging
from datetime import datetime
from FlightRadar24 import FlightRadar24API
from kafka import KafkaProducer

# Import the configuration from a separate file
from fr24config import (
    KAFKA_BROKERS, 
    OUTPUT_KAFKA_TOPIC_FLIGHTRADAR, 
    AIRPORT_CODE
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def publish_kafka_message(topic: str, message: dict):
    """
    Publishes a JSON-formatted message to a Kafka topic.
    """
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        producer.send(topic, value=message)
        producer.flush()
        return True
    except Exception as e:
        logging.error(f"❌ Error publishing to Kafka: {e}")
        return False
    finally:
        if producer:
            producer.close()

def fetch_and_publish_arrivals():
    """
    Fetches arrival data for a specified airport and publishes it to Kafka.
    """
    fr_api = FlightRadar24API()
    if not AIRPORT_CODE:
        logging.error("❌ Missing airport code in configuration.")
        return False

    logging.info(f"⏳ Fetching arrival flights for airport: {AIRPORT_CODE}")

    try:
        flights = fr_api.get_flights(bounds=None)
    except Exception as e:
        logging.error(f"❌ FR24 API error: {e}")
        return False

    arrivals = []
    for f in flights:
        if getattr(f, "destination_airport_iata", "").upper() != AIRPORT_CODE.upper():
            continue
        
        # Access the correct time attribute
        scheduled_timestamp = getattr(f, "time_scheduled_arrival", None)
        
        # Convert the Unix timestamp to a readable datetime string
        scheduled_datetime = None
        if scheduled_timestamp:
            try:
                scheduled_datetime = datetime.fromtimestamp(scheduled_timestamp).isoformat()
            except (ValueError, TypeError) as e:
                logging.warning(f"Failed to convert timestamp for flight {getattr(f, 'id', 'unknown')}: {e}")
                scheduled_datetime = None
        
        # 1. Try to use the most reliable method for getting status
        status_text = None
        if hasattr(f, "get_status_as_dict"):
            status_info = f.get_status_as_dict()
            status_text = status_info.get('text')
        # 2. As a fallback, try to access the raw status attribute
        elif hasattr(f, "status") and hasattr(f.status, "text"):
            status_text = f.status.text
        # 3. Last resort, check if the raw status is a string
        elif hasattr(f, "status") and isinstance(f.status, str):
            status_text = f.status
        
        arrivals.append({
            "flight_id": getattr(f, "id", None),
            "callsign": getattr(f, "callsign", None),
            "origin": getattr(f, "origin_airport_iata", None),
            "scheduled": scheduled_datetime,
            "status": status_text,
        })

    if not arrivals:
        logging.info(f"ℹ️ No arrivals found for {AIRPORT_CODE}. Nothing to publish.")
        return True

    payload = {
        "airport": AIRPORT_CODE,
        "count": len(arrivals),
        "arrivals": arrivals
    }

    # Publish the payload to Kafka
    success = publish_kafka_message(OUTPUT_KAFKA_TOPIC_FLIGHTRADAR, payload)

    if success:
        logging.info(f"✅ Successfully published {len(arrivals)} arrivals for {AIRPORT_CODE} to Kafka topic '{OUTPUT_KAFKA_TOPIC_FLIGHTRADAR}'.")
        return True
    else:
        logging.error("❌ Failed to publish message to Kafka.")
        return False

if __name__ == "__main__":
    while True:
        fetch_and_publish_arrivals()
        logging.info("--- Waiting for 60 seconds before next fetch ---")
        time.sleep(60)