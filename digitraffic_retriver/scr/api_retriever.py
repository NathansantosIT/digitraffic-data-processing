import os
import logging
from confluent_kafka import Producer
import requests
import json
import time
from typing import Optional, Dict, Any
from tenacity import retry, stop_after_attempt, wait_exponential

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'digitraffic_live_trains')
API_URL = "https://rata.digitraffic.fi/api/v1/live-trains/"

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def delivery_log(err: Optional[Exception], msg: Any) -> None:
    """Callback function to log the delivery result of a Kafka message."""
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=5, max=80))
def fetch_api_data() -> Optional[Dict[str, Any]]:
    """Fetch data from the API and return it as a JSON object."""
    logger.info("Starting to retrieve messages")
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        logger.info(f"Request successful with Status Code: {response.status_code}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data: {e}")
        raise

def send_to_kafka(producer: Producer, data: Dict[str, Any]) -> None:
    """Send data to Kafka."""
    if data:
        for item in data:
            message = json.dumps(item)
            producer.produce(KAFKA_TOPIC, message.encode('utf-8'), callback=delivery_log)
            producer.flush()
    else:
        logger.warning("No data fetched from API")

def main() -> None:
    """Main function to initialize the producer and send data to Kafka."""
    data = fetch_api_data()
    time.sleep(5)  # Sleep for 5 seconds to garantuee that Kafka is up and running

    producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    logger.info(f"Producer created with Kafka broker: {KAFKA_BROKER}")
    send_to_kafka(producer, data)

if __name__ == "__main__":
    main()