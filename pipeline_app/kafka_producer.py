import json
import logging
import time
from kafka import KafkaProducer
from pipeline_app.config import get_settings
from pipeline_app.generator import generate_web_event_batch

logger = logging.getLogger(__name__)

class WebEventProducer:
    def __init__(self) -> None:
        settings = get_settings()
        self.topic = settings.kafka_web_events_topic
        self.producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            acks="all",
            retries=5,
            max_in_flight_requests_per_connection=1,
            request_timeout_ms=10000,
        )

    def send_event(self, event: dict) -> None:
        payload = dict(event)
        timestamp = payload.get("event_timestamp")
        if hasattr(timestamp, "isoformat"):
            payload["event_timestamp"] = timestamp.isoformat()
        future = self.producer.send(self.topic, value=payload)
        future.get(timeout=10)

    def send_batch(self, events: list[dict]) -> None:
        for event in events:
            self.send_event(event)
        self.producer.flush(timeout=10)

    def close(self) -> None:
        self.producer.flush(timeout=10)
        self.producer.close(timeout=5)

def publish_web_events_forever(customers: list[dict]) -> None:
    producer = WebEventProducer()
    logger.info("starting direct web_events publishing loop")
    try:
        while True:
            producer.send_batch(generate_web_event_batch(customers))
            time.sleep(1)
    finally:
        producer.close()