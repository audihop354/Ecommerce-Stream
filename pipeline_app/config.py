import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

@dataclass(frozen=True)
class Settings:
    postgres_db: str = os.environ["POSTGRES_DB"]
    postgres_user: str = os.environ["POSTGRES_USER"]
    postgres_password: str = os.environ["POSTGRES_PASSWORD"]
    db_host: str = os.environ["DB_HOST"]
    db_port: int = int(os.environ["DB_PORT"])
    kafka_bootstrap_servers: str = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    kafka_web_events_topic: str = os.environ["KAFKA_WEB_EVENTS_TOPIC"]
    debezium_url: str = os.environ["DEBEZIUM_URL"]
    debezium_connector_name: str = os.environ["DEBEZIUM_CONNECTOR_NAME"]
    debezium_topic_prefix: str = os.environ["DEBEZIUM_TOPIC_PREFIX"]
    minio_endpoint: str = os.environ["MINIO_ENDPOINT"]
    minio_access_key: str = os.environ["MINIO_ACCESS_KEY"]
    minio_secret_key: str = os.environ["MINIO_SECRET_KEY"]
    minio_bucket: str = os.environ["MINIO_BUCKET"]

def get_settings() -> Settings:
    return Settings()