import logging
import time
from contextlib import contextmanager
import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
from pipeline_app.config import get_settings
logger = logging.getLogger(__name__)

@contextmanager
def get_db_connection():
    settings = get_settings()
    connection = psycopg2.connect(
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        host=settings.db_host,
        port=settings.db_port,
    )
    try:
        yield connection
    finally:
        connection.close()

def wait_for_database(max_retries: int = 30, delay_seconds: int = 2) -> None:
    for attempt in range(1, max_retries + 1):
        try:
            with get_db_connection():
                logger.info("postgres is ready")
                return
        except Exception as error:
            logger.info("waiting for postgres attempt %s/%s: %s", attempt, max_retries, error)
            time.sleep(delay_seconds)
    raise RuntimeError("postgres did not become ready in time")

def create_source_tables() -> None:
    statements = [
        """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id VARCHAR(36) PRIMARY KEY,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            email VARCHAR(255) UNIQUE,
            phone VARCHAR(50),
            address TEXT,
            city VARCHAR(100),
            country VARCHAR(100),
            registration_date TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id VARCHAR(36) PRIMARY KEY,
            name VARCHAR(255),
            category VARCHAR(100),
            price DECIMAL(10, 2),
            cost DECIMAL(10, 2),
            stock_quantity INTEGER,
            created_at TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS orders (
            order_id VARCHAR(36) PRIMARY KEY,
            customer_id VARCHAR(36) REFERENCES customers(customer_id),
            order_date TIMESTAMP,
            total_amount DECIMAL(10, 2),
            status VARCHAR(50),
            shipping_address TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS order_items (
            order_item_id VARCHAR(36) PRIMARY KEY,
            order_id VARCHAR(36) REFERENCES orders(order_id),
            product_id VARCHAR(36) REFERENCES products(product_id),
            quantity INTEGER,
            unit_price DECIMAL(10, 2),
            total_price DECIMAL(10, 2)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS payments (
            payment_id VARCHAR(36) PRIMARY KEY,
            order_id VARCHAR(36) REFERENCES orders(order_id),
            amount DECIMAL(10, 2) NOT NULL,
            payment_method VARCHAR(50),
            status VARCHAR(50),
            transaction_id VARCHAR(100),
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS incidents (
            incident_id VARCHAR(36) PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            description TEXT,
            service VARCHAR(100) NOT NULL,
            severity VARCHAR(20),
            status VARCHAR(20),
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP,
            resolution_time_minutes INTEGER,
            impact_description TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS support_tickets (
            ticket_id VARCHAR(36) PRIMARY KEY,
            customer_id VARCHAR(36) REFERENCES customers(customer_id),
            order_id VARCHAR(36) REFERENCES orders(order_id),
            subject VARCHAR(255),
            description TEXT,
            status VARCHAR(50),
            priority VARCHAR(20),
            assigned_to VARCHAR(100),
            created_at TIMESTAMP,
            resolved_at TIMESTAMP,
            resolution_time_minutes INTEGER
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS marketing_spend (
            spend_id VARCHAR(36) PRIMARY KEY,
            campaign_name VARCHAR(255),
            channel VARCHAR(100),
            spend_date DATE,
            amount DECIMAL(10, 2),
            currency VARCHAR(3),
            impressions INTEGER,
            clicks INTEGER,
            conversions INTEGER,
            created_at TIMESTAMP
        )
        """,
    ]

    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
        connection.commit()
    logger.info("source tables are ready")

def table_has_rows(table_name: str) -> bool:
    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT EXISTS (SELECT 1 FROM {table_name} LIMIT 1)")
            return bool(cursor.fetchone()[0])

def insert_batch(table_name: str, records: list[dict]) -> None:
    if not records:
        return
    columns = list(records[0].keys())
    placeholders = ", ".join(["%s"] * len(columns))
    quoted_columns = ", ".join(f'"{column}"' for column in columns)
    query = f"INSERT INTO {table_name} ({quoted_columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
    values = [[record.get(column) for column in columns] for record in records]
    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            execute_batch(cursor, query, values, page_size=100)
        connection.commit()
    logger.info("inserted %s rows into %s", len(records), table_name)

def fetch_rows(query: str, params: tuple | None = None) -> list[dict]:
    with get_db_connection() as connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params or ())
            return [dict(row) for row in cursor.fetchall()]