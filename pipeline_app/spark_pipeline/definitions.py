from dataclasses import dataclass
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


ProjectionBuilder = Callable[[DataFrame], DataFrame]
EnrichBuilder = Callable[[DataFrame], DataFrame]

BOOTSTRAP_STARTING_OFFSETS = "earliest"
STREAM_STARTING_OFFSETS = "latest"
CDC_CONSOLE_STARTING_OFFSETS = "latest"


@dataclass(frozen=True)
class RawDatasetSpec:
    label: str
    topic_suffix: str
    raw_prefix: str
    checkpoint_name: str
    projection_builder: ProjectionBuilder
    enrich_builder: EnrichBuilder

    @property
    def topic_name(self) -> str:
        return f"public.{self.topic_suffix}"


CUSTOMERS_CDC_SCHEMA = StructType(
    [
        StructField("op", StringType(), True),
        StructField(
            "after",
            StructType(
                [
                    StructField("customer_id", StringType(), True),
                    StructField("first_name", StringType(), True),
                    StructField("last_name", StringType(), True),
                    StructField("email", StringType(), True),
                    StructField("phone", StringType(), True),
                    StructField("address", StringType(), True),
                    StructField("city", StringType(), True),
                    StructField("country", StringType(), True),
                    StructField("registration_date", LongType(), True),
                ]
            ),
            True,
        ),
    ]
)

PRODUCTS_CDC_SCHEMA = StructType(
    [
        StructField("op", StringType(), True),
        StructField(
            "after",
            StructType(
                [
                    StructField("product_id", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("category", StringType(), True),
                    StructField("price", DoubleType(), True),
                    StructField("cost", DoubleType(), True),
                    StructField("stock_quantity", LongType(), True),
                    StructField("created_at", LongType(), True),
                ]
            ),
            True,
        ),
    ]
)

ORDERS_CDC_SCHEMA = StructType(
    [
        StructField("op", StringType(), True),
        StructField(
            "after",
            StructType(
                [
                    StructField("order_id", StringType(), True),
                    StructField("customer_id", StringType(), True),
                    StructField("order_date", LongType(), True),
                    StructField("total_amount", DoubleType(), True),
                    StructField("status", StringType(), True),
                    StructField("shipping_address", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

PAYMENTS_CDC_SCHEMA = StructType(
    [
        StructField("op", StringType(), True),
        StructField(
            "after",
            StructType(
                [
                    StructField("payment_id", StringType(), True),
                    StructField("order_id", StringType(), True),
                    StructField("amount", DoubleType(), True),
                    StructField("payment_method", StringType(), True),
                    StructField("status", StringType(), True),
                    StructField("transaction_id", StringType(), True),
                    StructField("created_at", LongType(), True),
                    StructField("updated_at", LongType(), True),
                ]
            ),
            True,
        ),
    ]
)

ORDER_ITEMS_CDC_SCHEMA = StructType(
    [
        StructField("op", StringType(), True),
        StructField(
            "after",
            StructType(
                [
                    StructField("order_item_id", StringType(), True),
                    StructField("order_id", StringType(), True),
                    StructField("product_id", StringType(), True),
                    StructField("quantity", LongType(), True),
                    StructField("unit_price", DoubleType(), True),
                    StructField("total_price", DoubleType(), True),
                ]
            ),
            True,
        ),
    ]
)

SUPPORT_TICKETS_CDC_SCHEMA = StructType(
    [
        StructField("op", StringType(), True),
        StructField(
            "after",
            StructType(
                [
                    StructField("ticket_id", StringType(), True),
                    StructField("customer_id", StringType(), True),
                    StructField("order_id", StringType(), True),
                    StructField("subject", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("status", StringType(), True),
                    StructField("priority", StringType(), True),
                    StructField("assigned_to", StringType(), True),
                    StructField("created_at", LongType(), True),
                    StructField("resolved_at", LongType(), True),
                    StructField("resolution_time_minutes", LongType(), True),
                ]
            ),
            True,
        ),
    ]
)

INCIDENTS_CDC_SCHEMA = StructType(
    [
        StructField("op", StringType(), True),
        StructField(
            "after",
            StructType(
                [
                    StructField("incident_id", StringType(), True),
                    StructField("title", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("service", StringType(), True),
                    StructField("severity", StringType(), True),
                    StructField("status", StringType(), True),
                    StructField("start_time", LongType(), True),
                    StructField("end_time", LongType(), True),
                    StructField("resolution_time_minutes", LongType(), True),
                    StructField("impact_description", StringType(), True),
                    StructField("created_at", LongType(), True),
                    StructField("updated_at", LongType(), True),
                ]
            ),
            True,
        ),
    ]
)

MARKETING_SPEND_CDC_SCHEMA = StructType(
    [
        StructField("op", StringType(), True),
        StructField(
            "after",
            StructType(
                [
                    StructField("spend_id", StringType(), True),
                    StructField("campaign_name", StringType(), True),
                    StructField("channel", StringType(), True),
                    StructField("spend_date", LongType(), True),
                    StructField("amount", DoubleType(), True),
                    StructField("currency", StringType(), True),
                    StructField("impressions", LongType(), True),
                    StructField("clicks", LongType(), True),
                    StructField("conversions", LongType(), True),
                    StructField("created_at", LongType(), True),
                ]
            ),
            True,
        ),
    ]
)