from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipeline_app.spark_pipeline.definitions import (
    CUSTOMERS_CDC_SCHEMA,
    INCIDENTS_CDC_SCHEMA,
    MARKETING_SPEND_CDC_SCHEMA,
    ORDER_ITEMS_CDC_SCHEMA,
    ORDERS_CDC_SCHEMA,
    PAYMENTS_CDC_SCHEMA,
    PRODUCTS_CDC_SCHEMA,
    SUPPORT_TICKETS_CDC_SCHEMA,
)


def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("year", F.date_format(F.col("_ingested_at"), "yyyy"))
        .withColumn("month", F.date_format(F.col("_ingested_at"), "MM"))
        .withColumn("day", F.date_format(F.col("_ingested_at"), "dd"))
        .withColumn("hour", F.date_format(F.col("_ingested_at"), "HH"))
    )


def epoch_millis_to_timestamp(column_name: str):
    return F.to_timestamp(F.from_unixtime(F.col(column_name) / F.lit(1000)))


def enrich_web_events_for_landing(web_events: DataFrame) -> DataFrame:
    return add_ingestion_metadata(
        web_events.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
    )


def enrich_customers_for_landing(customers: DataFrame) -> DataFrame:
    return add_ingestion_metadata(
        customers.withColumn("registration_date", epoch_millis_to_timestamp("registration_date_ms"))
    ).drop("registration_date_ms")


def enrich_products_for_landing(products: DataFrame) -> DataFrame:
    return add_ingestion_metadata(
        products.withColumn("created_at", epoch_millis_to_timestamp("created_at_ms"))
    ).drop("created_at_ms")


def enrich_orders_for_landing(orders: DataFrame) -> DataFrame:
    return add_ingestion_metadata(
        orders.withColumn("order_date", epoch_millis_to_timestamp("order_date_ms"))
    ).drop("order_date_ms")


def enrich_payments_for_landing(payments: DataFrame) -> DataFrame:
    return add_ingestion_metadata(
        payments.withColumn("created_at", epoch_millis_to_timestamp("created_at_ms")).withColumn(
            "updated_at", epoch_millis_to_timestamp("updated_at_ms")
        )
    ).drop("created_at_ms", "updated_at_ms")


def enrich_order_items_for_landing(order_items: DataFrame) -> DataFrame:
    return add_ingestion_metadata(order_items)


def enrich_support_tickets_for_landing(tickets: DataFrame) -> DataFrame:
    return add_ingestion_metadata(
        tickets.withColumn("created_at", epoch_millis_to_timestamp("created_at_ms")).withColumn(
            "resolved_at",
            F.when(F.col("resolved_at_ms").isNotNull(), epoch_millis_to_timestamp("resolved_at_ms")),
        )
    ).drop("created_at_ms", "resolved_at_ms")


def enrich_incidents_for_landing(incidents: DataFrame) -> DataFrame:
    return add_ingestion_metadata(
        incidents.withColumn("start_time", epoch_millis_to_timestamp("start_time_ms"))
        .withColumn(
            "end_time",
            F.when(F.col("end_time_ms").isNotNull(), epoch_millis_to_timestamp("end_time_ms")),
        )
        .withColumn("created_at", epoch_millis_to_timestamp("created_at_ms"))
        .withColumn("updated_at", epoch_millis_to_timestamp("updated_at_ms"))
    ).drop("start_time_ms", "end_time_ms", "created_at_ms", "updated_at_ms")


def enrich_marketing_spend_for_landing(spend: DataFrame) -> DataFrame:
    return add_ingestion_metadata(
        spend.withColumn(
            "spend_date",
            F.expr("date_add(to_date('1970-01-01'), cast(spend_date_days as int))"),
        ).withColumn("created_at", epoch_millis_to_timestamp("created_at_ms"))
    ).drop("spend_date_days", "created_at_ms")


def build_web_events_projection(source_df: DataFrame) -> DataFrame:
    return source_df.select(
        F.col("timestamp").alias("kafka_timestamp"),
        F.get_json_object(F.col("value").cast("string"), "$.event_id").alias("event_id"),
        F.get_json_object(F.col("value").cast("string"), "$.session_id").alias("session_id"),
        F.get_json_object(F.col("value").cast("string"), "$.customer_id").alias("customer_id"),
        F.get_json_object(F.col("value").cast("string"), "$.event_type").alias("event_type"),
        F.get_json_object(F.col("value").cast("string"), "$.event_timestamp").alias("event_timestamp"),
    )


def build_customers_projection(source_df: DataFrame) -> DataFrame:
    return (
        source_df.select(
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), CUSTOMERS_CDC_SCHEMA).alias("payload"),
        )
        .select(
            "kafka_timestamp",
            F.col("payload.op").alias("op"),
            F.col("payload.after.customer_id").alias("customer_id"),
            F.col("payload.after.first_name").alias("first_name"),
            F.col("payload.after.last_name").alias("last_name"),
            F.col("payload.after.email").alias("email"),
            F.col("payload.after.phone").alias("phone"),
            F.col("payload.after.address").alias("address"),
            F.col("payload.after.city").alias("city"),
            F.col("payload.after.country").alias("country"),
            F.col("payload.after.registration_date").alias("registration_date_ms"),
        )
        .filter(F.col("customer_id").isNotNull())
    )


def build_products_projection(source_df: DataFrame) -> DataFrame:
    return (
        source_df.select(
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), PRODUCTS_CDC_SCHEMA).alias("payload"),
        )
        .select(
            "kafka_timestamp",
            F.col("payload.op").alias("op"),
            F.col("payload.after.product_id").alias("product_id"),
            F.col("payload.after.name").alias("name"),
            F.col("payload.after.category").alias("category"),
            F.col("payload.after.price").alias("price"),
            F.col("payload.after.cost").alias("cost"),
            F.col("payload.after.stock_quantity").alias("stock_quantity"),
            F.col("payload.after.created_at").alias("created_at_ms"),
        )
        .filter(F.col("product_id").isNotNull())
    )


def build_orders_projection(source_df: DataFrame) -> DataFrame:
    return (
        source_df.select(
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), ORDERS_CDC_SCHEMA).alias("payload"),
        )
        .select(
            "kafka_timestamp",
            F.col("payload.op").alias("op"),
            F.col("payload.after.order_id").alias("order_id"),
            F.col("payload.after.customer_id").alias("customer_id"),
            F.col("payload.after.order_date").alias("order_date_ms"),
            F.col("payload.after.total_amount").alias("total_amount"),
            F.col("payload.after.status").alias("status"),
            F.col("payload.after.shipping_address").alias("shipping_address"),
        )
        .filter(F.col("order_id").isNotNull())
    )


def build_payments_projection(source_df: DataFrame) -> DataFrame:
    return (
        source_df.select(
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), PAYMENTS_CDC_SCHEMA).alias("payload"),
        )
        .select(
            "kafka_timestamp",
            F.col("payload.op").alias("op"),
            F.col("payload.after.payment_id").alias("payment_id"),
            F.col("payload.after.order_id").alias("order_id"),
            F.col("payload.after.amount").alias("amount"),
            F.col("payload.after.payment_method").alias("payment_method"),
            F.col("payload.after.status").alias("status"),
            F.col("payload.after.transaction_id").alias("transaction_id"),
            F.col("payload.after.created_at").alias("created_at_ms"),
            F.col("payload.after.updated_at").alias("updated_at_ms"),
        )
        .filter(F.col("payment_id").isNotNull())
    )


def build_order_items_projection(source_df: DataFrame) -> DataFrame:
    return (
        source_df.select(
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), ORDER_ITEMS_CDC_SCHEMA).alias("payload"),
        )
        .select(
            "kafka_timestamp",
            F.col("payload.op").alias("op"),
            F.col("payload.after.order_item_id").alias("order_item_id"),
            F.col("payload.after.order_id").alias("order_id"),
            F.col("payload.after.product_id").alias("product_id"),
            F.col("payload.after.quantity").alias("quantity"),
            F.col("payload.after.unit_price").alias("unit_price"),
            F.col("payload.after.total_price").alias("total_price"),
        )
        .filter(F.col("order_item_id").isNotNull())
    )


def build_support_tickets_projection(source_df: DataFrame) -> DataFrame:
    return (
        source_df.select(
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), SUPPORT_TICKETS_CDC_SCHEMA).alias("payload"),
        )
        .select(
            "kafka_timestamp",
            F.col("payload.op").alias("op"),
            F.col("payload.after.ticket_id").alias("ticket_id"),
            F.col("payload.after.customer_id").alias("customer_id"),
            F.col("payload.after.order_id").alias("order_id"),
            F.col("payload.after.subject").alias("subject"),
            F.col("payload.after.description").alias("description"),
            F.col("payload.after.status").alias("status"),
            F.col("payload.after.priority").alias("priority"),
            F.col("payload.after.assigned_to").alias("assigned_to"),
            F.col("payload.after.created_at").alias("created_at_ms"),
            F.col("payload.after.resolved_at").alias("resolved_at_ms"),
            F.col("payload.after.resolution_time_minutes").alias("resolution_time_minutes"),
        )
        .filter(F.col("ticket_id").isNotNull())
    )


def build_incidents_projection(source_df: DataFrame) -> DataFrame:
    return (
        source_df.select(
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), INCIDENTS_CDC_SCHEMA).alias("payload"),
        )
        .select(
            "kafka_timestamp",
            F.col("payload.op").alias("op"),
            F.col("payload.after.incident_id").alias("incident_id"),
            F.col("payload.after.title").alias("title"),
            F.col("payload.after.description").alias("description"),
            F.col("payload.after.service").alias("service"),
            F.col("payload.after.severity").alias("severity"),
            F.col("payload.after.status").alias("status"),
            F.col("payload.after.start_time").alias("start_time_ms"),
            F.col("payload.after.end_time").alias("end_time_ms"),
            F.col("payload.after.resolution_time_minutes").alias("resolution_time_minutes"),
            F.col("payload.after.impact_description").alias("impact_description"),
            F.col("payload.after.created_at").alias("created_at_ms"),
            F.col("payload.after.updated_at").alias("updated_at_ms"),
        )
        .filter(F.col("incident_id").isNotNull())
    )


def build_marketing_spend_projection(source_df: DataFrame) -> DataFrame:
    return (
        source_df.select(
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), MARKETING_SPEND_CDC_SCHEMA).alias("payload"),
        )
        .select(
            "kafka_timestamp",
            F.col("payload.op").alias("op"),
            F.col("payload.after.spend_id").alias("spend_id"),
            F.col("payload.after.campaign_name").alias("campaign_name"),
            F.col("payload.after.channel").alias("channel"),
            F.col("payload.after.spend_date").alias("spend_date_days"),
            F.col("payload.after.amount").alias("amount"),
            F.col("payload.after.currency").alias("currency"),
            F.col("payload.after.impressions").alias("impressions"),
            F.col("payload.after.clicks").alias("clicks"),
            F.col("payload.after.conversions").alias("conversions"),
            F.col("payload.after.created_at").alias("created_at_ms"),
        )
        .filter(F.col("spend_id").isNotNull())
    )