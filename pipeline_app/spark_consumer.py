import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
from pipeline_app.config import get_settings
from pipeline_app.storage import build_s3_client

CUSTOMERS_BOOTSTRAP_STARTING_OFFSETS = "earliest"
CUSTOMERS_STREAM_STARTING_OFFSETS = "latest"
PRODUCTS_BOOTSTRAP_STARTING_OFFSETS = "earliest"
PRODUCTS_STREAM_STARTING_OFFSETS = "latest"
ORDERS_BOOTSTRAP_STARTING_OFFSETS = "earliest"
ORDERS_STREAM_STARTING_OFFSETS = "latest"
ORDER_ITEMS_BOOTSTRAP_STARTING_OFFSETS = "earliest"
ORDER_ITEMS_STREAM_STARTING_OFFSETS = "latest"
PAYMENTS_BOOTSTRAP_STARTING_OFFSETS = "earliest"
PAYMENTS_STREAM_STARTING_OFFSETS = "latest"
SUPPORT_TICKETS_BOOTSTRAP_STARTING_OFFSETS = "earliest"
SUPPORT_TICKETS_STREAM_STARTING_OFFSETS = "latest"
INCIDENTS_BOOTSTRAP_STARTING_OFFSETS = "earliest"
INCIDENTS_STREAM_STARTING_OFFSETS = "latest"
MARKETING_SPEND_BOOTSTRAP_STARTING_OFFSETS = "earliest"
MARKETING_SPEND_STREAM_STARTING_OFFSETS = "latest"
CDC_CONSOLE_STARTING_OFFSETS = "latest"

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

def build_spark_session() -> SparkSession:
    settings = get_settings()
    spark = SparkSession.builder.appName("ecom-spark-consumer").getOrCreate()
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", settings.minio_endpoint)
    hadoop_conf.set("fs.s3a.access.key", settings.minio_access_key)
    hadoop_conf.set("fs.s3a.secret.key", settings.minio_secret_key)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    return spark

def enrich_for_landing(web_events):
    return (
        web_events.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("year", F.date_format(F.col("_ingested_at"), "yyyy"))
        .withColumn("month", F.date_format(F.col("_ingested_at"), "MM"))
        .withColumn("day", F.date_format(F.col("_ingested_at"), "dd"))
        .withColumn("hour", F.date_format(F.col("_ingested_at"), "HH"))
    )

def enrich_customers_for_landing(customers):
    return (
        customers.withColumn("registration_date", F.to_timestamp(F.from_unixtime(F.col("registration_date_ms") / F.lit(1000))))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("year", F.date_format(F.col("_ingested_at"), "yyyy"))
        .withColumn("month", F.date_format(F.col("_ingested_at"), "MM"))
        .withColumn("day", F.date_format(F.col("_ingested_at"), "dd"))
        .withColumn("hour", F.date_format(F.col("_ingested_at"), "HH"))
        .drop("registration_date_ms")
    )

def enrich_products_for_landing(products):
    return (
        products.withColumn("created_at", F.to_timestamp(F.from_unixtime(F.col("created_at_ms") / F.lit(1000))))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("year", F.date_format(F.col("_ingested_at"), "yyyy"))
        .withColumn("month", F.date_format(F.col("_ingested_at"), "MM"))
        .withColumn("day", F.date_format(F.col("_ingested_at"), "dd"))
        .withColumn("hour", F.date_format(F.col("_ingested_at"), "HH"))
        .drop("created_at_ms")
    )

def enrich_orders_for_landing(orders):
    return (
        orders.withColumn("order_date", F.to_timestamp(F.from_unixtime(F.col("order_date_ms") / F.lit(1000))))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("year", F.date_format(F.col("_ingested_at"), "yyyy"))
        .withColumn("month", F.date_format(F.col("_ingested_at"), "MM"))
        .withColumn("day", F.date_format(F.col("_ingested_at"), "dd"))
        .withColumn("hour", F.date_format(F.col("_ingested_at"), "HH"))
        .drop("order_date_ms")
    )

def enrich_payments_for_landing(payments):
    return (
        payments.withColumn("created_at", F.to_timestamp(F.from_unixtime(F.col("created_at_ms") / F.lit(1000))))
        .withColumn("updated_at", F.to_timestamp(F.from_unixtime(F.col("updated_at_ms") / F.lit(1000))))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("year", F.date_format(F.col("_ingested_at"), "yyyy"))
        .withColumn("month", F.date_format(F.col("_ingested_at"), "MM"))
        .withColumn("day", F.date_format(F.col("_ingested_at"), "dd"))
        .withColumn("hour", F.date_format(F.col("_ingested_at"), "HH"))
        .drop("created_at_ms", "updated_at_ms")
    )

def enrich_order_items_for_landing(order_items):
    return (
        order_items.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("year", F.date_format(F.col("_ingested_at"), "yyyy"))
        .withColumn("month", F.date_format(F.col("_ingested_at"), "MM"))
        .withColumn("day", F.date_format(F.col("_ingested_at"), "dd"))
        .withColumn("hour", F.date_format(F.col("_ingested_at"), "HH"))
    )

def enrich_support_tickets_for_landing(tickets):
    return (
        tickets.withColumn("created_at", F.to_timestamp(F.from_unixtime(F.col("created_at_ms") / F.lit(1000))))
        .withColumn(
            "resolved_at",
            F.when(
                F.col("resolved_at_ms").isNotNull(),
                F.to_timestamp(F.from_unixtime(F.col("resolved_at_ms") / F.lit(1000))),
            ),
        )
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("year", F.date_format(F.col("_ingested_at"), "yyyy"))
        .withColumn("month", F.date_format(F.col("_ingested_at"), "MM"))
        .withColumn("day", F.date_format(F.col("_ingested_at"), "dd"))
        .withColumn("hour", F.date_format(F.col("_ingested_at"), "HH"))
        .drop("created_at_ms", "resolved_at_ms")
    )

def enrich_incidents_for_landing(incidents):
    return (
        incidents.withColumn("start_time", F.to_timestamp(F.from_unixtime(F.col("start_time_ms") / F.lit(1000))))
        .withColumn(
            "end_time",
            F.when(
                F.col("end_time_ms").isNotNull(),
                F.to_timestamp(F.from_unixtime(F.col("end_time_ms") / F.lit(1000))),
            ),
        )
        .withColumn("created_at", F.to_timestamp(F.from_unixtime(F.col("created_at_ms") / F.lit(1000))))
        .withColumn("updated_at", F.to_timestamp(F.from_unixtime(F.col("updated_at_ms") / F.lit(1000))))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("year", F.date_format(F.col("_ingested_at"), "yyyy"))
        .withColumn("month", F.date_format(F.col("_ingested_at"), "MM"))
        .withColumn("day", F.date_format(F.col("_ingested_at"), "dd"))
        .withColumn("hour", F.date_format(F.col("_ingested_at"), "HH"))
        .drop("start_time_ms", "end_time_ms", "created_at_ms", "updated_at_ms")
    )

def enrich_marketing_spend_for_landing(spend):
    return (
        spend.withColumn("spend_date", F.expr("date_add(to_date('1970-01-01'), cast(spend_date_days as int))"))
        .withColumn("created_at", F.to_timestamp(F.from_unixtime(F.col("created_at_ms") / F.lit(1000))))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("year", F.date_format(F.col("_ingested_at"), "yyyy"))
        .withColumn("month", F.date_format(F.col("_ingested_at"), "MM"))
        .withColumn("day", F.date_format(F.col("_ingested_at"), "dd"))
        .withColumn("hour", F.date_format(F.col("_ingested_at"), "HH"))
        .drop("spend_date_days", "created_at_ms")
    )

def build_customers_projection(source_df):
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

def build_products_projection(source_df):
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

def build_orders_projection(source_df):
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

def build_payments_projection(source_df):
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

def build_order_items_projection(source_df):
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

def build_support_tickets_projection(source_df):
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

def build_incidents_projection(source_df):
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

def build_marketing_spend_projection(source_df):
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

def raw_prefix_has_data(prefix: str) -> bool:
    settings = get_settings()
    client = build_s3_client()
    response = client.list_objects_v2(Bucket=settings.minio_bucket, Prefix=prefix)
    for item in response.get("Contents", []):
        key = item["Key"]
        if not key.startswith(f"{prefix}_spark_metadata/"):
            return True
    return False

def bootstrap_table_if_needed(spark: SparkSession, prefix: str, topic: str, starting_offsets: str, projection_builder, enrich_builder, label: str) -> None:
    settings = get_settings()
    if raw_prefix_has_data(prefix):
        logger.info("%s raw data already exists, skipping bootstrap", label)
        return

    logger.info("%s raw data missing, bootstrapping historical %s from Kafka", label, label)
    bootstrap_raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .option("endingOffsets", "latest")
        .load()
    )
    bootstrap_df = enrich_builder(projection_builder(bootstrap_raw))
    if bootstrap_df.limit(1).count() == 0:
        logger.info("%s bootstrap found no historical rows in Kafka", label)
        return

    (
        bootstrap_df.write.mode("append")
        .partitionBy("year", "month", "day", "hour")
        .parquet(f"s3a://{settings.minio_bucket}/{prefix.rstrip('/')}")
    )
    logger.info("%s bootstrap finished writing historical rows to MinIO", label)

def bootstrap_orders_if_needed(spark: SparkSession) -> None:
    settings = get_settings()
    bootstrap_table_if_needed(
        spark,
        "raw/orders/",
        f"{settings.debezium_topic_prefix}.public.orders",
        ORDERS_BOOTSTRAP_STARTING_OFFSETS,
        build_orders_projection,
        enrich_orders_for_landing,
        "orders",
    )

def bootstrap_customers_if_needed(spark: SparkSession) -> None:
    settings = get_settings()
    bootstrap_table_if_needed(
        spark,
        "raw/customers/",
        f"{settings.debezium_topic_prefix}.public.customers",
        CUSTOMERS_BOOTSTRAP_STARTING_OFFSETS,
        build_customers_projection,
        enrich_customers_for_landing,
        "customers",
    )

def bootstrap_products_if_needed(spark: SparkSession) -> None:
    settings = get_settings()
    bootstrap_table_if_needed(
        spark,
        "raw/products/",
        f"{settings.debezium_topic_prefix}.public.products",
        PRODUCTS_BOOTSTRAP_STARTING_OFFSETS,
        build_products_projection,
        enrich_products_for_landing,
        "products",
    )

def bootstrap_payments_if_needed(spark: SparkSession) -> None:
    settings = get_settings()
    bootstrap_table_if_needed(
        spark,
        "raw/payments/",
        f"{settings.debezium_topic_prefix}.public.payments",
        PAYMENTS_BOOTSTRAP_STARTING_OFFSETS,
        build_payments_projection,
        enrich_payments_for_landing,
        "payments",
    )

def bootstrap_order_items_if_needed(spark: SparkSession) -> None:
    settings = get_settings()
    bootstrap_table_if_needed(
        spark,
        "raw/order_items/",
        f"{settings.debezium_topic_prefix}.public.order_items",
        ORDER_ITEMS_BOOTSTRAP_STARTING_OFFSETS,
        build_order_items_projection,
        enrich_order_items_for_landing,
        "order_items",
    )

def bootstrap_support_tickets_if_needed(spark: SparkSession) -> None:
    settings = get_settings()
    bootstrap_table_if_needed(
        spark,
        "raw/support_tickets/",
        f"{settings.debezium_topic_prefix}.public.support_tickets",
        SUPPORT_TICKETS_BOOTSTRAP_STARTING_OFFSETS,
        build_support_tickets_projection,
        enrich_support_tickets_for_landing,
        "support_tickets",
    )

def bootstrap_incidents_if_needed(spark: SparkSession) -> None:
    settings = get_settings()
    bootstrap_table_if_needed(
        spark,
        "raw/incidents/",
        f"{settings.debezium_topic_prefix}.public.incidents",
        INCIDENTS_BOOTSTRAP_STARTING_OFFSETS,
        build_incidents_projection,
        enrich_incidents_for_landing,
        "incidents",
    )

def bootstrap_marketing_spend_if_needed(spark: SparkSession) -> None:
    settings = get_settings()
    bootstrap_table_if_needed(
        spark,
        "raw/marketing_spend/",
        f"{settings.debezium_topic_prefix}.public.marketing_spend",
        MARKETING_SPEND_BOOTSTRAP_STARTING_OFFSETS,
        build_marketing_spend_projection,
        enrich_marketing_spend_for_landing,
        "marketing_spend",
    )

def main() -> None:
    settings = get_settings()
    spark = build_spark_session()
    bootstrap_customers_if_needed(spark)
    bootstrap_products_if_needed(spark)
    bootstrap_orders_if_needed(spark)
    bootstrap_order_items_if_needed(spark)
    bootstrap_payments_if_needed(spark)
    bootstrap_support_tickets_if_needed(spark)
    bootstrap_incidents_if_needed(spark)
    bootstrap_marketing_spend_if_needed(spark)
    web_events_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_web_events_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    web_events = web_events_raw.select(
        F.col("timestamp").alias("kafka_timestamp"),
        F.get_json_object(F.col("value").cast("string"), "$.event_id").alias("event_id"),
        F.get_json_object(F.col("value").cast("string"), "$.session_id").alias("session_id"),
        F.get_json_object(F.col("value").cast("string"), "$.customer_id").alias("customer_id"),
        F.get_json_object(F.col("value").cast("string"), "$.event_type").alias("event_type"),
        F.get_json_object(F.col("value").cast("string"), "$.event_timestamp").alias("event_timestamp"),
    )
    web_events_for_landing = enrich_for_landing(web_events)
    customers_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", f"{settings.debezium_topic_prefix}.public.customers")
        .option("startingOffsets", CUSTOMERS_STREAM_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )
    customers_cdc = build_customers_projection(customers_raw)
    customers_for_landing = enrich_customers_for_landing(customers_cdc)
    products_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", f"{settings.debezium_topic_prefix}.public.products")
        .option("startingOffsets", PRODUCTS_STREAM_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )
    products_cdc = build_products_projection(products_raw)
    products_for_landing = enrich_products_for_landing(products_cdc)
    orders_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", f"{settings.debezium_topic_prefix}.public.orders")
        .option("startingOffsets", ORDERS_STREAM_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )
    orders_cdc = build_orders_projection(orders_raw)
    orders_for_landing = enrich_orders_for_landing(orders_cdc)
    order_items_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", f"{settings.debezium_topic_prefix}.public.order_items")
        .option("startingOffsets", ORDER_ITEMS_STREAM_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )
    order_items_cdc = build_order_items_projection(order_items_raw)
    order_items_for_landing = enrich_order_items_for_landing(order_items_cdc)
    payments_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", f"{settings.debezium_topic_prefix}.public.payments")
        .option("startingOffsets", PAYMENTS_STREAM_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )
    payments_cdc = build_payments_projection(payments_raw)
    payments_for_landing = enrich_payments_for_landing(payments_cdc)
    support_tickets_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", f"{settings.debezium_topic_prefix}.public.support_tickets")
        .option("startingOffsets", SUPPORT_TICKETS_STREAM_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )
    support_tickets_cdc = build_support_tickets_projection(support_tickets_raw)
    support_tickets_for_landing = enrich_support_tickets_for_landing(support_tickets_cdc)
    incidents_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", f"{settings.debezium_topic_prefix}.public.incidents")
        .option("startingOffsets", INCIDENTS_STREAM_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )
    incidents_cdc = build_incidents_projection(incidents_raw)
    incidents_for_landing = enrich_incidents_for_landing(incidents_cdc)
    marketing_spend_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", f"{settings.debezium_topic_prefix}.public.marketing_spend")
        .option("startingOffsets", MARKETING_SPEND_STREAM_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )
    marketing_spend_cdc = build_marketing_spend_projection(marketing_spend_raw)
    marketing_spend_for_landing = enrich_marketing_spend_for_landing(marketing_spend_cdc)
    cdc_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribePattern", f"{settings.debezium_topic_prefix}\\.public\\..*")
        .option("startingOffsets", CDC_CONSOLE_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )
    cdc_events = cdc_raw.select(
        F.col("topic"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.get_json_object(F.col("value").cast("string"), "$.op").alias("op"),
        F.get_json_object(F.col("value").cast("string"), "$.after").alias("after_json"),
    )
    cdc_console_events = cdc_events.filter(
        ~F.col("topic").isin(
            f"{settings.debezium_topic_prefix}.public.customers",
            f"{settings.debezium_topic_prefix}.public.products",
            f"{settings.debezium_topic_prefix}.public.orders",
            f"{settings.debezium_topic_prefix}.public.order_items",
            f"{settings.debezium_topic_prefix}.public.payments",
            f"{settings.debezium_topic_prefix}.public.support_tickets",
            f"{settings.debezium_topic_prefix}.public.incidents",
            f"{settings.debezium_topic_prefix}.public.marketing_spend",
        )
    )
    web_query = (
        web_events_for_landing.writeStream.format("parquet")
        .outputMode("append")
        .option("path", f"s3a://{settings.minio_bucket}/raw/web_events")
        .option("checkpointLocation", "/tmp/spark-checkpoints/web_events")
        .partitionBy("year", "month", "day", "hour")
        .start()
    )
    customers_query = (
        customers_for_landing.writeStream.format("parquet")
        .outputMode("append")
        .option("path", f"s3a://{settings.minio_bucket}/raw/customers")
        .option("checkpointLocation", "/tmp/spark-checkpoints/customers")
        .partitionBy("year", "month", "day", "hour")
        .start()
    )
    products_query = (
        products_for_landing.writeStream.format("parquet")
        .outputMode("append")
        .option("path", f"s3a://{settings.minio_bucket}/raw/products")
        .option("checkpointLocation", "/tmp/spark-checkpoints/products")
        .partitionBy("year", "month", "day", "hour")
        .start()
    )
    orders_query = (
        orders_for_landing.writeStream.format("parquet")
        .outputMode("append")
        .option("path", f"s3a://{settings.minio_bucket}/raw/orders")
        .option("checkpointLocation", "/tmp/spark-checkpoints/orders")
        .partitionBy("year", "month", "day", "hour")
        .start()
    )
    order_items_query = (
        order_items_for_landing.writeStream.format("parquet")
        .outputMode("append")
        .option("path", f"s3a://{settings.minio_bucket}/raw/order_items")
        .option("checkpointLocation", "/tmp/spark-checkpoints/order_items")
        .partitionBy("year", "month", "day", "hour")
        .start()
    )
    payments_query = (
        payments_for_landing.writeStream.format("parquet")
        .outputMode("append")
        .option("path", f"s3a://{settings.minio_bucket}/raw/payments")
        .option("checkpointLocation", "/tmp/spark-checkpoints/payments")
        .partitionBy("year", "month", "day", "hour")
        .start()
    )
    support_tickets_query = (
        support_tickets_for_landing.writeStream.format("parquet")
        .outputMode("append")
        .option("path", f"s3a://{settings.minio_bucket}/raw/support_tickets")
        .option("checkpointLocation", "/tmp/spark-checkpoints/support_tickets")
        .partitionBy("year", "month", "day", "hour")
        .start()
    )
    incidents_query = (
        incidents_for_landing.writeStream.format("parquet")
        .outputMode("append")
        .option("path", f"s3a://{settings.minio_bucket}/raw/incidents")
        .option("checkpointLocation", "/tmp/spark-checkpoints/incidents")
        .partitionBy("year", "month", "day", "hour")
        .start()
    )
    marketing_spend_query = (
        marketing_spend_for_landing.writeStream.format("parquet")
        .outputMode("append")
        .option("path", f"s3a://{settings.minio_bucket}/raw/marketing_spend")
        .option("checkpointLocation", "/tmp/spark-checkpoints/marketing_spend")
        .partitionBy("year", "month", "day", "hour")
        .start()
    )
    cdc_query = (
        cdc_console_events.writeStream.format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", 10)
        .start()
    )
    logger.info("spark consumer started: landing web_events plus all source tables to MinIO, printing remaining CDC topics")
    spark.streams.awaitAnyTermination()
    web_query.stop()
    customers_query.stop()
    products_query.stop()
    orders_query.stop()
    order_items_query.stop()
    payments_query.stop()
    support_tickets_query.stop()
    incidents_query.stop()
    marketing_spend_query.stop()
    cdc_query.stop()

if __name__ == "__main__":
    main()