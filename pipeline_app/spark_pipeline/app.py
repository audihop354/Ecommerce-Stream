import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from pipeline_app.config import Settings, get_settings
from pipeline_app.spark_pipeline.definitions import (
    BOOTSTRAP_STARTING_OFFSETS,
    CDC_CONSOLE_STARTING_OFFSETS,
    STREAM_STARTING_OFFSETS,
    RawDatasetSpec,
)
from pipeline_app.spark_pipeline.specs import RAW_DATASET_SPECS
from pipeline_app.spark_pipeline.transforms import (
    build_web_events_projection,
    enrich_web_events_for_landing,
)
from pipeline_app.storage import build_s3_client


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


def topic_name(settings: Settings, topic_suffix: str) -> str:
    return f"{settings.debezium_topic_prefix}.public.{topic_suffix}"


def prefix_has_data(prefix: str) -> bool:
    settings = get_settings()
    client = build_s3_client()
    response = client.list_objects_v2(Bucket=settings.minio_bucket, Prefix=prefix)
    for item in response.get("Contents", []):
        key = item["Key"]
        if not key.startswith(f"{prefix}_spark_metadata/"):
            return True
    return False


def build_kafka_batch_reader(spark: SparkSession, topic: str, starting_offsets: str) -> DataFrame:
    settings = get_settings()
    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .option("endingOffsets", "latest")
        .load()
    )


def build_kafka_stream_reader(spark: SparkSession, topic: str, starting_offsets: str) -> DataFrame:
    settings = get_settings()
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )


def bootstrap_raw_table_if_needed(spark: SparkSession, spec: RawDatasetSpec, settings: Settings) -> None:
    if prefix_has_data(spec.raw_prefix):
        logger.info("%s raw data already exists, skipping bootstrap", spec.label)
        return

    logger.info("%s raw data missing, bootstrapping historical rows from Kafka", spec.label)
    bootstrap_raw = build_kafka_batch_reader(
        spark,
        topic_name(settings, spec.topic_suffix),
        BOOTSTRAP_STARTING_OFFSETS,
    )
    bootstrap_df = spec.enrich_builder(spec.projection_builder(bootstrap_raw))
    if bootstrap_df.limit(1).count() == 0:
        logger.info("%s bootstrap found no historical rows in Kafka", spec.label)
        return

    (
        bootstrap_df.write.mode("append")
        .partitionBy("year", "month", "day", "hour")
        .parquet(f"s3a://{settings.minio_bucket}/{spec.raw_prefix.rstrip('/')}")
    )
    logger.info("%s bootstrap finished writing historical rows to MinIO", spec.label)


def build_raw_stream(spark: SparkSession, spec: RawDatasetSpec, settings: Settings) -> DataFrame:
    raw_stream = build_kafka_stream_reader(
        spark,
        topic_name(settings, spec.topic_suffix),
        STREAM_STARTING_OFFSETS,
    )
    return spec.enrich_builder(spec.projection_builder(raw_stream))


def start_partitioned_parquet_query(
    df: DataFrame,
    settings: Settings,
    prefix: str,
    checkpoint_name: str,
) -> StreamingQuery:
    return (
        df.writeStream.format("parquet")
        .outputMode("append")
        .option("path", f"s3a://{settings.minio_bucket}/{prefix.rstrip('/')}")
        .option("checkpointLocation", f"/tmp/spark-checkpoints/{checkpoint_name}")
        .partitionBy("year", "month", "day", "hour")
        .start()
    )


def build_web_events_stream(spark: SparkSession, settings: Settings) -> DataFrame:
    web_events_raw = build_kafka_stream_reader(
        spark,
        settings.kafka_web_events_topic,
        STREAM_STARTING_OFFSETS,
    )
    return enrich_web_events_for_landing(build_web_events_projection(web_events_raw))


def build_cdc_console_query(spark: SparkSession, settings: Settings) -> StreamingQuery:
    excluded_topics = [topic_name(settings, spec.topic_suffix) for spec in RAW_DATASET_SPECS]
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
    cdc_console_events = cdc_events.filter(~F.col("topic").isin(*excluded_topics))
    return (
        cdc_console_events.writeStream.format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", 10)
        .start()
    )


def run_spark_consumer() -> None:
    settings = get_settings()
    spark = build_spark_session()

    for spec in RAW_DATASET_SPECS:
        bootstrap_raw_table_if_needed(spark, spec, settings)

    queries: list[StreamingQuery] = []
    queries.append(
        start_partitioned_parquet_query(
            build_web_events_stream(spark, settings),
            settings,
            "raw/web_events/",
            "web_events",
        )
    )

    for spec in RAW_DATASET_SPECS:
        queries.append(
            start_partitioned_parquet_query(
                build_raw_stream(spark, spec, settings),
                settings,
                spec.raw_prefix,
                spec.checkpoint_name,
            )
        )

    queries.append(build_cdc_console_query(spark, settings))
    logger.info("spark consumer started: landing raw layer to MinIO and printing remaining CDC topics")

    try:
        spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            if query.isActive:
                query.stop()


if __name__ == "__main__":
    run_spark_consumer()