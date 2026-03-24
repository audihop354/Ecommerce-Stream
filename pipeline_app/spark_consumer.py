import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pipeline_app.config import get_settings

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

def main() -> None:
    settings = get_settings()
    spark = build_spark_session()
    web_events_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_web_events_topic)
        .option("startingOffsets", "latest")
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
    cdc_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribePattern", f"{settings.debezium_topic_prefix}\\.public\\..*")
        .option("startingOffsets", "latest")
        .load()
    )
    cdc_events = cdc_raw.select(
        F.col("topic"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.get_json_object(F.col("value").cast("string"), "$.op").alias("op"),
        F.get_json_object(F.col("value").cast("string"), "$.after").alias("after_json"),
    )
    web_query = (
        web_events_for_landing.writeStream.format("parquet")
        .outputMode("append")
        .option("path", f"s3a://{settings.minio_bucket}/raw/web_events")
        .option("checkpointLocation", "/tmp/spark-checkpoints/web_events")
        .partitionBy("year", "month", "day", "hour")
        .start()
    )
    cdc_query = (
        cdc_events.writeStream.format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", 10)
        .start()
    )
    logger.info("spark consumer started: landing web_events to MinIO and printing CDC topics")
    spark.streams.awaitAnyTermination()
    web_query.stop()
    cdc_query.stop()

if __name__ == "__main__":
    main()