"""Spark Structured Streaming pipeline — real-time telemetry processing (Speed Layer)."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    MapType,
)

from src.utils.config import get_spark_config, get_kafka_config, get_hdfs_config


# Schema matching TelemetryEvent
TELEMETRY_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("device_id", StringType(), False),
    StructField("device_type", StringType(), False),
    StructField("os", StringType(), False),
    StructField("app_version", StringType(), False),
    StructField("region", StringType(), False),
    StructField("payload", MapType(StringType(), StringType()), True),
])


def create_spark_session() -> SparkSession:
    spark_cfg = get_spark_config()
    return (
        SparkSession.builder
        .appName(f"{spark_cfg['app_name']}-Streaming")
        .master(spark_cfg["master"])
        .config("spark.sql.shuffle.partitions", 12)
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        )
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession, topic: str):
    """Read a Kafka topic as a structured stream."""
    kafka_cfg = get_kafka_config(external=False)
    brokers = ",".join(kafka_cfg["bootstrap_servers"])

    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(F.from_json(F.col("json_str"), TELEMETRY_SCHEMA).alias("data"))
        .select("data.*")
        .withWatermark("timestamp", "10 seconds")
    )


def compute_active_users_per_minute(events_df):
    """Active unique users per minute, grouped by region and device type."""
    return (
        events_df
        .groupBy(
            F.window("timestamp", "1 minute"),
            "region",
            "device_type",
        )
        .agg(
            F.countDistinct("device_id").alias("active_users"),
            F.count("*").alias("event_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "region",
            "device_type",
            "active_users",
            "event_count",
        )
    )


def compute_error_spikes(crash_df):
    """Detect error spikes — crash counts per 30-second window."""
    return (
        crash_df
        .filter(F.col("event_type") == "crash_event")
        .groupBy(
            F.window("timestamp", "30 seconds"),
            "os",
            "app_version",
        )
        .agg(
            F.count("*").alias("crash_count"),
            F.countDistinct("device_id").alias("affected_devices"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "os",
            "app_version",
            "crash_count",
            "affected_devices",
        )
    )


def compute_feature_usage(events_df):
    """Real-time feature usage counts per minute."""
    return (
        events_df
        .filter(F.col("event_type") == "feature_used")
        .withColumn("feature_name", F.col("payload").getItem("feature_name"))
        .groupBy(
            F.window("timestamp", "1 minute"),
            "feature_name",
        )
        .agg(
            F.count("*").alias("usage_count"),
            F.countDistinct("device_id").alias("unique_users"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "feature_name",
            "usage_count",
            "unique_users",
        )
    )


def write_to_hdfs(df, query_name: str, output_path: str, checkpoint_path: str):
    """Write streaming results to HDFS as Parquet, partitioned by date."""
    return (
        df.writeStream
        .queryName(query_name)
        .outputMode("append")
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("window_start")
        .trigger(processingTime="10 seconds")
        .start()
    )


def write_to_console(df, query_name: str):
    """Write to console for debugging."""
    return (
        df.writeStream
        .queryName(query_name)
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime="10 seconds")
        .start()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    hdfs_cfg = get_hdfs_config()
    spark_cfg = get_spark_config()
    base_output = f"{hdfs_cfg['namenode']}{hdfs_cfg['paths']['processed']}"
    base_checkpoint = spark_cfg["streaming"]["checkpoint_dir"]

    # Read from all topics
    app_events = read_kafka_stream(spark, "app_events")
    crashes = read_kafka_stream(spark, "crashes")
    performance = read_kafka_stream(spark, "performance")

    # Compute real-time metrics
    active_users = compute_active_users_per_minute(app_events)
    error_spikes = compute_error_spikes(crashes)
    feature_usage = compute_feature_usage(app_events)

    # Write outputs
    queries = [
        write_to_hdfs(
            active_users,
            "active_users_per_minute",
            f"{base_output}/metrics/active_users",
            f"{base_checkpoint}/active_users",
        ),
        write_to_hdfs(
            error_spikes,
            "error_spikes",
            f"{base_output}/metrics/error_spikes",
            f"{base_checkpoint}/error_spikes",
        ),
        write_to_hdfs(
            feature_usage,
            "feature_usage",
            f"{base_output}/metrics/feature_usage",
            f"{base_checkpoint}/feature_usage",
        ),
    ]

    print("Streaming queries started. Waiting for termination...")
    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
