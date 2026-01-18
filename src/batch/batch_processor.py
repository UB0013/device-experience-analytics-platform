"""Spark batch processing — historical analytics on telemetry data (Batch Layer)."""

from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.config import get_spark_config, get_hdfs_config


def create_spark_session() -> SparkSession:
    spark_cfg = get_spark_config()
    return (
        SparkSession.builder
        .appName(f"{spark_cfg['app_name']}-Batch")
        .master(spark_cfg["master"])
        .config("spark.sql.shuffle.partitions", 12)
        .config("spark.executor.memory", spark_cfg["batch"]["executor_memory"])
        .config("spark.executor.cores", spark_cfg["batch"]["executor_cores"])
        .enableHiveSupport()
        .getOrCreate()
    )


def load_raw_events(spark: SparkSession, event_path: str, date: str | None = None):
    """Load raw events from HDFS. Optionally filter by date partition."""
    df = spark.read.json(event_path)
    if date:
        df = df.filter(F.to_date("timestamp") == date)
    return df


# ── Daily Trend Analysis ─────────────────────────────────────────────

def daily_usage_trends(events_df):
    """Compute daily active users, sessions, and event counts."""
    return (
        events_df
        .withColumn("date", F.to_date("timestamp"))
        .groupBy("date")
        .agg(
            F.countDistinct("device_id").alias("daily_active_users"),
            F.count("*").alias("total_events"),
            F.countDistinct(
                F.when(F.col("event_type") == "app_open", F.col("payload.session_id"))
            ).alias("total_sessions"),
        )
        .orderBy("date")
    )


def daily_trends_by_region(events_df):
    """Daily active users broken down by region."""
    return (
        events_df
        .withColumn("date", F.to_date("timestamp"))
        .groupBy("date", "region")
        .agg(
            F.countDistinct("device_id").alias("daily_active_users"),
            F.count("*").alias("total_events"),
        )
        .orderBy("date", "region")
    )


# ── Feature Adoption ─────────────────────────────────────────────────

def feature_adoption_rates(events_df):
    """Feature adoption: unique users per feature over time."""
    feature_events = (
        events_df
        .filter(F.col("event_type") == "feature_used")
        .withColumn("feature_name", F.col("payload.feature_name"))
        .withColumn("date", F.to_date("timestamp"))
    )

    total_users = events_df.select("device_id").distinct().count()

    return (
        feature_events
        .groupBy("date", "feature_name")
        .agg(
            F.countDistinct("device_id").alias("unique_users"),
            F.count("*").alias("total_usage"),
            F.avg("payload.duration_ms").alias("avg_duration_ms"),
        )
        .withColumn("adoption_rate", F.col("unique_users") / F.lit(total_users))
        .orderBy("date", F.desc("unique_users"))
    )


def feature_ranking(events_df):
    """Rank features by total usage and unique users."""
    return (
        events_df
        .filter(F.col("event_type") == "feature_used")
        .withColumn("feature_name", F.col("payload.feature_name"))
        .groupBy("feature_name")
        .agg(
            F.countDistinct("device_id").alias("unique_users"),
            F.count("*").alias("total_usage"),
            F.avg("payload.duration_ms").alias("avg_duration_ms"),
            F.avg("payload.interaction_count").alias("avg_interactions"),
        )
        .withColumn(
            "rank",
            F.row_number().over(Window.orderBy(F.desc("unique_users"))),
        )
        .orderBy("rank")
    )


# ── Retention Analysis ───────────────────────────────────────────────

def retention_cohorts(events_df, cohort_days: int = 7):
    """Simple cohort retention: % of users returning after first seen."""
    user_first_seen = (
        events_df
        .groupBy("device_id")
        .agg(F.min(F.to_date("timestamp")).alias("first_seen"))
    )

    events_with_cohort = (
        events_df
        .withColumn("date", F.to_date("timestamp"))
        .join(user_first_seen, "device_id")
        .withColumn("days_since_first", F.datediff("date", "first_seen"))
        .withColumn(
            "cohort_bucket",
            (F.col("days_since_first") / cohort_days).cast("int") * cohort_days,
        )
    )

    cohort_size = user_first_seen.groupBy("first_seen").count().alias("cohort_size")

    return (
        events_with_cohort
        .groupBy("first_seen", "cohort_bucket")
        .agg(F.countDistinct("device_id").alias("retained_users"))
        .orderBy("first_seen", "cohort_bucket")
    )


# ── Device Performance ───────────────────────────────────────────────

def device_performance_report(events_df):
    """Performance breakdown by device type and OS."""
    perf_events = events_df.filter(F.col("event_type") == "performance_metric")

    return (
        perf_events
        .groupBy("device_type", "os", "app_version")
        .agg(
            F.avg("payload.value").alias("avg_metric_value"),
            F.percentile_approx("payload.value", 0.5).alias("p50"),
            F.percentile_approx("payload.value", 0.95).alias("p95"),
            F.percentile_approx("payload.value", 0.99).alias("p99"),
            F.count("*").alias("sample_count"),
        )
        .orderBy("device_type", "os")
    )


def crash_analysis(events_df):
    """Crash rates by device type, OS, and app version."""
    total_by_segment = (
        events_df
        .groupBy("device_type", "os", "app_version")
        .agg(F.countDistinct("device_id").alias("total_devices"))
    )

    crashes = (
        events_df
        .filter(F.col("event_type") == "crash_event")
        .groupBy("device_type", "os", "app_version")
        .agg(
            F.count("*").alias("crash_count"),
            F.countDistinct("device_id").alias("affected_devices"),
            F.sum(F.when(F.col("payload.is_fatal") == "true", 1).otherwise(0)).alias(
                "fatal_crashes"
            ),
        )
    )

    return (
        crashes
        .join(total_by_segment, ["device_type", "os", "app_version"], "left")
        .withColumn(
            "crash_rate",
            F.col("affected_devices") / F.col("total_devices"),
        )
        .orderBy(F.desc("crash_count"))
    )


# ── Main Entry Point ─────────────────────────────────────────────────

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    hdfs_cfg = get_hdfs_config()
    namenode = hdfs_cfg["namenode"]
    raw_path = f"{namenode}{hdfs_cfg['paths']['raw']}"
    output_path = f"{namenode}{hdfs_cfg['paths']['processed']}/batch"

    print("Loading raw events from HDFS...")
    events = load_raw_events(spark, f"{raw_path}/app_events")
    crashes_raw = load_raw_events(spark, f"{raw_path}/crashes")
    perf_raw = load_raw_events(spark, f"{raw_path}/performance")

    # Combine all events for cross-cutting analysis
    all_events = events.unionByName(crashes_raw, allowMissingColumns=True)
    all_events = all_events.unionByName(perf_raw, allowMissingColumns=True)
    all_events.cache()

    print("Running batch analytics...")

    # 1. Daily trends
    trends = daily_usage_trends(all_events)
    trends.write.mode("overwrite").parquet(f"{output_path}/daily_trends")
    print("  -> Daily trends complete")

    # 2. Regional trends
    regional = daily_trends_by_region(all_events)
    regional.write.mode("overwrite").parquet(f"{output_path}/regional_trends")
    print("  -> Regional trends complete")

    # 3. Feature adoption
    adoption = feature_adoption_rates(all_events)
    adoption.write.mode("overwrite").parquet(f"{output_path}/feature_adoption")
    print("  -> Feature adoption complete")

    # 4. Feature ranking
    ranking = feature_ranking(all_events)
    ranking.write.mode("overwrite").parquet(f"{output_path}/feature_ranking")
    print("  -> Feature ranking complete")

    # 5. Retention cohorts
    retention = retention_cohorts(all_events)
    retention.write.mode("overwrite").parquet(f"{output_path}/retention_cohorts")
    print("  -> Retention cohorts complete")

    # 6. Device performance
    perf_report = device_performance_report(all_events)
    perf_report.write.mode("overwrite").parquet(f"{output_path}/device_performance")
    print("  -> Device performance complete")

    # 7. Crash analysis
    crash_report = crash_analysis(all_events)
    crash_report.write.mode("overwrite").parquet(f"{output_path}/crash_analysis")
    print("  -> Crash analysis complete")

    all_events.unpersist()
    print("Batch processing complete!")
    spark.stop()


if __name__ == "__main__":
    main()
