"""Metrics store — reads processed data from HDFS/local and serves to API layer."""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from src.utils.config import get_hdfs_config
from src.utils.logging import setup_logging

logger = setup_logging("metrics-store")


class MetricsStore:
    """
    Abstraction layer between the API and the storage backend.

    In production, this reads from HDFS Parquet files via PySpark or a
    query engine. For local development, it can fall back to reading
    from local JSON/Parquet files or return sample data.
    """

    def __init__(self):
        self.hdfs_cfg = get_hdfs_config()
        self._spark = None

    def _get_spark(self):
        if self._spark is None:
            try:
                from pyspark.sql import SparkSession

                self._spark = (
                    SparkSession.builder
                    .appName("MetricsStore")
                    .config("spark.ui.enabled", "false")
                    .getOrCreate()
                )
            except Exception as e:
                logger.warning("spark_unavailable", error=str(e))
        return self._spark

    def check_dependencies(self) -> dict:
        """Check connectivity to backend services."""
        status = {}
        spark = self._get_spark()
        status["spark"] = "connected" if spark else "unavailable"
        # In production, check HDFS, Kafka connectivity here
        status["hdfs"] = "configured"
        status["kafka"] = "configured"
        return status

    def get_usage_metrics(
        self,
        region: Optional[str] = None,
        device_type: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        granularity: str = "daily",
    ) -> dict:
        """Fetch usage metrics from processed data."""
        spark = self._get_spark()
        if spark:
            return self._read_usage_from_spark(
                spark, region, device_type, start_date, end_date, granularity
            )
        return self._sample_usage_data(region, device_type)

    def get_crash_metrics(
        self,
        os: Optional[str] = None,
        app_version: Optional[str] = None,
        device_type: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> dict:
        spark = self._get_spark()
        if spark:
            return self._read_crashes_from_spark(
                spark, os, app_version, device_type, start_date, end_date
            )
        return self._sample_crash_data()

    def get_adoption_metrics(
        self,
        feature: Optional[str] = None,
        top_n: int = 10,
    ) -> dict:
        spark = self._get_spark()
        if spark:
            return self._read_adoption_from_spark(spark, feature, top_n)
        return self._sample_adoption_data(top_n)

    def get_performance_metrics(
        self,
        device_type: Optional[str] = None,
        os: Optional[str] = None,
        metric_name: Optional[str] = None,
    ) -> dict:
        spark = self._get_spark()
        if spark:
            return self._read_performance_from_spark(spark, device_type, os, metric_name)
        return self._sample_performance_data()

    def get_realtime_snapshot(self) -> dict:
        """Fetch latest streaming metrics."""
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "active_users_1m": 0,
            "events_per_second": 0,
            "error_rate": 0.0,
            "note": "Connect to streaming output for live data",
        }

    # ── Spark readers (production) ───────────────────────────────────

    def _read_usage_from_spark(self, spark, region, device_type, start, end, granularity):
        try:
            base = f"{self.hdfs_cfg['namenode']}{self.hdfs_cfg['paths']['processed']}/batch/daily_trends"
            df = spark.read.parquet(base)
            if start:
                df = df.filter(df["date"] >= start)
            if end:
                df = df.filter(df["date"] <= end)
            rows = df.orderBy("date").limit(100).collect()
            return {
                "granularity": granularity,
                "data": [row.asDict() for row in rows],
            }
        except Exception as e:
            logger.error("read_usage_failed", error=str(e))
            return self._sample_usage_data(region, device_type)

    def _read_crashes_from_spark(self, spark, os_filter, app_ver, dev_type, start, end):
        try:
            base = f"{self.hdfs_cfg['namenode']}{self.hdfs_cfg['paths']['processed']}/batch/crash_analysis"
            df = spark.read.parquet(base)
            if os_filter:
                df = df.filter(df["os"] == os_filter)
            if app_ver:
                df = df.filter(df["app_version"] == app_ver)
            if dev_type:
                df = df.filter(df["device_type"] == dev_type)
            rows = df.orderBy(df["crash_count"].desc()).limit(50).collect()
            return {"data": [row.asDict() for row in rows]}
        except Exception as e:
            logger.error("read_crashes_failed", error=str(e))
            return self._sample_crash_data()

    def _read_adoption_from_spark(self, spark, feature, top_n):
        try:
            base = f"{self.hdfs_cfg['namenode']}{self.hdfs_cfg['paths']['processed']}/batch/feature_ranking"
            df = spark.read.parquet(base)
            if feature:
                df = df.filter(df["feature_name"] == feature)
            rows = df.orderBy("rank").limit(top_n).collect()
            return {"data": [row.asDict() for row in rows]}
        except Exception as e:
            logger.error("read_adoption_failed", error=str(e))
            return self._sample_adoption_data(top_n)

    def _read_performance_from_spark(self, spark, device_type, os_filter, metric_name):
        try:
            base = f"{self.hdfs_cfg['namenode']}{self.hdfs_cfg['paths']['processed']}/batch/device_performance"
            df = spark.read.parquet(base)
            if device_type:
                df = df.filter(df["device_type"] == device_type)
            if os_filter:
                df = df.filter(df["os"] == os_filter)
            rows = df.limit(50).collect()
            return {"data": [row.asDict() for row in rows]}
        except Exception as e:
            logger.error("read_performance_failed", error=str(e))
            return self._sample_performance_data()

    # ── Sample data (development fallback) ───────────────────────────

    def _sample_usage_data(self, region=None, device_type=None):
        today = datetime.utcnow().date()
        return {
            "granularity": "daily",
            "filters": {"region": region, "device_type": device_type},
            "data": [
                {
                    "date": str(today - timedelta(days=i)),
                    "daily_active_users": 1200 + i * 50,
                    "total_events": 45000 + i * 1000,
                    "total_sessions": 3000 + i * 100,
                }
                for i in range(7)
            ],
        }

    def _sample_crash_data(self):
        return {
            "data": [
                {"device_type": "mobile", "os": "Android", "app_version": "2.4.1", "crash_count": 142, "crash_rate": 0.032},
                {"device_type": "mobile", "os": "iOS", "app_version": "2.4.1", "crash_count": 67, "crash_rate": 0.015},
                {"device_type": "desktop", "os": "Windows", "app_version": "2.4.0", "crash_count": 23, "crash_rate": 0.008},
            ]
        }

    def _sample_adoption_data(self, top_n):
        features = [
            {"feature_name": "search", "unique_users": 8500, "total_usage": 34000, "adoption_rate": 0.85, "rank": 1},
            {"feature_name": "checkout", "unique_users": 6200, "total_usage": 12400, "adoption_rate": 0.62, "rank": 2},
            {"feature_name": "dashboard", "unique_users": 5800, "total_usage": 23200, "adoption_rate": 0.58, "rank": 3},
            {"feature_name": "notifications", "unique_users": 4500, "total_usage": 9000, "adoption_rate": 0.45, "rank": 4},
            {"feature_name": "analytics", "unique_users": 3200, "total_usage": 6400, "adoption_rate": 0.32, "rank": 5},
        ]
        return {"data": features[:top_n]}

    def _sample_performance_data(self):
        return {
            "data": [
                {"device_type": "mobile", "os": "iOS", "avg_metric_value": 320.5, "p50": 280.0, "p95": 890.0, "p99": 1200.0},
                {"device_type": "mobile", "os": "Android", "avg_metric_value": 445.2, "p50": 380.0, "p95": 1100.0, "p99": 1800.0},
                {"device_type": "desktop", "os": "Windows", "avg_metric_value": 210.8, "p50": 180.0, "p95": 520.0, "p99": 780.0},
            ]
        }
