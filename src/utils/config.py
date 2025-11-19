"""Centralized configuration loader for the telemetry platform."""

import os
from pathlib import Path
from functools import lru_cache

import yaml


CONFIG_DIR = Path(__file__).resolve().parent.parent.parent / "config"


@lru_cache(maxsize=1)
def load_config(config_path: str | None = None) -> dict:
    """Load platform configuration from YAML file."""
    path = Path(config_path) if config_path else CONFIG_DIR / "platform.yml"
    with open(path) as f:
        config = yaml.safe_load(f)

    # Allow environment variable overrides
    overrides = {
        "kafka.brokers_external": os.getenv("KAFKA_BROKERS"),
        "api.port": os.getenv("API_PORT"),
        "hdfs.namenode": os.getenv("HDFS_NAMENODE"),
    }
    for dotted_key, value in overrides.items():
        if value is not None:
            keys = dotted_key.split(".")
            d = config
            for k in keys[:-1]:
                d = d[k]
            if "," in value:
                d[keys[-1]] = [v.strip() for v in value.split(",")]
            elif value.isdigit():
                d[keys[-1]] = int(value)
            else:
                d[keys[-1]] = value

    return config


def get_kafka_config(external: bool = False) -> dict:
    cfg = load_config()
    brokers_key = "brokers_external" if external else "brokers"
    return {
        "bootstrap_servers": cfg["kafka"][brokers_key],
        "topics": cfg["kafka"]["topics"],
        **cfg["kafka"].get("producer", {}),
    }


def get_spark_config() -> dict:
    return load_config()["spark"]


def get_hdfs_config() -> dict:
    return load_config()["hdfs"]


def get_generator_config() -> dict:
    return load_config()["generator"]
