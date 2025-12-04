"""Kafka producer — pushes generated telemetry events into Kafka topics."""

import time
import json
import signal
import sys
from datetime import datetime

import click
from confluent_kafka import Producer, KafkaError
from prometheus_client import start_http_server, Counter, Histogram, Gauge

from src.ingestion.generator import TelemetryGenerator
from src.utils.config import get_kafka_config
from src.utils.schemas import EventType
from src.utils.logging import setup_logging

logger = setup_logging("telemetry-producer")

# --- Prometheus Metrics ---
EVENTS_PRODUCED = Counter(
    "telemetry_events_produced_total",
    "Total events produced to Kafka",
    ["topic", "event_type"],
)
PRODUCE_LATENCY = Histogram(
    "telemetry_produce_latency_seconds",
    "Time to produce a single event",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25],
)
PRODUCE_ERRORS = Counter(
    "telemetry_produce_errors_total",
    "Total produce errors",
)
EVENTS_PER_SECOND = Gauge(
    "telemetry_events_per_second",
    "Current event production rate",
)

# Topic routing
TOPIC_MAP = {
    EventType.APP_OPEN: "app_events",
    EventType.FEATURE_USED: "app_events",
    EventType.CRASH_EVENT: "crashes",
    EventType.PERFORMANCE_METRIC: "performance",
}

_running = True


def _signal_handler(sig, frame):
    global _running
    logger.info("shutdown_signal_received", signal=sig)
    _running = False


def delivery_callback(err, msg):
    if err:
        PRODUCE_ERRORS.inc()
        logger.error("delivery_failed", error=str(err), topic=msg.topic())
    else:
        EVENTS_PRODUCED.labels(
            topic=msg.topic(), event_type="unknown"
        ).inc()


def create_producer(external: bool = True) -> Producer:
    kafka_cfg = get_kafka_config(external=external)
    conf = {
        "bootstrap.servers": ",".join(kafka_cfg["bootstrap_servers"]),
        "acks": str(kafka_cfg.get("acks", "all")),
        "retries": kafka_cfg.get("retries", 3),
        "batch.size": kafka_cfg.get("batch_size", 16384),
        "linger.ms": kafka_cfg.get("linger_ms", 10),
        "compression.type": "lz4",
        "queue.buffering.max.messages": 100000,
    }
    return Producer(conf)


@click.command()
@click.option("--rate", default=1000, help="Events per second to generate")
@click.option(
    "--topics",
    default="app_events,crashes,performance",
    help="Comma-separated list of Kafka topics",
)
@click.option("--external/--internal", default=True, help="Use external broker addresses")
@click.option("--metrics-port", default=8001, help="Prometheus metrics port")
@click.option("--duration", default=0, help="Run duration in seconds (0 = infinite)")
def main(rate: int, topics: str, external: bool, metrics_port: int, duration: int):
    """Start the telemetry event producer."""
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    start_http_server(metrics_port)
    logger.info("metrics_server_started", port=metrics_port)

    producer = create_producer(external=external)
    generator = TelemetryGenerator()

    logger.info(
        "producer_started",
        rate=rate,
        topics=topics,
        external=external,
    )

    interval = 1.0 / rate if rate > 0 else 0.001
    events_this_second = 0
    second_start = time.monotonic()
    start_time = time.monotonic()
    total_events = 0

    while _running:
        if duration > 0 and (time.monotonic() - start_time) >= duration:
            logger.info("duration_reached", duration=duration, total_events=total_events)
            break

        event = generator.generate_event()
        topic = TOPIC_MAP.get(event.event_type, "app_events")

        with PRODUCE_LATENCY.time():
            producer.produce(
                topic=topic,
                key=event.device_id.encode("utf-8"),
                value=event.to_kafka_value(),
                callback=delivery_callback,
            )

        EVENTS_PRODUCED.labels(topic=topic, event_type=event.event_type.value).inc()
        total_events += 1
        events_this_second += 1

        # Rate limiting
        elapsed = time.monotonic() - second_start
        if elapsed >= 1.0:
            EVENTS_PER_SECOND.set(events_this_second)
            if total_events % (rate * 10) == 0:
                logger.info(
                    "producer_stats",
                    events_per_sec=events_this_second,
                    total_events=total_events,
                )
            events_this_second = 0
            second_start = time.monotonic()
        elif events_this_second >= rate:
            sleep_time = 1.0 - elapsed
            if sleep_time > 0:
                producer.poll(sleep_time)
                events_this_second = 0
                second_start = time.monotonic()
                continue

        producer.poll(0)

    producer.flush(timeout=10)
    logger.info("producer_shutdown", total_events=total_events)


if __name__ == "__main__":
    main()
