"""Load and scalability testing — measures throughput, latency, and resource usage."""

import time
import json
import statistics
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

import click
from confluent_kafka import Producer

from src.ingestion.generator import TelemetryGenerator
from src.utils.schemas import EventType

KAFKA_BROKERS = "localhost:9092"
TOPIC_MAP = {
    EventType.APP_OPEN: "app_events",
    EventType.FEATURE_USED: "app_events",
    EventType.CRASH_EVENT: "crashes",
    EventType.PERFORMANCE_METRIC: "performance",
}


@dataclass
class LoadTestResult:
    target_rate: int
    actual_rate: float
    duration_seconds: float
    total_events: int
    errors: int
    latencies_ms: list[float] = field(default_factory=list)

    @property
    def p50_latency(self) -> float:
        if not self.latencies_ms:
            return 0
        return statistics.median(self.latencies_ms)

    @property
    def p95_latency(self) -> float:
        if not self.latencies_ms:
            return 0
        sorted_lats = sorted(self.latencies_ms)
        idx = int(len(sorted_lats) * 0.95)
        return sorted_lats[idx]

    @property
    def p99_latency(self) -> float:
        if not self.latencies_ms:
            return 0
        sorted_lats = sorted(self.latencies_ms)
        idx = int(len(sorted_lats) * 0.99)
        return sorted_lats[idx]

    @property
    def error_rate(self) -> float:
        return self.errors / max(self.total_events, 1)


def delivery_tracker(latencies: list, errors: list):
    """Create a delivery callback that tracks latency and errors."""
    def callback(err, msg):
        if err:
            errors.append(str(err))
    return callback


def run_load_level(target_rate: int, duration: int = 30) -> LoadTestResult:
    """Run a single load level test."""
    producer = Producer({
        "bootstrap.servers": KAFKA_BROKERS,
        "acks": "all",
        "compression.type": "lz4",
        "batch.size": 32768,
        "linger.ms": 5,
        "queue.buffering.max.messages": 500000,
    })

    generator = TelemetryGenerator()
    latencies = []
    errors = []
    callback = delivery_tracker(latencies, errors)

    total_events = 0
    interval = 1.0 / target_rate if target_rate > 0 else 0.001
    start_time = time.monotonic()
    second_start = time.monotonic()
    events_this_second = 0

    while (time.monotonic() - start_time) < duration:
        event = generator.generate_event()
        topic = TOPIC_MAP.get(event.event_type, "app_events")

        t0 = time.monotonic()
        producer.produce(
            topic=topic,
            key=event.device_id.encode(),
            value=event.to_kafka_value(),
            callback=callback,
        )
        t1 = time.monotonic()
        latencies.append((t1 - t0) * 1000)

        total_events += 1
        events_this_second += 1

        # Rate limiting
        elapsed = time.monotonic() - second_start
        if elapsed >= 1.0:
            events_this_second = 0
            second_start = time.monotonic()
        elif events_this_second >= target_rate:
            sleep_time = 1.0 - elapsed
            if sleep_time > 0:
                producer.poll(sleep_time)
            events_this_second = 0
            second_start = time.monotonic()
            continue

        producer.poll(0)

    producer.flush(timeout=30)
    actual_duration = time.monotonic() - start_time

    return LoadTestResult(
        target_rate=target_rate,
        actual_rate=total_events / actual_duration,
        duration_seconds=actual_duration,
        total_events=total_events,
        errors=len(errors),
        latencies_ms=latencies,
    )


def print_result(result: LoadTestResult):
    print(f"\n{'=' * 60}")
    print(f"  Load Level: {result.target_rate:,} events/sec (target)")
    print(f"{'=' * 60}")
    print(f"  Actual Rate:     {result.actual_rate:,.1f} events/sec")
    print(f"  Duration:        {result.duration_seconds:.1f}s")
    print(f"  Total Events:    {result.total_events:,}")
    print(f"  Errors:          {result.errors} ({result.error_rate:.4%})")
    print(f"  Latency p50:     {result.p50_latency:.3f} ms")
    print(f"  Latency p95:     {result.p95_latency:.3f} ms")
    print(f"  Latency p99:     {result.p99_latency:.3f} ms")
    print(f"{'=' * 60}")


@click.command()
@click.option("--levels", default="1000,5000,10000", help="Comma-separated load levels")
@click.option("--duration", default=30, help="Duration per level in seconds")
@click.option("--output", default=None, help="Output results to JSON file")
def main(levels: str, duration: int, output: str | None):
    """Run progressive load testing across multiple throughput levels."""
    load_levels = [int(l.strip()) for l in levels.split(",")]

    print("\n" + "=" * 60)
    print("  TELEMETRY PLATFORM — LOAD TEST")
    print(f"  Levels: {load_levels}")
    print(f"  Duration per level: {duration}s")
    print("=" * 60)

    results = []
    for level in load_levels:
        print(f"\nStarting load test at {level:,} events/sec for {duration}s...")
        result = run_load_level(level, duration)
        print_result(result)
        results.append(result)

        # Cool-down between levels
        if level != load_levels[-1]:
            print("Cooling down for 5 seconds...")
            time.sleep(5)

    # Summary
    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    print(f"{'Level':>10} {'Actual':>12} {'Events':>10} {'Errors':>8} {'p95(ms)':>10} {'p99(ms)':>10}")
    print("-" * 60)
    for r in results:
        print(
            f"{r.target_rate:>10,} {r.actual_rate:>12,.1f} {r.total_events:>10,} "
            f"{r.errors:>8} {r.p95_latency:>10.3f} {r.p99_latency:>10.3f}"
        )

    if output:
        with open(output, "w") as f:
            json.dump(
                [
                    {
                        "target_rate": r.target_rate,
                        "actual_rate": r.actual_rate,
                        "total_events": r.total_events,
                        "errors": r.errors,
                        "error_rate": r.error_rate,
                        "p50_ms": r.p50_latency,
                        "p95_ms": r.p95_latency,
                        "p99_ms": r.p99_latency,
                        "duration_seconds": r.duration_seconds,
                    }
                    for r in results
                ],
                f,
                indent=2,
            )
        print(f"\nResults saved to {output}")


if __name__ == "__main__":
    main()
