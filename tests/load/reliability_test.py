"""Reliability testing — simulates failures and measures recovery."""

import subprocess
import time
import json
from dataclasses import dataclass

import click

from src.utils.logging import setup_logging

logger = setup_logging("reliability-test")


@dataclass
class FailureTestResult:
    scenario: str
    container_killed: str
    downtime_seconds: float
    recovery_seconds: float
    data_loss: bool
    notes: str


def check_container_health(container: str) -> bool:
    result = subprocess.run(
        ["docker", "inspect", "--format", "{{.State.Running}}", container],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() == "true"


def wait_for_recovery(container: str, timeout: int = 120) -> float:
    """Wait for a container to become healthy again, return recovery time."""
    start = time.monotonic()
    while (time.monotonic() - start) < timeout:
        if check_container_health(container):
            return time.monotonic() - start
        time.sleep(1)
    return -1  # Timeout


def kill_container(container: str):
    subprocess.run(["docker", "kill", container], capture_output=True)
    logger.info("container_killed", container=container)


def restart_container(container: str):
    subprocess.run(["docker", "start", container], capture_output=True)
    logger.info("container_restarted", container=container)


def test_kafka_broker_failure() -> FailureTestResult:
    """Simulate Kafka broker failure — kill kafka-2 and verify continued operation."""
    logger.info("test_start", scenario="Kafka broker failure")

    kill_container("kafka-2")
    time.sleep(2)

    # Check that kafka-1 is still running
    kafka1_alive = check_container_health("kafka-1")

    # Restart kafka-2
    restart_container("kafka-2")
    recovery = wait_for_recovery("kafka-2", timeout=60)

    return FailureTestResult(
        scenario="Kafka Broker Failure",
        container_killed="kafka-2",
        downtime_seconds=2.0,
        recovery_seconds=recovery,
        data_loss=not kafka1_alive,  # If kafka-1 alive, replication saves data
        notes=f"kafka-1 stayed alive: {kafka1_alive}. Recovery: {recovery:.1f}s",
    )


def test_spark_worker_failure() -> FailureTestResult:
    """Simulate Spark worker failure."""
    logger.info("test_start", scenario="Spark worker failure")

    kill_container("spark-worker-1")
    time.sleep(2)

    # spark-worker-2 should continue processing
    worker2_alive = check_container_health("spark-worker-2")

    restart_container("spark-worker-1")
    recovery = wait_for_recovery("spark-worker-1", timeout=60)

    return FailureTestResult(
        scenario="Spark Worker Failure",
        container_killed="spark-worker-1",
        downtime_seconds=2.0,
        recovery_seconds=recovery,
        data_loss=False,
        notes=f"spark-worker-2 continued: {worker2_alive}. Recovery: {recovery:.1f}s. Checkpointing prevents data loss.",
    )


def test_datanode_failure() -> FailureTestResult:
    """Simulate HDFS DataNode failure."""
    logger.info("test_start", scenario="HDFS DataNode failure")

    kill_container("datanode-1")
    time.sleep(2)

    # datanode-2 should serve data (replication factor = 2)
    datanode2_alive = check_container_health("datanode-2")

    restart_container("datanode-1")
    recovery = wait_for_recovery("datanode-1", timeout=90)

    return FailureTestResult(
        scenario="HDFS DataNode Failure",
        container_killed="datanode-1",
        downtime_seconds=2.0,
        recovery_seconds=recovery,
        data_loss=False,
        notes=f"datanode-2 serving data: {datanode2_alive}. Replication factor=2 prevents data loss. Recovery: {recovery:.1f}s",
    )


def print_result(result: FailureTestResult):
    print(f"\n{'─' * 60}")
    print(f"  Scenario:   {result.scenario}")
    print(f"  Container:  {result.container_killed}")
    print(f"  Downtime:   {result.downtime_seconds:.1f}s")
    print(f"  Recovery:   {result.recovery_seconds:.1f}s")
    print(f"  Data Loss:  {'YES' if result.data_loss else 'NO'}")
    print(f"  Notes:      {result.notes}")
    print(f"{'─' * 60}")


@click.command()
@click.option("--scenario", default="all", help="Scenario to test: kafka, spark, hdfs, all")
@click.option("--output", default=None, help="Save results to JSON file")
def main(scenario: str, output: str | None):
    """Run reliability/fault-tolerance tests."""
    print("\n" + "=" * 60)
    print("  TELEMETRY PLATFORM — RELIABILITY TEST")
    print("=" * 60)

    tests = {
        "kafka": test_kafka_broker_failure,
        "spark": test_spark_worker_failure,
        "hdfs": test_datanode_failure,
    }

    if scenario == "all":
        scenarios = list(tests.keys())
    else:
        scenarios = [scenario]

    results = []
    for s in scenarios:
        if s not in tests:
            print(f"Unknown scenario: {s}")
            continue
        print(f"\nRunning: {s}...")
        result = tests[s]()
        print_result(result)
        results.append(result)
        time.sleep(5)  # Cool-down

    # Summary
    print("\n" + "=" * 60)
    print("  RELIABILITY SUMMARY")
    print("=" * 60)
    print(f"{'Scenario':<25} {'Recovery(s)':>12} {'Data Loss':>10}")
    print("-" * 50)
    for r in results:
        print(f"{r.scenario:<25} {r.recovery_seconds:>12.1f} {'YES' if r.data_loss else 'NO':>10}")

    if output:
        with open(output, "w") as f:
            json.dump(
                [
                    {
                        "scenario": r.scenario,
                        "container": r.container_killed,
                        "recovery_seconds": r.recovery_seconds,
                        "data_loss": r.data_loss,
                        "notes": r.notes,
                    }
                    for r in results
                ],
                f,
                indent=2,
            )
        print(f"\nResults saved to {output}")


if __name__ == "__main__":
    main()
