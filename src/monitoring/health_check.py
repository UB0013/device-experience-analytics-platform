"""Health check utility — verifies all platform components are running."""

import sys
import socket
from datetime import datetime

import click

from src.utils.logging import setup_logging

logger = setup_logging("health-check")

SERVICES = {
    "Zookeeper": ("localhost", 2181),
    "Kafka Broker 1": ("localhost", 9092),
    "Kafka Broker 2": ("localhost", 9093),
    "HDFS NameNode (Web)": ("localhost", 9870),
    "HDFS NameNode (RPC)": ("localhost", 9000),
    "Spark Master": ("localhost", 8080),
    "Hive Metastore": ("localhost", 9083),
    "Prometheus": ("localhost", 9090),
    "Grafana": ("localhost", 3000),
    "Telemetry API": ("localhost", 8000),
}


def check_port(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (ConnectionRefusedError, TimeoutError, OSError):
        return False


@click.command()
@click.option("--verbose", "-v", is_flag=True, help="Show all services including healthy ones")
def main(verbose: bool):
    """Check health of all platform services."""
    print(f"\nPlatform Health Check — {datetime.utcnow().isoformat()}Z\n")
    print(f"{'Service':<25} {'Endpoint':<20} {'Status'}")
    print("-" * 60)

    all_healthy = True
    for name, (host, port) in SERVICES.items():
        healthy = check_port(host, port)
        status = "UP" if healthy else "DOWN"
        marker = "  " if healthy else "  "

        if not healthy:
            all_healthy = False
        if verbose or not healthy:
            print(f"{marker} {name:<23} {host}:{port:<13} {status}")

    print("-" * 60)
    if all_healthy:
        print("All services are healthy.\n")
    else:
        print("Some services are down. Run 'docker-compose ps' for details.\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
