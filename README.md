# Device Experience Analytics Platform

Distributed data platform for ingesting and analyzing product telemetry at scale. Uses Lambda Architecture — real-time stream processing + batch analytics on the same data.

## Architecture

```
  Telemetry Generators (Python)
          |
      Apache Kafka
    (app_events, crashes, performance)
          |
    ------+------
    |            |
  Spark        HDFS
  Streaming    (raw data)
    |            |
    |        Spark Batch + Hive
    |            |
    +-----+------+
          |
    Serving Layer
    (FastAPI, Grafana)
```

## Tech Stack

- **Ingestion**: Kafka (2 brokers, topic partitioning, LZ4 compression)
- **Processing**: Spark Streaming (real-time), Spark + Hive (batch)
- **Storage**: HDFS (raw → processed → historical), optional Azure Blob
- **API**: FastAPI with Prometheus instrumentation
- **Monitoring**: Prometheus + Grafana dashboards
- **Infra**: Docker Compose (Kafka, Hadoop, Spark, Hive, Grafana, Prometheus)

## Getting Started

**Requirements:** Docker, Python 3.11+, 8GB+ RAM

```bash
# start everything
docker-compose up -d
pip install -r requirements.txt

# generate telemetry events (1000/sec)
python -m src.ingestion.producer --rate 1000

# start stream processing
make stream-start

# start API
make api-start  # http://localhost:8000/docs
```

**Dashboards:** Grafana at `localhost:3000` (admin/admin), Spark UI at `localhost:8080`, HDFS at `localhost:9870`

## What It Does

The platform simulates device/app telemetry (app opens, feature usage, crashes, performance metrics) from 500 devices across 6 regions, then processes it two ways:

**Real-time (speed layer):** Spark Streaming computes active users per minute, detects error spikes in 30s windows, and tracks feature usage — all written to HDFS as Parquet.

**Batch (batch layer):** Spark jobs run daily analytics — usage trends, feature adoption, retention cohorts, device performance (p50/p95/p99), crash rates. Hive tables sit on top for SQL queries.

**Serving:** FastAPI exposes `/metrics/usage`, `/metrics/crashes`, `/metrics/adoption`, `/metrics/performance` endpoints. Grafana dashboards visualize throughput, latency, and error rates.

## Project Structure

```
src/
├── ingestion/    # event generator + Kafka producer
├── streaming/    # Spark Streaming jobs
├── batch/        # Spark batch jobs + Hive SQL
├── serving/      # FastAPI REST API
├── storage/      # HDFS data lake manager
├── monitoring/   # health checks + exporters
└── utils/        # schemas, config, logging
tests/
├── unit/         # generator, schema, API tests
├── integration/  # Kafka pipeline tests
└── load/         # throughput + reliability tests
```

## Testing

```bash
make test           # unit tests
make test-integration  # requires running Kafka
make load-test      # progressive load: 1K → 5K → 10K events/sec
```

Reliability testing covers broker failure, worker crashes, and DataNode loss — recovery time and data loss are measured. See `tests/load/reliability_test.py`.

## Fault Tolerance

- Kafka replication factor = 2 across brokers
- Spark checkpointing for stream recovery
- HDFS block replication across DataNodes
- Tested scenarios: kill broker, stop worker, drop DataNode

## License

MIT
