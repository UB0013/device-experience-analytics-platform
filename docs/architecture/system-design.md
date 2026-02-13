# System Architecture — Lambda Architecture

## Overview

This platform follows the **Lambda Architecture** pattern, combining:
- **Speed Layer** (real-time): Spark Structured Streaming
- **Batch Layer** (historical): Spark Batch + Hive
- **Serving Layer** (query): FastAPI REST API + Dashboards

## Data Flow

```
Telemetry Generators (Python)
        │
        ▼
   Apache Kafka (3 topics)
   ├── app_events   (partitions=6, RF=2)
   ├── crashes       (partitions=6, RF=2)
   └── performance   (partitions=6, RF=2)
        │                         │
        ▼                         ▼
   Speed Layer               Raw Storage
   (Spark Streaming)         (HDFS /data/raw/)
        │                         │
        ▼                         ▼
   Real-time Metrics         Batch Layer
   (HDFS /data/processed/    (Spark Batch Jobs)
    metrics/)                     │
        │                         ▼
        │                    Processed Data
        │                    (HDFS /data/processed/
        │                     batch/)
        │                         │
        ▼                         ▼
   ┌─────────────────────────────────────┐
   │          Serving Layer              │
   │   FastAPI  │  Grafana  │  Superset  │
   └─────────────────────────────────────┘
```

## Component Details

### Ingestion Layer
- Python producers using confluent-kafka
- Configurable event generation rate (100 - 10,000+ events/sec)
- 4 event types with realistic distributions
- LZ4 compression for efficient transport

### Speed Layer (Real-Time)
- Spark Structured Streaming with 5-second micro-batches
- 10-second watermark for late event handling
- Computes: active users/min, error spikes, feature usage
- Results written to HDFS as Parquet

### Batch Layer (Historical)
- Daily batch jobs for deep analytics
- 7 analysis types: trends, adoption, retention, performance, crashes
- Hive tables for SQL-based BI queries
- Parquet storage with columnar compression

### Storage Layer
- HDFS with replication factor 2
- Three-tier organization: raw → processed → historical
- Automated archival and checkpoint cleanup

### Serving Layer
- FastAPI with 5 metric endpoints
- Prometheus instrumentation on all requests
- Fallback to sample data when Spark unavailable
- CORS-enabled for dashboard integration

### Monitoring
- Prometheus scraping all components
- Grafana dashboards: throughput, latency, errors, API metrics
- Health check utility for all services
