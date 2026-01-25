"""FastAPI serving layer — REST API for telemetry metrics."""

from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import (
    make_asgi_app,
    Counter,
    Histogram,
)

from src.serving.metrics_store import MetricsStore
from src.utils.config import load_config

app = FastAPI(
    title="Telemetry Platform API",
    description="Enterprise Product Telemetry & Decision Intelligence API",
    version="1.0.0",
)

config = load_config()
app.add_middleware(
    CORSMiddleware,
    allow_origins=config["api"]["cors_origins"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount Prometheus metrics endpoint
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# Prometheus instrumentation
REQUEST_COUNT = Counter(
    "api_requests_total", "Total API requests", ["endpoint", "method"]
)
REQUEST_LATENCY = Histogram(
    "api_request_latency_seconds", "API request latency", ["endpoint"]
)

store = MetricsStore()


@app.get("/health")
async def health_check():
    """Service health and dependency status."""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "dependencies": store.check_dependencies(),
    }


@app.get("/metrics/usage")
async def get_usage_metrics(
    region: Optional[str] = Query(None, description="Filter by region"),
    device_type: Optional[str] = Query(None, description="Filter by device type"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    granularity: str = Query("daily", description="daily or hourly"),
):
    """Active users, session counts, feature usage stats."""
    REQUEST_COUNT.labels(endpoint="/metrics/usage", method="GET").inc()
    with REQUEST_LATENCY.labels(endpoint="/metrics/usage").time():
        return store.get_usage_metrics(
            region=region,
            device_type=device_type,
            start_date=start_date,
            end_date=end_date,
            granularity=granularity,
        )


@app.get("/metrics/crashes")
async def get_crash_metrics(
    os: Optional[str] = Query(None, description="Filter by OS"),
    app_version: Optional[str] = Query(None, description="Filter by app version"),
    device_type: Optional[str] = Query(None, description="Filter by device type"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
):
    """Crash rates by device type, OS, app version."""
    REQUEST_COUNT.labels(endpoint="/metrics/crashes", method="GET").inc()
    with REQUEST_LATENCY.labels(endpoint="/metrics/crashes").time():
        return store.get_crash_metrics(
            os=os,
            app_version=app_version,
            device_type=device_type,
            start_date=start_date,
            end_date=end_date,
        )


@app.get("/metrics/adoption")
async def get_adoption_metrics(
    feature: Optional[str] = Query(None, description="Filter by feature name"),
    top_n: int = Query(10, description="Number of top features to return"),
):
    """Feature adoption rates and trends."""
    REQUEST_COUNT.labels(endpoint="/metrics/adoption", method="GET").inc()
    with REQUEST_LATENCY.labels(endpoint="/metrics/adoption").time():
        return store.get_adoption_metrics(feature=feature, top_n=top_n)


@app.get("/metrics/performance")
async def get_performance_metrics(
    device_type: Optional[str] = Query(None, description="Filter by device type"),
    os: Optional[str] = Query(None, description="Filter by OS"),
    metric_name: Optional[str] = Query(None, description="Filter by metric name"),
):
    """App performance metrics (latency, load times, resource usage)."""
    REQUEST_COUNT.labels(endpoint="/metrics/performance", method="GET").inc()
    with REQUEST_LATENCY.labels(endpoint="/metrics/performance").time():
        return store.get_performance_metrics(
            device_type=device_type,
            os=os,
            metric_name=metric_name,
        )


@app.get("/metrics/realtime")
async def get_realtime_metrics():
    """Current real-time streaming metrics snapshot."""
    REQUEST_COUNT.labels(endpoint="/metrics/realtime", method="GET").inc()
    with REQUEST_LATENCY.labels(endpoint="/metrics/realtime").time():
        return store.get_realtime_snapshot()


def start():
    """Entry point for the API server."""
    import uvicorn

    uvicorn.run(
        "src.serving.api:app",
        host=config["api"]["host"],
        port=config["api"]["port"],
        reload=False,
        workers=2,
    )


if __name__ == "__main__":
    start()
