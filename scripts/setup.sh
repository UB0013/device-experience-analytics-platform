#!/bin/bash
# Setup script for the Telemetry Platform

set -e

echo "================================================"
echo "  Telemetry Platform — Setup"
echo "================================================"

# 1. Install Python dependencies
echo ""
echo "[1/4] Installing Python dependencies..."
pip install -r requirements.txt

# 2. Start infrastructure
echo ""
echo "[2/4] Starting infrastructure services..."
docker-compose up -d

# 3. Wait for services
echo ""
echo "[3/4] Waiting for services to initialize (30s)..."
sleep 30

# 4. Verify
echo ""
echo "[4/4] Running health check..."
python -m src.monitoring.health_check -v

echo ""
echo "================================================"
echo "  Setup complete!"
echo ""
echo "  Next steps:"
echo "    make produce        # Start event generation"
echo "    make stream-start   # Start stream processing"
echo "    make api-start      # Start REST API"
echo ""
echo "  Dashboards:"
echo "    Grafana:   http://localhost:3000"
echo "    Spark UI:  http://localhost:8080"
echo "    HDFS UI:   http://localhost:9870"
echo "    API Docs:  http://localhost:8000/docs"
echo "================================================"
