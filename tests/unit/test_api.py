"""Tests for the FastAPI serving layer."""

from fastapi.testclient import TestClient

from src.serving.api import app


client = TestClient(app)


class TestHealthEndpoint:
    def test_health_returns_200(self):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data
        assert "dependencies" in data


class TestUsageEndpoint:
    def test_usage_returns_data(self):
        response = client.get("/metrics/usage")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data

    def test_usage_with_filters(self):
        response = client.get("/metrics/usage?region=us-east&device_type=mobile")
        assert response.status_code == 200

    def test_usage_with_date_range(self):
        response = client.get(
            "/metrics/usage?start_date=2025-01-01&end_date=2025-01-31"
        )
        assert response.status_code == 200


class TestCrashEndpoint:
    def test_crashes_returns_data(self):
        response = client.get("/metrics/crashes")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert len(data["data"]) > 0

    def test_crashes_with_os_filter(self):
        response = client.get("/metrics/crashes?os=Android")
        assert response.status_code == 200


class TestAdoptionEndpoint:
    def test_adoption_returns_data(self):
        response = client.get("/metrics/adoption")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data

    def test_adoption_top_n(self):
        response = client.get("/metrics/adoption?top_n=3")
        assert response.status_code == 200
        data = response.json()
        assert len(data["data"]) <= 3


class TestPerformanceEndpoint:
    def test_performance_returns_data(self):
        response = client.get("/metrics/performance")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data


class TestRealtimeEndpoint:
    def test_realtime_returns_snapshot(self):
        response = client.get("/metrics/realtime")
        assert response.status_code == 200
        data = response.json()
        assert "timestamp" in data
