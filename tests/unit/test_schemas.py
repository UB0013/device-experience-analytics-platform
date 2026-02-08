"""Tests for telemetry event schemas."""

import json
from datetime import datetime

from src.utils.schemas import (
    TelemetryEvent,
    EventType,
    DeviceType,
    AppOpenPayload,
    FeatureUsedPayload,
    CrashPayload,
    PerformancePayload,
)


class TestTelemetryEvent:
    def test_create_event(self):
        event = TelemetryEvent(
            event_type=EventType.APP_OPEN,
            device_id="test-device-001",
            device_type=DeviceType.MOBILE,
            os="iOS",
            app_version="2.4.1",
            region="us-east",
        )
        assert event.event_id is not None
        assert event.event_type == EventType.APP_OPEN
        assert event.device_type == DeviceType.MOBILE

    def test_auto_generated_fields(self):
        event = TelemetryEvent(
            event_type=EventType.FEATURE_USED,
            device_id="test-device",
            device_type=DeviceType.DESKTOP,
            os="Windows",
            app_version="2.4.0",
            region="eu-west",
        )
        assert len(event.event_id) == 36  # UUID format
        assert isinstance(event.timestamp, datetime)

    def test_serialization_roundtrip(self):
        event = TelemetryEvent(
            event_type=EventType.CRASH_EVENT,
            device_id="test-device-002",
            device_type=DeviceType.TABLET,
            os="Android",
            app_version="2.5.0-beta",
            region="ap-south",
            payload={"crash_type": "NullPointerException", "is_fatal": "true"},
        )
        serialized = event.to_kafka_value()
        deserialized = TelemetryEvent.from_kafka_value(serialized)
        assert deserialized.event_id == event.event_id
        assert deserialized.payload == event.payload


class TestPayloads:
    def test_app_open_payload(self):
        payload = AppOpenPayload(launch_time_ms=1500, is_cold_start=True)
        assert payload.session_id is not None
        assert payload.launch_time_ms == 1500

    def test_feature_used_payload(self):
        payload = FeatureUsedPayload(
            feature_name="search", duration_ms=5000, interaction_count=12
        )
        assert payload.feature_name == "search"

    def test_crash_payload(self):
        payload = CrashPayload(
            crash_type="OutOfMemoryError",
            stack_trace="at com.app.Main.run(Main.java:42)",
            is_fatal=True,
            memory_usage_mb=380.5,
        )
        assert payload.is_fatal is True

    def test_performance_payload(self):
        payload = PerformancePayload(
            metric_name="screen_load_time",
            value=1250.5,
            unit="ms",
            screen_name="HomeScreen",
        )
        assert payload.unit == "ms"
