"""Tests for the telemetry event generator."""

import json
from datetime import datetime

from src.ingestion.generator import TelemetryGenerator
from src.utils.schemas import TelemetryEvent, EventType, DeviceType


class TestTelemetryGenerator:
    def setup_method(self):
        self.generator = TelemetryGenerator()

    def test_generate_single_event(self):
        event = self.generator.generate_event()
        assert isinstance(event, TelemetryEvent)
        assert event.event_id is not None
        assert event.event_type in EventType
        assert event.device_type in [dt.value for dt in DeviceType]
        assert isinstance(event.timestamp, datetime)

    def test_generate_batch(self):
        batch = self.generator.generate_batch(100)
        assert len(batch) == 100
        assert all(isinstance(e, TelemetryEvent) for e in batch)

    def test_event_serialization(self):
        event = self.generator.generate_event()
        kafka_bytes = event.to_kafka_value()
        assert isinstance(kafka_bytes, bytes)

        parsed = json.loads(kafka_bytes)
        assert "event_id" in parsed
        assert "event_type" in parsed
        assert "device_id" in parsed

    def test_event_deserialization(self):
        event = self.generator.generate_event()
        kafka_bytes = event.to_kafka_value()
        restored = TelemetryEvent.from_kafka_value(kafka_bytes)
        assert restored.event_id == event.event_id
        assert restored.event_type == event.event_type
        assert restored.device_id == event.device_id

    def test_event_type_distribution(self):
        events = self.generator.generate_batch(10000)
        type_counts = {}
        for e in events:
            type_counts[e.event_type] = type_counts.get(e.event_type, 0) + 1

        # feature_used should be the most common (~50%)
        assert type_counts[EventType.FEATURE_USED] > type_counts[EventType.CRASH_EVENT]
        # crash_event should be rare (~5%)
        assert type_counts[EventType.CRASH_EVENT] < 1500

    def test_os_matches_device_type(self):
        for _ in range(500):
            event = self.generator.generate_event()
            if event.device_type == "mobile":
                assert event.os in ["iOS", "Android"]
            elif event.device_type == "desktop":
                assert event.os in ["Windows", "macOS", "Linux"]

    def test_payload_not_empty(self):
        events = self.generator.generate_batch(100)
        for event in events:
            assert isinstance(event.payload, dict)
            assert len(event.payload) > 0

    def test_all_regions_represented(self):
        events = self.generator.generate_batch(5000)
        regions = {e.region for e in events}
        assert len(regions) >= 4  # At least 4 of 6 regions should appear
