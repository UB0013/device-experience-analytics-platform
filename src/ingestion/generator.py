"""Telemetry event generator — simulates realistic device/app events."""

import random
import uuid
from datetime import datetime

from src.utils.config import get_generator_config
from src.utils.schemas import (
    TelemetryEvent,
    EventType,
    DeviceType,
    AppOpenPayload,
    FeatureUsedPayload,
    CrashPayload,
    PerformancePayload,
)

FEATURES = [
    "search", "checkout", "profile", "settings", "notifications",
    "dashboard", "analytics", "export", "share", "comments",
    "file_upload", "calendar", "chat", "video_call", "reports",
]

CRASH_TYPES = [
    "NullPointerException", "OutOfMemoryError", "IndexOutOfBoundsException",
    "NetworkTimeoutException", "IllegalStateException", "SecurityException",
    "ConcurrentModificationException", "StackOverflowError",
]

PERFORMANCE_METRICS = [
    ("screen_load_time", "ms", 100, 5000),
    ("api_response_time", "ms", 50, 3000),
    ("memory_usage", "MB", 50, 500),
    ("cpu_usage", "percent", 5, 95),
    ("battery_drain_rate", "percent_per_hour", 1, 15),
    ("frame_rate", "fps", 15, 60),
    ("network_latency", "ms", 10, 500),
]

SCREENS = [
    "HomeScreen", "SearchScreen", "ProductDetail", "CartScreen",
    "CheckoutScreen", "ProfileScreen", "SettingsScreen", "DashboardScreen",
]


class TelemetryGenerator:
    """Generates realistic telemetry events with configurable distributions."""

    def __init__(self):
        cfg = get_generator_config()
        self.regions = cfg["regions"]
        self.device_types = [dt.value for dt in DeviceType]
        self.operating_systems = cfg["operating_systems"]
        self.app_versions = cfg["app_versions"]

        # Simulate a pool of devices
        self._device_pool = [str(uuid.uuid4()) for _ in range(500)]

        # Event type weights (app_open and feature_used are most common)
        self._event_weights = {
            EventType.APP_OPEN: 0.25,
            EventType.FEATURE_USED: 0.50,
            EventType.CRASH_EVENT: 0.05,
            EventType.PERFORMANCE_METRIC: 0.20,
        }

    def generate_event(self) -> TelemetryEvent:
        event_type = random.choices(
            list(self._event_weights.keys()),
            weights=list(self._event_weights.values()),
            k=1,
        )[0]

        device_id = random.choice(self._device_pool)
        device_type = random.choice(self.device_types)
        os = self._pick_os_for_device(device_type)

        base = {
            "event_type": event_type,
            "timestamp": datetime.utcnow(),
            "device_id": device_id,
            "device_type": device_type,
            "os": os,
            "app_version": random.choice(self.app_versions),
            "region": random.choice(self.regions),
        }

        payload = self._generate_payload(event_type)
        base["payload"] = payload.model_dump() if payload else {}

        return TelemetryEvent(**base)

    def generate_batch(self, size: int) -> list[TelemetryEvent]:
        return [self.generate_event() for _ in range(size)]

    def _pick_os_for_device(self, device_type: str) -> str:
        if device_type == "mobile":
            return random.choices(["iOS", "Android"], weights=[0.45, 0.55])[0]
        elif device_type == "tablet":
            return random.choices(["iOS", "Android"], weights=[0.6, 0.4])[0]
        else:
            return random.choices(
                ["Windows", "macOS", "Linux"], weights=[0.65, 0.25, 0.10]
            )[0]

    def _generate_payload(self, event_type: EventType):
        if event_type == EventType.APP_OPEN:
            return AppOpenPayload(
                launch_time_ms=random.randint(200, 8000),
                is_cold_start=random.random() < 0.3,
            )
        elif event_type == EventType.FEATURE_USED:
            return FeatureUsedPayload(
                feature_name=random.choice(FEATURES),
                duration_ms=random.randint(500, 60000),
                interaction_count=random.randint(1, 50),
            )
        elif event_type == EventType.CRASH_EVENT:
            return CrashPayload(
                crash_type=random.choice(CRASH_TYPES),
                stack_trace=f"at com.app.module.Class.method(Class.java:{random.randint(10, 500)})",
                is_fatal=random.random() < 0.2,
                memory_usage_mb=round(random.uniform(100, 450), 1),
            )
        elif event_type == EventType.PERFORMANCE_METRIC:
            metric = random.choice(PERFORMANCE_METRICS)
            return PerformancePayload(
                metric_name=metric[0],
                value=round(random.uniform(metric[2], metric[3]), 2),
                unit=metric[1],
                screen_name=random.choice(SCREENS),
            )
        return None
