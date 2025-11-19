"""Telemetry event schemas — shared across all layers."""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field
import uuid as _uuid


class EventType(str, Enum):
    APP_OPEN = "app_open"
    FEATURE_USED = "feature_used"
    CRASH_EVENT = "crash_event"
    PERFORMANCE_METRIC = "performance_metric"


class DeviceType(str, Enum):
    MOBILE = "mobile"
    DESKTOP = "desktop"
    TABLET = "tablet"


class TelemetryEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(_uuid.uuid4()))
    event_type: EventType
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    device_id: str
    device_type: DeviceType
    os: str
    app_version: str
    region: str
    payload: dict[str, Any] = Field(default_factory=dict)

    def to_kafka_value(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_kafka_value(cls, value: bytes) -> "TelemetryEvent":
        return cls.model_validate_json(value)


class AppOpenPayload(BaseModel):
    session_id: str = Field(default_factory=lambda: str(_uuid.uuid4()))
    launch_time_ms: int
    is_cold_start: bool


class FeatureUsedPayload(BaseModel):
    feature_name: str
    duration_ms: int
    interaction_count: int


class CrashPayload(BaseModel):
    crash_type: str
    stack_trace: str
    is_fatal: bool
    memory_usage_mb: float


class PerformancePayload(BaseModel):
    metric_name: str
    value: float
    unit: str
    screen_name: str | None = None
