"""Integration tests — requires running Kafka cluster."""

import json
import time
import pytest
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic

from src.ingestion.generator import TelemetryGenerator
from src.utils.schemas import TelemetryEvent

KAFKA_BROKERS = "localhost:9092"
TEST_TOPIC = "test_integration_events"


@pytest.fixture(scope="module")
def kafka_admin():
    admin = AdminClient({"bootstrap.servers": KAFKA_BROKERS})
    # Create test topic
    topic = NewTopic(TEST_TOPIC, num_partitions=2, replication_factor=1)
    admin.create_topics([topic])
    time.sleep(2)
    yield admin
    # Cleanup
    admin.delete_topics([TEST_TOPIC])


@pytest.fixture
def producer():
    return Producer({"bootstrap.servers": KAFKA_BROKERS})


@pytest.fixture
def consumer():
    c = Consumer({
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id": "test-consumer-group",
        "auto.offset.reset": "earliest",
    })
    c.subscribe([TEST_TOPIC])
    yield c
    c.close()


class TestKafkaPipeline:
    def test_produce_and_consume_event(self, kafka_admin, producer, consumer):
        generator = TelemetryGenerator()
        event = generator.generate_event()

        # Produce
        producer.produce(
            TEST_TOPIC,
            key=event.device_id.encode(),
            value=event.to_kafka_value(),
        )
        producer.flush()

        # Consume
        msg = consumer.poll(timeout=10.0)
        assert msg is not None
        assert msg.error() is None

        consumed_event = TelemetryEvent.from_kafka_value(msg.value())
        assert consumed_event.event_id == event.event_id
        assert consumed_event.event_type == event.event_type

    def test_produce_batch(self, kafka_admin, producer, consumer):
        generator = TelemetryGenerator()
        batch = generator.generate_batch(50)

        for event in batch:
            producer.produce(
                TEST_TOPIC,
                key=event.device_id.encode(),
                value=event.to_kafka_value(),
            )
        producer.flush()

        consumed = 0
        deadline = time.time() + 15
        while consumed < 50 and time.time() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg and not msg.error():
                consumed += 1

        assert consumed == 50

    def test_event_serialization_integrity(self, kafka_admin, producer, consumer):
        generator = TelemetryGenerator()
        events = generator.generate_batch(10)

        for event in events:
            producer.produce(
                TEST_TOPIC,
                key=event.device_id.encode(),
                value=event.to_kafka_value(),
            )
        producer.flush()

        deadline = time.time() + 10
        consumed_ids = set()
        while time.time() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg and not msg.error():
                parsed = TelemetryEvent.from_kafka_value(msg.value())
                consumed_ids.add(parsed.event_id)

        original_ids = {e.event_id for e in events}
        assert consumed_ids == original_ids
