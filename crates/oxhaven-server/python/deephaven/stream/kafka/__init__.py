"""Consuming and producing Kafka streams.

Behavior: `deephaven.stream.kafka`. `docs/KAFKA.md` says what is built.
"""

from ..._java_time import NotPorted


def topics(kafka_config: dict) -> list[str]:
    """The names of the topics a broker has."""
    raise NotPorted("io.deephaven.kafka.KafkaTools#listTopics")
