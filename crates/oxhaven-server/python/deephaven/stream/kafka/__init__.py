"""Consuming and producing Kafka streams.

Behavior: `deephaven.stream.kafka`. `docs/KAFKA.md` says what is built.
"""

from ... import _engine
from ...dherror import DHError


def _properties(kafka_config: dict) -> list[tuple[str, str]]:
    """The properties as pairs of text, refusing what the reference cannot store.

    Behavior: `deephaven.jcompat.j_properties`, which calls
    `java.util.Properties#setProperty(String, String)` with each name and value as it is:
    jpy finds no such method for a number or a bool, and `None` is a Java null.
    """
    pairs = []
    for name, value in kafka_config.items():
        if name is None or value is None:
            raise RuntimeError("java.lang.NullPointerException")
        if not isinstance(name, str) or not isinstance(value, str):
            raise RuntimeError("no matching Java method overloads found")
        pairs.append((name, value))
    return pairs


def topics(kafka_config: dict) -> list[str]:
    """The names of the topics a broker has, without Kafka's internal ones.

    `kafka_config` holds the properties of the client, `bootstrap.servers` among them.
    """
    try:
        return _engine.native.kafka_topics(_properties(kafka_config))
    except Exception as e:
        raise DHError(e, "failed to list Kafka topics") from e
