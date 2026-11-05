"""Envoi des messages vers Kafka (producteur idempotent, acks=all)."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from typing import Any, Protocol

log = logging.getLogger(__name__)


class Sink(Protocol):
    def send(self, topic: str, key: str, value: dict[str, Any]) -> None: ...
    def flush(self, timeout: float = 30) -> int: ...


class KafkaSink:
    def __init__(self, bootstrap_servers: str, client_id: str):
        from confluent_kafka import Producer  # import tardif : les tests n'en ont pas besoin

        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": client_id,
                "acks": "all",
                "enable.idempotence": True,  # pas de doublon en cas de retry réseau
                "compression.type": "lz4",
                "linger.ms": 50,
            }
        )
        self.errors = 0

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.errors += 1
            log.error("Échec de livraison vers %s : %s", msg.topic(), err)

    def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        self._producer.produce(
            topic,
            key=key.encode(),
            value=json.dumps(value, ensure_ascii=False).encode(),
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 30) -> int:
        return self._producer.flush(timeout)


class StdoutSink:
    """Affiche les messages au lieu de les envoyer (mode --dry-run)."""

    def __init__(self, limit: int = 3):
        self._limit = limit
        self._shown = 0

    def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        if self._shown < self._limit:
            print(f"[{topic}] key={key} {json.dumps(value, ensure_ascii=False)}")
            self._shown += 1

    def flush(self, timeout: float = 30) -> int:
        self._shown = 0
        return 0


def publish(sink: Sink, topic: str, key_field: str, messages: Iterable[dict[str, Any]]) -> int:
    count = 0
    for msg in messages:
        sink.send(topic, str(msg[key_field]), msg)
        count += 1
    remaining = sink.flush()
    if remaining:
        log.warning("%s messages non confirmés par Kafka après flush", remaining)
    return count
