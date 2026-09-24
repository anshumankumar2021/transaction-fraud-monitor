"""Publish synthetic transactions to Kafka, keyed by card_id.

Keying by card_id keeps every card's events on one partition, preserving
per-card ordering for the stateful detector.
"""
from __future__ import annotations

import json
import logging
import os
import time

from confluent_kafka import Producer
from prometheus_client import Counter, start_http_server

from app.simulate import generate

log = logging.getLogger("producer")
SENT = Counter("txn_produced_total", "Transactions published")


def make_producer(bootstrap: str) -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,   # no duplicates on retry
        "acks": "all",
        "linger.ms": 5,
        "batch.num.messages": 10_000,
        "compression.type": "lz4",
    })


def publish(p: Producer, topic: str, events: list[dict], rate: float = 0.0) -> float:
    """Send events; rate=0 means as fast as possible. Returns elapsed seconds."""
    start = time.perf_counter()
    for i, e in enumerate(events):
        while True:
            try:
                p.produce(topic, key=e["card_id"], value=json.dumps(e))
                break
            except BufferError:
                p.poll(0.05)
        SENT.inc()
        if i % 1000 == 0:
            p.poll(0)
        if rate > 0:
            target = start + (i + 1) / rate
            delay = target - time.perf_counter()
            if delay > 0:
                time.sleep(delay)
    p.flush(60)
    return time.perf_counter() - start


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    start_http_server(int(os.environ.get("METRICS_PORT", "8001")))
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    rate = float(os.environ.get("RATE", "500"))
    events, _ = generate(n_cards=int(os.environ.get("CARDS", "20000")),
                         n_normal=int(os.environ.get("NORMAL_TXNS", "200000")),
                         n_incidents=int(os.environ.get("INCIDENTS", "1500")))
    p = make_producer(bootstrap)
    log.info("publishing %d events at %s msg/s", len(events), rate or "max")
    publish(p, os.environ.get("TOPIC", "transactions"), events, rate)
    log.info("done")
    offset = 0.0
    while os.environ.get("LOOP", "1") == "1":   # keep the demo stream alive
        offset += 7 * 86_400                     # keep event time moving forward
        events, _ = generate(seed=int(time.time()))
        for e in events:
            e["ts"] += offset
        publish(p, os.environ.get("TOPIC", "transactions"), events, rate)


if __name__ == "__main__":
    main()
