"""Streaming detector: consume `transactions`, emit `alerts`, expose Prometheus metrics.

Delivery semantics: at-least-once. Offsets are committed only after a batch is
fully processed and its alerts are flushed, so a crash replays (never drops)
transactions. Malformed messages go to a dead-letter topic instead of blocking
the partition.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import time

from confluent_kafka import Consumer, KafkaException, Producer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from app.rules import RuleConfig, RuleEngine, validate

log = logging.getLogger("detector")

TXNS = Counter("txn_processed_total", "Transactions processed")
ALERTS = Counter("txn_alerts_total", "Alerts raised", ["rule"])
DLQ = Counter("txn_dead_letter_total", "Malformed messages sent to the dead-letter topic")
PROC = Histogram("txn_processing_seconds", "Per-transaction rule evaluation time",
                 buckets=(1e-5, 2.5e-5, 5e-5, 1e-4, 2.5e-4, 5e-4, 1e-3, 5e-3))
E2E = Histogram("txn_end_to_end_seconds", "Kafka produce timestamp to alert decision",
                buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5))
LAG = Gauge("txn_consumer_lag", "Messages behind the log end", ["partition"])
CARDS = Gauge("txn_tracked_cards", "Cards with in-memory state")


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


class Detector:
    def __init__(self, bootstrap: str, group: str = "txn-detector", in_topic: str = "transactions",
                 alert_topic: str = "alerts", dlq_topic: str = "transactions.dlq",
                 cfg: RuleConfig | None = None):
        self.in_topic, self.alert_topic, self.dlq_topic = in_topic, alert_topic, dlq_topic
        self.engine = RuleEngine(cfg)
        self.consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "partition.assignment.strategy": "cooperative-sticky",
            "fetch.wait.max.ms": 20,          # don't let an idle fetch add latency
        })
        self.producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 5,
                                  "enable.idempotence": True, "compression.type": "lz4"})
        self._running = True
        self._last_lag = 0.0
        self.processed = 0
        self.latencies: list[float] | None = None   # set to [] to record exact e2e latencies (benchmarks)

    def stop(self, *_):
        self._running = False

    def _report_lag(self):
        if time.time() - self._last_lag < 5:
            return
        self._last_lag = time.time()
        for tp in self.consumer.assignment():
            try:
                _, high = self.consumer.get_watermark_offsets(tp, timeout=1, cached=True)
                pos = self.consumer.position([tp])[0].offset
                if high >= 0 and pos >= 0:
                    LAG.labels(str(tp.partition)).set(high - pos)
            except KafkaException:
                pass

    def handle(self, msg) -> None:
        try:
            txn = json.loads(msg.value())
        except (ValueError, TypeError):
            txn = None
        err = validate(txn) if txn is not None else "invalid JSON"
        if err:
            DLQ.inc()
            self.producer.produce(self.dlq_topic, key=msg.key(), value=msg.value(),
                                  headers=[("error", err.encode())])
            return
        t0 = time.perf_counter()
        alerts = self.engine.evaluate(txn)
        PROC.observe(time.perf_counter() - t0)
        TXNS.inc()
        self.processed += 1
        for a in alerts:
            ALERTS.labels(a.rule).inc()
            self.producer.produce(self.alert_topic, key=a.card_id, value=json.dumps(a.to_dict()))
        _, produced_ms = msg.timestamp()
        if produced_ms > 0:
            lat = max(time.time() - produced_ms / 1000.0, 0.0)
            E2E.observe(lat)
            if self.latencies is not None:
                self.latencies.append(lat)

    def _checkpoint(self) -> None:
        self.producer.flush(10)                  # alerts durable before we commit
        self.consumer.commit(asynchronous=False) # at-least-once
        self._uncommitted = 0
        self._last_commit = time.time()

    def run(self, batch_size: int = 500, commit_every_s: float = 1.0, commit_every_n: int = 5000) -> None:
        self.consumer.subscribe([self.in_topic])
        self._uncommitted, self._last_commit = 0, time.time()
        try:
            while self._running:
                # consume() waits for a full batch OR the timeout, so a short timeout bounds latency
                msgs = self.consumer.consume(num_messages=batch_size, timeout=0.02)
                if not msgs:
                    if self._uncommitted:
                        self._checkpoint()
                    continue
                for m in msgs:
                    if m.error():
                        log.warning("consume error: %s", m.error())
                        continue
                    self.handle(m)
                self._uncommitted += len(msgs)
                self.producer.poll(0)
                # batch commits: bounded replay on crash, without a broker round trip per poll
                if self._uncommitted >= commit_every_n or time.time() - self._last_commit >= commit_every_s:
                    self._checkpoint()
                CARDS.set(self.engine.tracked_cards)
                self._report_lag()
        finally:
            if self._uncommitted:
                self._checkpoint()
            self.consumer.close()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    start_http_server(int(env("METRICS_PORT", "8000")))
    d = Detector(env("KAFKA_BOOTSTRAP", "localhost:9092"), group=env("GROUP_ID", "txn-detector"))
    signal.signal(signal.SIGTERM, d.stop)
    signal.signal(signal.SIGINT, d.stop)
    log.info("detector started")
    d.run()


if __name__ == "__main__":
    main()
