"""Measure detection quality and throughput.

Offline (default): run the rule engine directly over the simulated stream.
Kafka (--kafka HOST:PORT): publish to Kafka, run the real consumer-group
detector, read the `alerts` topic back, and score it the same way.

Scoring is per incident: an injected incident counts as caught if any of its
transactions raised an alert. Precision is over alerted transactions.
"""
from __future__ import annotations

import argparse
import json
import threading
import time
import uuid

from app.rules import RuleEngine
from app.simulate import generate, incident_of


def score(events, labels, alerted: set[str], elapsed: float, label: str) -> dict:
    incidents = incident_of(labels)
    caught = {inc for inc, ids in incidents.items() if ids & alerted}
    by_kind = {}
    for k in ("amount_spike", "velocity", "impossible_travel"):
        tot = [i for i in incidents if i.endswith(k)]
        by_kind[k] = round(sum(i in caught for i in tot) / len(tot), 4) if tot else None
    tp = len(alerted & labels.keys())
    fp = len(alerted - labels.keys())
    res = {
        "mode": label,
        "events": len(events),
        "incidents": len(incidents),
        "incident_recall": round(len(caught) / len(incidents), 4),
        "recall_by_type": by_kind,
        "alerted_txns": len(alerted),
        "alert_precision": round(tp / (tp + fp), 4) if tp + fp else None,
        "false_positive_rate": round(fp / (len(events) - len(labels)), 6),
        "elapsed_s": round(elapsed, 2),
        "throughput_msgs_per_s": round(len(events) / elapsed),
    }
    print(json.dumps(res, indent=2))
    return res


def offline(args):
    events, labels = generate(args.cards, args.normal, args.incidents, seed=args.seed)
    eng = RuleEngine()
    alerted: set[str] = set()
    lat = []
    t0 = time.perf_counter()
    for e in events:
        s = time.perf_counter()
        if eng.evaluate(e):
            alerted.add(e["txn_id"])
        lat.append(time.perf_counter() - s)
    el = time.perf_counter() - t0
    lat.sort()
    print(f"rule eval p50={lat[len(lat)//2]*1e6:.1f}us p99={lat[int(len(lat)*.99)]*1e6:.1f}us")
    return score(events, labels, alerted, el, "offline")


def drain(bootstrap: str, topic: str, timeout_s: float = 60) -> list[bytes]:
    """Read a topic from the beginning until it goes quiet.

    Waits for a partition assignment first: real Kafka delays the first group
    rebalance (group.initial.rebalance.delay.ms, 3s by default), so polling
    "until empty" without this can exit before any partition is assigned.
    """
    from confluent_kafka import Consumer

    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"drain-{uuid.uuid4().hex[:6]}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic])
    out: list[bytes] = []
    deadline, empty = time.time() + timeout_s, 0
    while time.time() < deadline and empty < 3:
        batch = c.consume(1000, timeout=1)
        if not c.assignment():
            continue
        empty = empty + 1 if not batch else 0
        out.extend(m.value() for m in batch if not m.error())
    c.close()
    return out


def kafka(args):
    from confluent_kafka.admin import AdminClient, NewTopic

    from app.detector import Detector
    from app.producer import make_producer, publish

    run = uuid.uuid4().hex[:6]
    t_in, t_alert, t_dlq = f"transactions-{run}", f"alerts-{run}", f"dlq-{run}"
    admin = AdminClient({"bootstrap.servers": args.kafka})
    fs = admin.create_topics([NewTopic(t, num_partitions=args.partitions, replication_factor=1)
                              for t in (t_in, t_alert, t_dlq)])
    for f in fs.values():
        f.result(30)

    events, labels = generate(args.cards, args.normal, args.incidents, seed=args.seed)
    # a few malformed messages to exercise the dead-letter path
    prod = make_producer(args.kafka)
    for bad in (b"not json", b'{"txn_id": "x"}', b'{"txn_id":"y","card_id":"c","ts":1,"amount":-5,"country":"US","merchant":"m"}'):
        prod.produce(t_in, key=b"bad", value=bad)
    prod.flush(10)

    detectors = [Detector(args.kafka, group=f"det-{run}", in_topic=t_in, alert_topic=t_alert, dlq_topic=t_dlq)
                 for _ in range(args.replicas)]
    for d in detectors:
        d.latencies = []
    threads = [threading.Thread(target=d.run) for d in detectors]
    for t in threads:
        t.start()
    # wait until the group has settled and every partition is owned before measuring
    deadline = time.time() + 60
    while time.time() < deadline:
        counts = [len(d.consumer.assignment()) for d in detectors]
        if sum(counts) == args.partitions and min(counts) > 0:
            break
        time.sleep(0.5)
    time.sleep(3)
    print('partition assignment:', [len(d.consumer.assignment()) for d in detectors])
    for d in detectors:  # drop latencies of the malformed warm-up messages
        d.latencies.clear()

    t0 = time.perf_counter()
    publish(prod, t_in, events, rate=args.rate)
    while sum(d.processed for d in detectors) < len(events) and time.perf_counter() - t0 < 600:
        time.sleep(0.01)
    elapsed = time.perf_counter() - t0          # first publish -> last transaction scored
    for d in detectors:
        d.stop()
    for t in threads:
        t.join()

    alerted = {json.loads(v)["txn_id"] for v in drain(args.kafka, t_alert)}
    n_dlq = len(drain(args.kafka, t_dlq))

    lats = sorted(x for d in detectors for x in d.latencies)
    pct = {p: round(lats[min(int(len(lats) * p), len(lats) - 1)] * 1000, 1) for p in (0.5, 0.95, 0.99)}
    print(f"dead-lettered: {n_dlq}/3 malformed; end-to-end latency ms p50={pct[0.5]} p95={pct[0.95]} p99={pct[0.99]}")
    res = score(events, labels, alerted, elapsed, f"kafka x{args.replicas} replicas, {args.partitions} partitions, rate={args.rate or 'max'}")
    res.update({"dead_lettered": n_dlq, "e2e_latency_ms": pct})

    if args.check:
        # The pipeline must reproduce the offline rule engine exactly and dead-letter every bad message.
        eng, expected = RuleEngine(), set()
        for e in events:
            if eng.evaluate(e):
                expected.add(e["txn_id"])
        problems = []
        if alerted != expected:
            problems.append(f"alerts differ from offline: {len(alerted ^ expected)} mismatched txns")
        if n_dlq != 3:
            problems.append(f"expected 3 dead-lettered messages, got {n_dlq}")
        if problems:
            raise SystemExit("CHECK FAILED: " + "; ".join(problems))
        print("CHECK PASSED: Kafka alerts identical to offline, 3/3 malformed messages dead-lettered")
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--kafka")
    ap.add_argument("--cards", type=int, default=20_000)
    ap.add_argument("--normal", type=int, default=200_000)
    ap.add_argument("--incidents", type=int, default=1_500)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--partitions", type=int, default=6)
    ap.add_argument("--replicas", type=int, default=3)
    ap.add_argument("--rate", type=float, default=0.0)
    ap.add_argument("--check", action="store_true", help="fail unless Kafka results match offline exactly")
    a = ap.parse_args()
    kafka(a) if a.kafka else offline(a)
