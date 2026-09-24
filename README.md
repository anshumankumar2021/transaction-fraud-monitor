# Real-Time Transaction Stream Monitor

[![ci](https://github.com/anshumankumar2021/transaction-fraud-monitor/actions/workflows/ci.yml/badge.svg)](https://github.com/anshumankumar2021/transaction-fraud-monitor/actions/workflows/ci.yml)

A Kafka streaming pipeline that watches card transactions as they happen and
flags suspicious activity within milliseconds: spending spikes, bursts of
rapid purchases, and "impossible travel" between countries. It runs as
containerized services on Docker Compose or Kubernetes, with Prometheus
metrics, alert rules, and a Grafana dashboard.

```
 producer ──► [transactions] ──► detector (consumer group, N replicas) ──► [alerts]
 (keyed by card_id,               │  per-card state, event-time rules          
  6 partitions)                   ├──► [transactions.dlq]  malformed messages
                                  └──► /metrics ──► Prometheus ──► Grafana + alert rules
```

## Design

| Concern | Choice |
|---|---|
| Ordering | Messages are keyed by `card_id`, so each card's events stay on one partition and in order. That lets each detector replica keep per-card state in memory safely. |
| Scaling | Detectors form one consumer group (cooperative-sticky rebalancing). Add replicas up to the partition count; the Kubernetes HPA scales 2–6 on CPU. |
| Delivery | At-least-once: offsets are committed only after a batch's alerts are flushed. Producers use idempotence and `acks=all`. |
| Bad data | Schema validation runs on every message. Invalid messages go to `transactions.dlq` with the error in a header instead of blocking the partition. |
| Determinism | Rules use event time, not wall-clock time, so replaying a topic reproduces the same alerts. |
| Security | Containers run as non-root with a read-only root filesystem and no privilege escalation. |

### Detection rules (`app/rules.py`)

- **Velocity**: a card makes more than 5 transactions within 60 seconds.
- **Amount spike**: the purchase is more than 3σ above the card's usual spending (measured in log space, since card spending is roughly log-normal) *and* more than 4× its typical amount. Flagged amounts are kept out of the baseline so fraud can't shift it.
- **Impossible travel**: consecutive purchases imply travel faster than 900 km/h. A flagged location isn't trusted as the card's new position, so the cardholder's next purchase at home isn't flagged as a "return trip".

## Results

Measured on synthetic data: 20,000 cards, about 206K transactions over one
simulated week, and 1,500 injected fraud incidents (one-third of each type).
Legitimate international trips are included as hard negatives.
Reproduce with `python -m scripts.evaluate` (offline) or
`python -m scripts.evaluate --kafka localhost:29092` (end to end through Kafka).

| Metric | Result |
|---|---|
| Fraud incidents caught | **96.9%** (velocity 100%, impossible travel 100%, amount spike 90.6%) |
| Alert precision | **92.6%** (false-positive rate 0.13% of legitimate transactions) |
| Throughput through Kafka | **~49,000 msgs/s** with 3 detector replicas and 6 partitions |
| End-to-end latency at a steady 5,000 msgs/s | p50 46 ms, **p95 61 ms**, p99 65 ms |
| Rule evaluation cost | about 2 µs per transaction |
| Malformed messages | 3/3 routed to the dead-letter topic, and the stream kept flowing |
| Kafka vs. offline | identical alerts, because rules run on event time and per-card ordering is preserved |

Benchmark setup: a single 2-vCPU VM running the broker, producer and all
detector replicas together, against [Tansu](https://github.com/tansu-io/tansu),
a Kafka-protocol-compatible broker. Docker Compose and Kubernetes use Apache
Kafka 3.7, so expect different absolute numbers on your hardware.

Most amount-spike misses are cold starts: the card had fewer than 4 prior
purchases, so there was no baseline to compare against yet.

### Known limitations

- Per-card state lives in memory. If a partition moves to another replica
  during a rebalance, that card's history is lost and rebuilds from new
  traffic. A production version would back the state with a compacted
  changelog topic (the Kafka Streams / Flink approach) or an external store.
- The rules are hand-tuned thresholds on synthetic data. The natural next step
  is to feed alert outcomes back as labels and train a model.

## Run it

```bash
docker compose up --build          # Kafka, 3 detectors, producer, Prometheus, Grafana
open http://localhost:3000         # "Transaction Stream Monitor" dashboard
```

On Kubernetes (kind or minikube):

```bash
docker build -t txn-stream-monitor:latest .
kind load docker-image txn-stream-monitor:latest
kubectl apply -f k8s/
kubectl -n txn-monitor get pods,hpa
```

Tests and evaluation:

```bash
pip install -r requirements-dev.txt
pytest -q
python -m scripts.evaluate
```

CI (GitHub Actions) runs the unit tests and offline evaluation, builds the
image, and runs the end-to-end evaluation against a real Apache Kafka service
container with `--check`. That check fails the build unless the Kafka pipeline's
alerts match the offline rule engine transaction for transaction, and every
malformed message lands in the dead-letter topic.

## Layout

```
app/rules.py        detection rules and schema validation (pure Python, unit tested)
app/detector.py     Kafka consumer group, alert producer, DLQ, Prometheus metrics
app/producer.py     synthetic transaction publisher (idempotent, keyed by card)
app/simulate.py     traffic generator with labelled fraud incidents
scripts/evaluate.py recall/precision/throughput, offline or through Kafka
k8s/                namespace, KRaft Kafka StatefulSet, detector Deployment + HPA
monitoring/         Prometheus scrape config and alert rules, Grafana dashboard
```
