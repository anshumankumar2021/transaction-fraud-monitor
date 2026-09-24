"""Vercel serverless endpoint: run the real rule engine over a fresh simulated stream.

GET /api/simulate?seed=123

Uses the same app/simulate.py and app/rules.py as the Kafka detector, minus
Kafka itself (Vercel functions are short-lived and can't host a broker or a
long-running consumer group). Returns scored metrics for the whole stream
plus a replay window of transactions and alerts for the dashboard to animate.
"""
from __future__ import annotations

import json
import os
import sys
import time
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.rules import RuleEngine  # noqa: E402
from app.simulate import generate, incident_of  # noqa: E402

CARDS, NORMAL, INCIDENTS, DAYS = 5_000, 50_000, 300, 7.0
REPLAY_START, REPLAY_LEN = 0.62, 1_500   # replay window: fraction of the week, number of events


def run(seed: int) -> dict:
    t0 = time.perf_counter()
    events, labels = generate(CARDS, NORMAL, INCIDENTS, days=DAYS, seed=seed)
    gen_s = time.perf_counter() - t0

    eng = RuleEngine()
    alerts_by_txn: dict[str, list] = {}
    lat = []
    t1 = time.perf_counter()
    for e in events:
        s = time.perf_counter()
        out = eng.evaluate(e)
        lat.append(time.perf_counter() - s)
        if out:
            alerts_by_txn[e["txn_id"]] = [[a.rule, a.detail] for a in out]
    eval_s = time.perf_counter() - t1
    lat.sort()

    alerted = set(alerts_by_txn)
    incidents = incident_of(labels)
    caught = {i for i, ids in incidents.items() if ids & alerted}
    kinds = ("velocity", "amount_spike", "impossible_travel")
    by_type = {}
    for k in kinds:
        tot = [i for i in incidents if i.endswith(k)]
        by_type[k] = {"caught": sum(i in caught for i in tot), "total": len(tot)}
    tp = len(alerted & labels.keys())
    fp = len(alerted) - tp

    span = DAYS * 86_400
    start = next(i for i, e in enumerate(events) if e["ts"] >= REPLAY_START * span)
    window = events[start:start + REPLAY_LEN]
    # incident ids look like inc_00012_amount_spike -> label each fraud txn with its kind
    kind_of = {txn: next(k for k in kinds if inc.endswith(k)) for txn, inc in labels.items()}

    def status(txn: str) -> str:
        fraud, flagged = txn in labels, txn in alerted
        if flagged:
            return "flagged" if fraud else "false_alarm"
        if fraud:  # e.g. the first few purchases of a burst, before the velocity threshold trips
            return "caught_later" if labels[txn] in caught else "missed"
        return "ok"

    replay = [[e["ts"], e["card_id"][-6:], e["amount"], e["country"], e["merchant"],
               kind_of.get(e["txn_id"], ""), alerts_by_txn.get(e["txn_id"], []), status(e["txn_id"])]
              for e in window]

    return {
        "seed": seed,
        "config": {"cards": CARDS, "days": DAYS},
        "metrics": {
            "events": len(events),
            "incidents": len(incidents),
            "caught": len(caught),
            "by_type": by_type,
            "alerted_txns": len(alerted),
            "precision": round(tp / len(alerted), 4) if alerted else None,
            "false_positive_rate": round(fp / (len(events) - len(labels)), 6),
            "eval_p50_us": round(lat[len(lat) // 2] * 1e6, 1),
            "eval_p99_us": round(lat[int(len(lat) * 0.99)] * 1e6, 1),
            "engine_events_per_s": round(len(events) / eval_s),
            "generate_s": round(gen_s, 3),
            "evaluate_s": round(eval_s, 3),
        },
        "replay": {"columns": ["ts", "card", "amount", "country", "merchant", "truth", "alerts", "status"],
                   "rows": replay},
    }


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        qs = parse_qs(urlparse(self.path).query)
        try:
            seed = int(qs.get("seed", ["7"])[0]) % 1_000_000
        except ValueError:
            seed = 7
        body = json.dumps(run(seed), separators=(",", ":")).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "public, s-maxage=86400, max-age=300")  # same seed -> same result
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
