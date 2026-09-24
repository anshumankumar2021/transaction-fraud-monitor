"""Synthetic card-transaction generator with labelled injected anomalies.

Normal traffic: each card has a home country and its own spending profile
(log-normal amounts). A small share of cards take a legitimate trip abroad
(with realistic gaps), which is the main source of hard negatives.

Injected incidents (ground truth), each with an incident_id:
  - amount_spike:      one purchase 6-20x the card's typical amount
  - velocity:          a burst of 7-12 purchases within ~40 seconds
  - impossible_travel: a purchase in a far-away country minutes after a home purchase
"""
from __future__ import annotations

import math
import random
import uuid
from dataclasses import dataclass

from app.rules import COUNTRY_CENTROIDS, haversine_km

COUNTRIES = list(COUNTRY_CENTROIDS)
MERCHANTS = ["grocery", "fuel", "restaurant", "online", "travel", "electronics", "pharmacy", "transit"]


@dataclass
class Card:
    card_id: str
    home: str
    mu: float      # log-normal location of amount
    sigma: float


def _txn(card: Card, ts: float, amount: float, country: str, merchant: str | None = None) -> dict:
    return {
        "txn_id": uuid.uuid4().hex,
        "card_id": card.card_id,
        "ts": round(ts, 3),
        "amount": round(amount, 2),
        "currency": "USD",
        "country": country,
        "merchant": merchant or random.choice(MERCHANTS),
    }


def generate(n_cards: int = 20_000, n_normal: int = 200_000, n_incidents: int = 1_500,
             days: float = 7.0, seed: int = 7):
    """Return (events sorted by event time, labels dict txn_id -> incident_id)."""
    random.seed(seed)
    span = days * 86_400
    cards = [Card(f"card_{i:06d}", random.choice(COUNTRIES), random.uniform(2.5, 4.5), random.uniform(0.3, 0.6))
             for i in range(n_cards)]
    events: list[dict] = []
    labels: dict[str, str] = {}

    # Legit travellers: 2% of cards spend abroad after a departure time.
    trips = {c.card_id: (random.uniform(0.2, 0.8) * span, random.choice([x for x in COUNTRIES if x != c.home]))
             for c in random.sample(cards, k=n_cards // 50)}

    for _ in range(n_normal):
        c = random.choice(cards)
        ts = random.uniform(0, span)
        country = c.home
        if c.card_id in trips:
            depart, dest = trips[c.card_id]
            # nobody swipes mid-flight: skip the flight window (cruise speed + 3h at airports)
            flight_s = (haversine_km(COUNTRY_CENTROIDS[c.home], COUNTRY_CENTROIDS[dest]) / 850 + 3) * 3600
            if depart <= ts < depart + flight_s:
                ts = depart - random.uniform(0, 3600)
            elif ts >= depart + flight_s:
                country = dest
        events.append(_txn(c, ts, random.lognormvariate(c.mu, c.sigma), country))

    kinds = ["amount_spike", "velocity", "impossible_travel"]
    for i in range(n_incidents):
        c = random.choice([x for x in random.sample(cards, 50) if x.card_id not in trips] or cards)
        kind = kinds[i % 3]
        inc = f"inc_{i:05d}_{kind}"
        ts = random.uniform(0.5 * span, span)  # after cards have built some history
        typical = math.exp(c.mu + c.sigma ** 2 / 2)  # the card's mean amount
        batch: list[dict] = []
        if kind == "amount_spike":
            batch.append(_txn(c, ts, typical * random.uniform(6, 20), c.home, "electronics"))
        elif kind == "velocity":
            for k in range(random.randint(7, 12)):
                batch.append(_txn(c, ts + k * random.uniform(2, 5), random.lognormvariate(c.mu, c.sigma), c.home, "online"))
        else:
            far = max((x for x in COUNTRIES if x != c.home),
                      key=lambda x: (COUNTRY_CENTROIDS[x][0] - COUNTRY_CENTROIDS[c.home][0]) ** 2
                      + (COUNTRY_CENTROIDS[x][1] - COUNTRY_CENTROIDS[c.home][1]) ** 2 + random.random())
            events.append(_txn(c, ts, random.lognormvariate(c.mu, c.sigma), c.home))  # genuine home purchase
            batch.append(_txn(c, ts + random.uniform(60, 1800), random.lognormvariate(c.mu, c.sigma), far))
        for t in batch:
            labels[t["txn_id"]] = inc
        events.extend(batch)

    events.sort(key=lambda e: e["ts"])
    return events, labels


def incident_of(labels: dict[str, str]) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for txn_id, inc in labels.items():
        out.setdefault(inc, set()).add(txn_id)
    return out


if __name__ == "__main__":
    ev, lab = generate(n_cards=100, n_normal=500, n_incidents=9)
    print(len(ev), "events,", len(set(lab.values())), "incidents; sample:", ev[0])
