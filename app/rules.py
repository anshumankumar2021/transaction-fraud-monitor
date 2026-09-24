"""Stateful fraud / anomaly rules evaluated per card.

All state is keyed by card_id. Because the producer keys every message by
card_id, all events for one card land on the same Kafka partition, so each
detector replica in the consumer group owns a disjoint set of cards and this
in-memory state stays consistent when the group scales out.

Rules use *event time* (the transaction timestamp), not wall-clock time, so
results are deterministic and replaying a topic gives the same alerts.
"""
from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import dataclass, field

# Approximate country centroids (lat, lon) used for impossible-travel checks.
COUNTRY_CENTROIDS: dict[str, tuple[float, float]] = {
    "US": (39.8, -98.6), "CA": (56.1, -106.3), "MX": (23.6, -102.6),
    "BR": (-14.2, -51.9), "GB": (55.4, -3.4), "FR": (46.2, 2.2),
    "DE": (51.2, 10.5), "IN": (20.6, 79.0), "SG": (1.35, 103.8),
    "JP": (36.2, 138.3), "AU": (-25.3, 133.8), "AE": (23.4, 53.8),
    "NG": (9.1, 8.7), "ZA": (-30.6, 22.9),
}


def haversine_km(a: tuple[float, float], b: tuple[float, float]) -> float:
    lat1, lon1 = map(math.radians, a)
    lat2, lon2 = map(math.radians, b)
    h = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 2 * 6371.0 * math.asin(math.sqrt(h))


@dataclass
class RuleConfig:
    velocity_window_s: float = 60.0
    velocity_max_txns: int = 5            # alert when a card exceeds this many txns in the window
    spike_min_history: int = 4            # need this many prior txns before judging amounts
    spike_z: float = 3.0                  # log(amount) must be this many std devs above the card's norm ...
    spike_ratio: float = 4.0              # ... and the amount at least this multiple of the card's median
    travel_max_kmh: float = 900.0         # faster than a commercial flight = impossible travel


@dataclass
class _CardState:
    # Welford running stats over log(amount): card spend is roughly log-normal,
    # so z-scores in log space give far fewer false alarms than raw amounts.
    n: int = 0
    mean: float = 0.0
    m2: float = 0.0
    recent: deque = field(default_factory=deque)   # event timestamps inside the velocity window
    last_country: str | None = None
    last_ts: float | None = None

    @property
    def std(self) -> float:
        return math.sqrt(self.m2 / (self.n - 1)) if self.n > 1 else 0.0

    def add_amount(self, x: float) -> None:
        self.n += 1
        d = x - self.mean
        self.mean += d / self.n
        self.m2 += d * (x - self.mean)


@dataclass
class Alert:
    txn_id: str
    card_id: str
    rule: str
    detail: str
    event_ts: float

    def to_dict(self) -> dict:
        return self.__dict__.copy()


class RuleEngine:
    def __init__(self, cfg: RuleConfig | None = None):
        self.cfg = cfg or RuleConfig()
        self._cards: dict[str, _CardState] = defaultdict(_CardState)

    @property
    def tracked_cards(self) -> int:
        return len(self._cards)

    def evaluate(self, txn: dict) -> list[Alert]:
        """Score one transaction, update card state, and return any alerts."""
        cfg = self.cfg
        card, ts, amount, country = txn["card_id"], float(txn["ts"]), float(txn["amount"]), txn["country"]
        st = self._cards[card]
        alerts: list[Alert] = []

        # 1) Velocity: too many transactions in a sliding event-time window.
        st.recent.append(ts)
        while st.recent and ts - st.recent[0] > cfg.velocity_window_s:
            st.recent.popleft()
        if len(st.recent) > cfg.velocity_max_txns:
            alerts.append(Alert(txn["txn_id"], card, "velocity",
                                f"{len(st.recent)} txns in {cfg.velocity_window_s:.0f}s", ts))

        # 2) Amount spike relative to this card's own history (log space).
        spiked = False
        log_amt = math.log(max(amount, 0.01))
        if st.n >= cfg.spike_min_history:
            z = (log_amt - st.mean) / max(st.std, 0.05)
            ratio = amount / math.exp(st.mean)
            if z > cfg.spike_z and ratio > cfg.spike_ratio:
                spiked = True
                alerts.append(Alert(txn["txn_id"], card, "amount_spike",
                                    f"{amount:.2f} is {ratio:.1f}x typical (z={z:.1f})", ts))
        if not spiked:  # keep outliers out of the baseline so fraud can't shift it
            st.add_amount(log_amt)

        # 3) Impossible travel between consecutive transactions.
        teleported = False
        if st.last_country and country != st.last_country and st.last_ts is not None:
            a, b = COUNTRY_CENTROIDS.get(st.last_country), COUNTRY_CENTROIDS.get(country)
            if a and b:
                hours = max(ts - st.last_ts, 1.0) / 3600.0
                kmh = haversine_km(a, b) / hours
                if kmh > cfg.travel_max_kmh:
                    teleported = True
                    alerts.append(Alert(txn["txn_id"], card, "impossible_travel",
                                        f"{st.last_country}->{country} at {kmh:,.0f} km/h", ts))
        # A flagged location is untrusted: keep the last good location so the
        # cardholder's next genuine purchase at home isn't flagged as a "return trip".
        if not teleported:
            st.last_country, st.last_ts = country, ts
        return alerts


REQUIRED_FIELDS = {"txn_id": str, "card_id": str, "ts": (int, float), "amount": (int, float),
                   "country": str, "merchant": str}


def validate(txn: object) -> str | None:
    """Return an error string if the message is malformed, else None."""
    if not isinstance(txn, dict):
        return "payload is not a JSON object"
    for k, t in REQUIRED_FIELDS.items():
        if k not in txn:
            return f"missing field {k}"
        if not isinstance(txn[k], t) or isinstance(txn[k], bool):
            return f"bad type for {k}"
    if txn["amount"] < 0:
        return "negative amount"
    return None
