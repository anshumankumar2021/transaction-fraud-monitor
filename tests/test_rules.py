from app.rules import RuleEngine, validate


def txn(i, ts, amount=50.0, country="US", card="c1"):
    return {"txn_id": f"t{i}", "card_id": card, "ts": ts, "amount": amount, "country": country, "merchant": "grocery"}


def rules(alerts):
    return {a.rule for a in alerts}


def test_normal_history_raises_nothing():
    eng = RuleEngine()
    for i in range(20):
        assert eng.evaluate(txn(i, i * 3600, 40 + i % 5)) == []


def test_amount_spike_after_history():
    eng = RuleEngine()
    for i in range(10):
        eng.evaluate(txn(i, i * 3600, 50 + (i % 3)))
    assert "amount_spike" in rules(eng.evaluate(txn(99, 20 * 3600, 900)))


def test_no_spike_without_enough_history():
    eng = RuleEngine()
    eng.evaluate(txn(0, 0, 50))
    assert eng.evaluate(txn(1, 3600, 900)) == []


def test_spike_is_not_absorbed_into_baseline():
    eng = RuleEngine()
    for i in range(10):
        eng.evaluate(txn(i, i * 3600, 50))
    eng.evaluate(txn(50, 11 * 3600, 1000))
    assert "amount_spike" in rules(eng.evaluate(txn(51, 12 * 3600, 1000)))


def test_velocity_burst():
    eng = RuleEngine()
    out = [eng.evaluate(txn(i, 1000 + i * 3)) for i in range(8)]
    assert all("velocity" not in rules(a) for a in out[:5])
    assert "velocity" in rules(out[5])


def test_velocity_window_slides():
    eng = RuleEngine()
    for i in range(20):  # one txn every 20s -> at most 4 in any 60s window
        assert "velocity" not in rules(eng.evaluate(txn(i, i * 20)))


def test_impossible_travel():
    eng = RuleEngine()
    eng.evaluate(txn(0, 0, country="US"))
    assert "impossible_travel" in rules(eng.evaluate(txn(1, 600, country="SG")))


def test_plausible_travel_is_allowed():
    eng = RuleEngine()
    eng.evaluate(txn(0, 0, country="US"))
    assert eng.evaluate(txn(1, 30 * 3600, country="GB")) == []


def test_state_is_per_card():
    eng = RuleEngine()
    eng.evaluate(txn(0, 0, country="US", card="a"))
    assert eng.evaluate(txn(1, 60, country="JP", card="b")) == []


def test_validate():
    good = txn(0, 1.0)
    assert validate(good) is None
    assert validate({**good, "amount": -1}) == "negative amount"
    assert validate({k: v for k, v in good.items() if k != "card_id"}) == "missing field card_id"
    assert validate({**good, "amount": "10"}) == "bad type for amount"
    assert validate([1, 2]) is not None


def test_flagged_location_does_not_poison_state():
    eng = RuleEngine()
    eng.evaluate(txn(0, 0, country="US"))
    assert "impossible_travel" in rules(eng.evaluate(txn(1, 600, country="SG")))
    assert eng.evaluate(txn(2, 1200, country="US")) == []
