from api.simulate import run


def test_demo_api_is_deterministic_and_consistent():
    a, b = run(123), run(123)
    assert a["metrics"]["caught"] == b["metrics"]["caught"]
    assert a["replay"]["rows"] == b["replay"]["rows"]

    m = a["metrics"]
    assert m["incidents"] == 300
    assert m["caught"] / m["incidents"] > 0.9
    assert 0 < m["precision"] <= 1

    cols = a["replay"]["columns"]
    rows = a["replay"]["rows"]
    assert len(rows) == 1500
    st, truth, alerts = cols.index("status"), cols.index("truth"), cols.index("alerts")
    for r in rows:
        # the outcome label must agree with the alerts and ground truth on that row
        if r[st] == "flagged":
            assert r[alerts] and r[truth]
        elif r[st] == "false_alarm":
            assert r[alerts] and not r[truth]
        elif r[st] in ("missed", "caught_later"):
            assert not r[alerts] and r[truth]
        else:
            assert not r[alerts] and not r[truth]
