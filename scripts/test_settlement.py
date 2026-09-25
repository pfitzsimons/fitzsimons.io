#!/usr/bin/env python3
"""
Guards the accuracy/ROI settlement rules, without touching the network:

  1. Bets settle at a BETTABLE price — the best bookmaker price from the
     start-of-day scrape, else SP — never the overnight forecast (odds_dec),
     which runs long on winners. Same rule on the site and in the backtests.
  2. Results match a horse by exact name before substring.
  3. The favourite baseline backs the morning favourite (ignoring horses
     already withdrawn at the morning scrape) only in races we bet.
  4. accuracy.json is fully derived from history/: regrading is
     deterministic, repairs a corrupted/conflicted file, trims race detail
     to the last DETAIL_DAYS, and the daily run produces the same file.
  5. The model-vs-market readout uses the bookmaker price only when the
     whole race has one.

Run: python3 scripts/test_settlement.py
"""

import json
import os
import sys
import tempfile
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_results as fr
import strike_rates as sr
import backtest_value as bv
import scrape_races as s


def pred_runner(name, forecast, book=None, rec="Skip", label="", non_runner=False):
    r = {"horse": name, "odds_dec": forecast, "odds_str": str(forecast),
         "best_odds_dec": book, "score": 50.0,
         "recommendation": {"type": rec, "label": label}}
    if non_runner:
        r["non_runner"] = True
    return r


def res_runner(name, pos, sp, status="finished"):
    return {"name": name, "position": pos, "status": status, "casualty": "",
            "odds": sp, "favourite": False}


def race(course, time, runners):
    return {"course": course, "time": time, "title": "Test Stakes", "runners": runners}


def result(course, time, runners):
    return {"course": course, "time": time, "race_name": "Test Stakes", "runners": runners}


def test_prices():
    assert fr.parse_sp("11/8") == 2.38
    assert fr.parse_sp("EVS") == fr.parse_sp("Evs") == fr.parse_sp("1/1") == 2.0
    assert fr.parse_sp("4.5") == 4.5
    for junk in ("", None, "SP", "-", "1.0"):
        assert fr.parse_sp(junk) is None, junk

    # Book price wins; SP is the fallback; the forecast is never used.
    assert fr.settle_price({"odds_dec": 9.0, "best_odds_dec": 3.0}, {"sp": 2.5}) == (3.0, "book")
    assert fr.settle_price({"odds_dec": 9.0, "best_odds_dec": None}, {"sp": 2.5}) == (2.5, "sp")
    assert fr.settle_price({"odds_dec": 9.0}, {}) == (None, None)


def test_matching():
    res = result("X", "14:00", [res_runner("Sea Legend", 1, "2/1"), res_runner("Sea", 5, "10/1")])
    assert fr.match_runner("Sea", res)["name"] == "Sea", "substring beat an exact match"
    assert fr.match_runner("Sea Legend", res)["name"] == "Sea Legend"
    # Archived names can carry HTML entities the scraper left unescaped.
    assert fr.match_runner("D&#39;Alboni", result("X", "14:00", [res_runner("D'Alboni", 1, "2/1")]))
    assert fr.match_runner("Nobody", res) is None

    oc = sr._outcome("Sea", res, 3)
    assert oc["pos"] == 5 and oc["sp"] == 11.0, oc


def test_day_report():
    preds = {"date": "2026-08-01", "races": [
        # Our Win pick wins. Forecast 5.0 but the best book price was 3.0.
        # Morning favourite "Short" (forecast 2.0) loses.
        race("Ascot", "14:00", [
            pred_runner("Pick", 5.0, book=3.0, rec="Win", label="Strong Win Bet"),
            pred_runner("Short", 2.0, book=2.2),
        ]),
        # Pick has no book price, so it settles at SP (4/1 = 5.0) and loses.
        # "Gone" is the shortest forecast but was withdrawn in the morning,
        # so the favourite baseline backs "Fav" instead, and Fav wins at SP.
        race("Ascot", "15:00", [
            pred_runner("Pick Two", 6.0, rec="Win", label="Strong Win Bet"),
            pred_runner("Gone", 1.5, non_runner=True),
            pred_runner("Fav", 2.5),
        ]),
        # No bet in this race, so no favourite bet either.
        race("Ascot", "16:00", [pred_runner("Solo", 3.0), pred_runner("Other", 4.0)]),
        # Our pick is withdrawn after the morning scrape: void, not a loss.
        race("Ascot", "17:00", [
            pred_runner("Late NR", 3.0, book=3.2, rec="Win", label="Strong Win Bet"),
            pred_runner("Rival", 2.0, book=2.1),
        ]),
    ]}
    results = [
        result("Ascot", "14:00", [res_runner("Pick", 1, "7/2"), res_runner("Short", 2, "6/4")]),
        result("Ascot", "15:00", [res_runner("Fav", 1, "6/4"), res_runner("Pick Two", 2, "4/1"),
                                  res_runner("Gone", 0, "", status="non_runner")]),
        result("Ascot", "16:00", [res_runner("Solo", 1, "2/1"), res_runner("Other", 2, "3/1")]),
        result("Ascot", "17:00", [res_runner("Rival", 1, "Evs"),
                                  res_runner("Late NR", 0, "", status="non_runner")]),
    ]
    s_ = fr.compare_predictions_to_results(preds, results)["summary"]

    assert (s_["win_correct"], s_["win_total"]) == (1, 2), s_
    assert s_["excluded"]["non_runner"] == 1, "late non-runner pick wasn't voided"
    assert s_["roi"]["win_stake"] == 2.0
    assert s_["roi"]["win_ret"] == 3.0, "winner not settled at the best book price"
    assert s_["settle_basis"] == {"book": 1, "sp": 1}, s_["settle_basis"]
    assert s_["roi_forecast"] == {"stake": 2.0, "ret": 5.0}, s_["roi_forecast"]
    # Favourite baseline only where we actually had a bet: 14:00 Short loses,
    # 15:00 Fav wins at SP 2.5. None at 16:00 (no pick) or 17:00 (our pick
    # was void), so the comparison stays like-for-like.
    assert s_["fav"] == {"stake": 2.0, "ret": 2.5, "wins": 1}, s_["fav"]


def test_backtest_settlement():
    res = result("X", "14:00", [res_runner("A", 1, "4/1"), res_runner("B", 2, "1/1")])
    run = pred_runner("A", 9.0, book=4.5, rec="Win", label="Strong Win Bet")
    run["_oc"] = sr._outcome("A", res, 3)
    assert bv.pnl_for("Win", run) == (1.0, 4.5), "backtest settled at the forecast"
    run["best_odds_dec"] = None
    assert bv.pnl_for("Win", run) == (1.0, 5.0), "backtest didn't fall back to SP"


def _write_history(hist, days):
    """`days` synthetic days, each with one winning Strong Win Bet."""
    first = date(2026, 6, 1)
    for i in range(days):
        d = (first + timedelta(days=i)).isoformat()
        preds = {"date": d, "races": [race("York", "14:00", [
            pred_runner("Winner", 4.0, book=3.5, rec="Win", label="Strong Win Bet"),
            pred_runner("Loser", 2.0, book=2.1)])]}
        res = {"date": d, "races": [result("York", "14:00", [
            res_runner("Winner", 1, "5/2"), res_runner("Loser", 2, "Evs")])]}
        json.dump(preds, open(os.path.join(hist, f"races_{d}.json"), "w"))
        json.dump(res, open(os.path.join(hist, f"results_full_{d}.json"), "w"))
    return (first + timedelta(days=days - 1)).isoformat()


def test_derived_log():
    with tempfile.TemporaryDirectory() as out:
        hist = os.path.join(out, "history")
        os.makedirs(hist)
        last = _write_history(hist, fr.DETAIL_DAYS + 5)
        acc = os.path.join(out, "accuracy.json")

        # A merge conflict left the file unparseable. Regrading must not care.
        open(acc, "w").write("<<<<<<< HEAD\n[]\n=======\n{}\n>>>>>>> main\n")
        fr.rebuild_accuracy_log(out)
        first_bytes = open(acc, "rb").read()
        log = json.loads(first_bytes)
        assert len(log) == fr.DETAIL_DAYS + 5, "not every day was regraded"
        assert [e["date"] for e in log] == sorted(e["date"] for e in log)
        assert all(not e["races"] for e in log[:5]), "old days kept race detail"
        assert all(e["races"] for e in log[5:]), "recent days lost race detail"
        assert all(e["summary"]["roi"]["win_ret"] == 3.5 for e in log)

        fr.rebuild_accuracy_log(out)
        assert open(acc, "rb").read() == first_bytes, "regrading isn't deterministic"

        # The daily run (Sporting Life stubbed) must land on the same file,
        # even starting from an empty log.
        saved = json.load(open(os.path.join(hist, f"results_full_{last}.json")))["races"]
        os.remove(os.path.join(hist, f"results_full_{last}.json"))
        open(acc, "w").write("[]")
        real_fetch, real_argv = fr.fetch_sl_results, sys.argv
        fr.fetch_sl_results = lambda d: saved
        sys.argv = ["fetch_results.py", "--out", out, "--results-date", last]
        try:
            fr.main()
        finally:
            fr.fetch_sl_results, sys.argv = real_fetch, real_argv
        assert open(acc, "rb").read() == first_bytes, "daily run differs from --rebuild"


def test_market_readout():
    def field(*books):
        return [{"_score": 60, "odds_dec": 3.0, "best_odds_dec": b} for b in books]

    rs = field(2.0, 4.0, 4.0)
    s.compute_value(rs)
    assert all(r["_market_basis"] == "book" for r in rs)
    assert abs(rs[0]["_market_prob"] - 0.5) < 1e-9, "market prob not from book prices"
    assert abs(sum(r["_market_prob"] for r in rs) - 1) < 1e-9

    rs = field(2.0, None, 4.0)  # one runner unpriced: whole race uses the forecast
    s.compute_value(rs)
    assert all(r["_market_basis"] == "forecast" for r in rs)
    assert all(abs(r["_market_prob"] - 1 / 3) < 1e-9 for r in rs)


def main():
    tests = [test_prices, test_matching, test_day_report, test_backtest_settlement,
             test_derived_log, test_market_readout]
    # fetch_results logs progress to stderr; keep the test output clean.
    real_log = fr.log
    fr.log = lambda msg: None
    try:
        for t in tests:
            t()
    finally:
        fr.log = real_log
    print(f"PASS — {len(tests)} settlement checks: bettable prices, exact-name matching, "
          f"favourite baseline, derived accuracy log, market readout")


if __name__ == "__main__":
    main()
