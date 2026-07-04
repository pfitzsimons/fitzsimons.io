#!/usr/bin/env python3
"""
Simulates a day of intraday scrapes and asserts the two invariants Task 2
guarantees, without touching the network:

  1. NOTHING is lost from the live races.json as scrapes re-trigger — races
     drop off Sporting Life's live feed once they finish, and withdrawals
     appear mid-day, but every race and every within-day non-runner survives.
  2. The start-of-day archive (history/races_<date>.json) is written once by
     the first run and is BYTE-IDENTICAL after every later run — no intraday
     odds move, withdrawal, or re-score can ever leak into the graded record.

Run: python3 scripts/test_intraday_merge.py
"""

import json
import os
import sys
import tempfile
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scrape_races as s

DATE = "2026-07-10"


def _at(hh, mm):
    """A UK/IRE-local aware datetime on the test date."""
    return datetime(2026, 7, 10, hh, mm, tzinfo=s.UK_IRE_TZ)


def runner(name, odds=3.0, non_runner=False):
    r = {
        "horse": name,
        "odds_dec": odds,
        "odds_str": str(odds),
        "score": 50.0,
        "recommendation": {"type": "Skip", "label": "Skip"},
    }
    if non_runner:
        r["non_runner"] = True
        r["score"] = None
        r["recommendation"] = {"type": "Skip", "label": "Non-runner"}
    return r


def race(rid, time, runners):
    active = [r for r in runners if not r.get("non_runner")]
    return {
        "id": rid, "course": "Testbury", "time": time, "date": DATE,
        "title": f"Race {rid}", "num_runners": len(active), "runners": runners,
    }


def live_races(out_dir):
    with open(os.path.join(out_dir, "races.json"), encoding="utf-8") as f:
        return {r["id"]: r for r in json.load(f)["races"]}


def find(race_dict, horse):
    for r in race_dict.get("runners", []):
        if r["horse"] == horse:
            return r
    return None


def main():
    with tempfile.TemporaryDirectory() as out:
        # ── Scrape A — start of day (12:00). Full card, all upcoming. ──
        a_r1 = race("r1", "13:00", [runner("Alpha"), runner("Bravo")])
        a_r2 = race("r2", "15:00", [runner("Cara"), runner("Delta"), runner("Echo")])
        a_r3 = race("r3", "18:00", [runner("Foxtrot"), runner("Golf")])
        s.persist_scrape(out, DATE, [a_r1, a_r2, a_r3], now_local=_at(12, 0))

        archive_path = os.path.join(out, "history", f"races_{DATE}.json")
        assert os.path.exists(archive_path), "start-of-day archive not written"
        archive_bytes = open(archive_path, "rb").read()
        r1_at_a = live_races(out)["r1"]

        # ── Scrape B — 14:30. R1 has started (frozen). A withdrawal appears
        #    in R2 (Delta → non-runner). R3 odds drift. ──
        b_r1 = race("r1", "13:00", [runner("Alpha", 1.5), runner("Bravo", 9.0)])  # post-off junk
        b_r2 = race("r2", "15:00",
                    [runner("Cara"), runner("Delta", non_runner=True), runner("Echo")])
        b_r3 = race("r3", "18:00", [runner("Foxtrot", 2.2), runner("Golf", 4.0)])
        s.persist_scrape(out, DATE, [b_r1, b_r2, b_r3], now_local=_at(14, 30))

        L = live_races(out)
        assert L["r1"] == r1_at_a, "started race R1 was not frozen (post-off data leaked)"
        assert find(L["r2"], "Delta")["non_runner"] is True, "withdrawal not recorded in R2"
        assert find(L["r3"], "Foxtrot")["odds_dec"] == 2.2, "upcoming R3 odds not refreshed"

        # ── Scrape B2 — 14:45. R2 still upcoming, but this partial scrape
        #    OMITS the withdrawn Delta entirely. Sticky non-runner must survive. ──
        b2_r2 = race("r2", "15:00", [runner("Cara"), runner("Echo")])  # Delta dropped
        s.persist_scrape(out, DATE, [b2_r2], now_local=_at(14, 45))

        L = live_races(out)
        delta = find(L["r2"], "Delta")
        assert delta is not None and delta["non_runner"] is True, \
            "sticky non-runner Delta lost when a later partial scrape omitted it"
        assert {"r1", "r2", "r3"} <= set(L), "a race was dropped by the partial scrape"

        # ── Scrape C — 16:00. R1 & R2 have finished and fallen off the feed;
        #    only R3 comes back. Union must keep the finished races. ──
        c_r3 = race("r3", "18:00", [runner("Foxtrot", 2.0), runner("Golf", 5.0)])
        s.persist_scrape(out, DATE, [c_r3], now_local=_at(16, 0))

        L = live_races(out)
        assert {"r1", "r2", "r3"} <= set(L), "finished races were dropped from the live file"
        assert find(L["r2"], "Delta")["non_runner"] is True, "frozen R2 lost its withdrawal"
        assert L["r1"] == r1_at_a, "finished R1 changed after falling off the feed"

        # ── Scrape D — 19:00. Empty scrape (partial failure). Must not delete. ──
        s.persist_scrape(out, DATE, [], now_local=_at(19, 0))
        L = live_races(out)
        assert {"r1", "r2", "r3"} <= set(L), "empty scrape deleted live data"
        assert find(L["r2"], "Delta")["non_runner"] is True, "empty scrape lost the withdrawal"

        # ── Archive invariant: untouched by every run after the first. ──
        assert open(archive_path, "rb").read() == archive_bytes, \
            "start-of-day archive was modified by a later run"
        arch = json.loads(archive_bytes)
        arch_r2 = next(r for r in arch["races"] if r["id"] == "r2")
        assert find(arch_r2, "Delta").get("non_runner") is not True, \
            "later withdrawal leaked into the start-of-day accuracy record"
        assert len(arch_r2["runners"]) == 3, "archive R2 field size changed"

    print("PASS — intraday merge keeps all live data and freezes the start-of-day archive")


if __name__ == "__main__":
    main()
