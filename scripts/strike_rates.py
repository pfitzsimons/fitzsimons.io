#!/usr/bin/env python3
"""
Data-derived jockey & trainer strike-rates from the accumulating results.

Replaces the hand-maintained JOCKEY_RATINGS table (which only covered ~28% of
jockey-runs; the rest got a flat default) and adds a trainer signal that was
scraped but never used. Each name's win- and place-strike-rate is estimated
from every prior race in horses/history, Bayesian-shrunk toward the global base
rate so small samples and the long tail of rare names degrade gracefully to
"league average" instead of adding noise.

A 45-day out-of-sample walk-forward (see scripts/backtest_value.py --walk-forward)
showed this lifts the profitable Strong Win Bet tier from about +4% to +11–15%
ROI, robustly across shrinkage/weight settings and burn-in windows. The signal
is deliberately confined to a small weight so it re-ranks similarly-priced
horses without overriding the market.

The results feed only carries horse names, so jockey/trainer are recovered by
joining each results_full_<date>.json to that date's races_<date>.json
prediction archive (which carries horse -> jockey/trainer). Only races present
in BOTH, matched by course + off-time, contribute.

No leakage by construction: build_from_history() reads only dates strictly
before a cutoff (production passes today's date, so only finished races count),
and the backtest rolls the table forward one day at a time.
"""
import glob
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_results as fr

# Shrinkage pseudo-count and per-name sub-score weights, fixed a priori and
# confirmed stable out-of-sample (equal jockey/trainer split, moderate
# shrinkage). Kept here so the scorer and the backtest share one source.
ALPHA = 20.0
JOCKEY_WEIGHT = 0.075
TRAINER_WEIGHT = 0.075

# Fallback priors used only before any results have accumulated.
_DEFAULT_P0_WIN = 0.10
_DEFAULT_P0_PLACE = 0.33

_clamp = lambda x: max(0.0, min(100.0, x))


def _norm(name: str) -> str:
    # Reuse the scorer's jockey normaliser via a late import to avoid a cycle.
    import scrape_races as s
    return s._norm_jockey(name)


class StrikeTable:
    """Rolling win/place record per jockey and per trainer, with sub-scores."""

    def __init__(self, alpha: float = ALPHA):
        self.alpha = alpha
        # kind -> {norm_name: [runs, wins, places]}
        self.rec = {"jockey": {}, "trainer": {}}
        # kind -> [runs, wins, places]
        self.glob = {"jockey": [0, 0, 0], "trainer": [0, 0, 0]}

    # ── accumulation ────────────────────────────────────────────
    def add(self, kind: str, name: str, oc: dict) -> None:
        """Record one completed/failed run. Non-runners and unmatched are void."""
        if not oc or oc.get("status") == "non_runner":
            return
        k = _norm(name)
        if not k:
            return
        r = self.rec[kind].setdefault(k, [0, 0, 0])
        g = self.glob[kind]
        won = oc.get("status") == "finished" and oc.get("pos") == 1
        placed = bool(oc.get("placed"))
        r[0] += 1; g[0] += 1
        if won:
            r[1] += 1; g[1] += 1
        if placed:
            r[2] += 1; g[2] += 1

    def add_race(self, prace: dict) -> None:
        """Fold one race's joined outcomes (see join_race) into the table."""
        for run in prace.get("runners", []):
            oc = run.get("_oc")
            self.add("jockey", run.get("jockey", ""), oc)
            self.add("trainer", run.get("trainer", ""), oc)

    # ── scoring ─────────────────────────────────────────────────
    def sub(self, kind: str, name: str) -> float:
        """0–100 sub-score: league-average name → 50, twice-average → 100."""
        g = self.glob[kind]
        p0w = g[1] / g[0] if g[0] else _DEFAULT_P0_WIN
        p0p = g[2] / g[0] if g[0] else _DEFAULT_P0_PLACE
        runs, wins, places = self.rec[kind].get(_norm(name), (0, 0, 0))
        a = self.alpha
        win = (wins + a * p0w) / (runs + a)
        plc = (places + a * p0p) / (runs + a)
        sw = _clamp(100 * win / (2 * p0w)) if p0w else 50.0
        sp = _clamp(100 * plc / (2 * p0p)) if p0p else 50.0
        return 0.5 * sw + 0.5 * sp

    def jockey_sub(self, name: str) -> float:
        return self.sub("jockey", name)

    def trainer_sub(self, name: str) -> float:
        return self.sub("trainer", name)

    def is_populated(self) -> bool:
        return self.glob["jockey"][0] > 0

    # ── construction ────────────────────────────────────────────
    @classmethod
    def build_from_history(cls, hist_dir: str, before_date: str | None = None,
                           alpha: float = ALPHA) -> "StrikeTable":
        """Build a cumulative table from every archived date strictly before
        `before_date` (None = use all available history)."""
        t = cls(alpha=alpha)
        for date_str, praces in iter_history(hist_dir):
            if before_date and date_str >= before_date:
                continue
            for prace in praces:
                t.add_race(prace)
        return t


def join_race(prace: dict, result: dict) -> dict:
    """Attach an `_oc` outcome dict to each runner in `prace` from `result`.

    Outcome: {status: finished|dnf|non_runner, pos, placed} or None if the
    horse could not be matched in the full field.
    """
    ewp = prace.get("ew_places", 3)
    for run in prace.get("runners", []):
        run["_oc"] = _outcome(run.get("horse", ""), result, ewp)
    return prace


def _outcome(horse: str, result: dict, ewp: int):
    key = fr.normalise_name(horse)
    for r in result.get("runners", []):
        rn = fr.normalise_name(r.get("name", ""))
        if key == rn or key in rn or rn in key:
            st = r.get("status", "finished")
            if st != "finished":
                return {"status": st, "pos": None, "placed": False}
            pos = r.get("position")
            return {"status": "finished", "pos": pos,
                    "placed": bool(pos and pos <= ewp)}
    return None


def iter_history(hist_dir: str):
    """Yield (date_str, [praces_with_outcomes]) for every date that has both a
    prediction archive and a full-field results file, ordered by date."""
    days = {}
    for pf in glob.glob(os.path.join(hist_dir, "races_*.json")):
        try:
            pred = json.load(open(pf, encoding="utf-8"))
        except Exception:
            continue
        date_str = pred.get("date", "")
        rf = os.path.join(hist_dir, f"results_full_{date_str}.json")
        if not date_str or not os.path.exists(rf):
            continue
        try:
            results = json.load(open(rf, encoding="utf-8")).get("races", [])
        except Exception:
            continue
        joined = []
        for prace in pred.get("races", []):
            res = fr.match_race(prace, results)
            if res:
                joined.append(join_race(prace, res))
        if joined:
            days[date_str] = joined
    return sorted(days.items())
