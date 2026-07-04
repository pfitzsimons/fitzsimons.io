#!/usr/bin/env python3
"""
Walk-forward A/B for the distance-suitability factor (scrape_races.DIST_WEIGHT).

Proves — or rejects — the "proven at the trip" factor out-of-sample, holding
everything else at the shipped model (data-derived jockey/trainer strike-rates).
Two arms are scored on every out-of-sample race:

    off : DIST_WEIGHT = 0   (current production model)
    on  : DIST_WEIGHT = w   (form weight reduced by the same w)

CONSERVATIVE BY CONSTRUCTION. Production reads each horse's distance history
from Sporting Life's racecard JSON (horse.previous_results, ~6 recent runs),
which reaches back before our archive began. The archives, however, predate that
capture, so here each horse's prior-run distances are RECONSTRUCTED from our own
results — only runs inside the 60-day archive, rolled strictly forward (a horse's
run on day D contributes only to races on days > D). That is leak-free but sees
far fewer prior runs per horse than production will, so this understates the
factor's real power: if it helps here, it should help at least as much live.

    python3 scripts/backtest_distance.py                 # sweep default weights
    python3 scripts/backtest_distance.py --weight 0.08    # single weight + bootstrap
    python3 scripts/backtest_distance.py --burn-in 15
"""

import argparse
import copy
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scrape_races as s
import strike_rates as sr
import fetch_results as fr


# ─────────────────────────────────────────────────────────────
# Rolling per-horse distance history, reconstructed from our archive
# ─────────────────────────────────────────────────────────────

class HorseForm:
    """norm_horse -> list of {dist_f, pos}, accumulated across finished runs."""

    def __init__(self):
        self.rec = {}

    def prev(self, horse: str) -> list:
        return self.rec.get(fr.normalise_name(horse), [])

    def add_race(self, prace: dict) -> None:
        df = s.distance_to_furlongs(prace.get("distance"))
        if not df:
            return
        for run in prace.get("runners", []):
            oc = run.get("_oc")
            if not oc or oc.get("status") != "finished":
                continue
            pos = oc.get("pos")
            if isinstance(pos, int) and pos >= 1:
                self.rec.setdefault(fr.normalise_name(run.get("horse", "")), []) \
                    .append({"dist_f": round(df, 2), "pos": pos})


# ─────────────────────────────────────────────────────────────
# Scoring one race through the real production code path
# ─────────────────────────────────────────────────────────────

def score_and_recommend(prace, table, horseform, dist_weight):
    """Re-score a race with the shipped strike-rate model plus DIST_WEIGHT,
    injecting each runner's reconstructed prior-run distances. Returns runners
    (deep-copied), sorted best-first with fresh recommendations."""
    runners = [copy.deepcopy(r) for r in prace["runners"]]
    n = len(runners)

    s.STRIKE_TABLE = table
    sr.JOCKEY_WEIGHT, sr.TRAINER_WEIGHT = 0.075, 0.075
    s.DIST_WEIGHT = dist_weight

    for run in runners:
        run["_prev_runs"] = horseform.prev(run.get("horse", ""))
        res = s.score_runner(run, n, prace["going"], prace["distance"], prace["title"])
        run["_score"] = res["_score"]
        run["_components"] = res["_components"]
    s.normalise_weight_scores(runners)

    cf = s.COURSE_COEFFICIENTS.get(prace["course"], 1.0)
    if cf != 1.0:
        for run in runners:
            run["_score"] = max(0.0, min(100.0, run["_score"] * cf))

    runners.sort(key=lambda r: r["_score"], reverse=True)
    for run in runners:
        form = s.parse_form(run.get("form", ""))
        run["recommendation"] = s.make_recommendation(run["_score"], run.get("odds_dec"), n, form)
    s._post_process_win_bets(runners, n)
    return runners


def primary_win(runners):
    """The single top-scored Win pick, or None."""
    for r in runners:
        if r["recommendation"]["type"] == "Win":
            return r
    return None


def pnl_for(run, strong_only=False):
    """Flat £1 (stake, ret) for a Win pick, or None if unpriced/void."""
    oc = run.get("_oc")
    if not oc or oc["status"] == "non_runner":
        return None
    od = run.get("odds_dec")
    if not od or od <= 1:
        return None
    if strong_only and run["recommendation"].get("label") != "Strong Win Bet":
        return None
    res = "correct" if (oc["status"] == "finished" and oc["pos"] == 1) else "incorrect"
    return fr.bet_pnl("Win", res, od)


# ─────────────────────────────────────────────────────────────
# Walk-forward driver
# ─────────────────────────────────────────────────────────────

def walk_forward(days, burn_in, weights):
    """Return per_day[weight][tier] -> [(date, [(stake,ret),...]), ...]."""
    arms = {"off": 0.0}
    arms.update({f"{w:.3f}": w for w in weights})
    table = sr.StrikeTable()
    horseform = HorseForm()
    per_day = {a: {t: [] for t in ("win", "swin")} for a in arms}

    for i, (date, praces) in enumerate(days):
        if i >= burn_in:
            day = {a: {t: [] for t in ("win", "swin")} for a in arms}
            for prace in praces:
                for arm, w in arms.items():
                    runners = score_and_recommend(prace, table, horseform, w)
                    run = primary_win(runners)
                    if not run:
                        continue
                    p = pnl_for(run)
                    if p:
                        day[arm]["win"].append(p)
                    ps = pnl_for(run, strong_only=True)
                    if ps:
                        day[arm]["swin"].append(ps)
            for a in arms:
                for t in ("win", "swin"):
                    per_day[a][t].append((date, day[a][t]))
        # advance both rolling tables with the day's outcomes (after scoring)
        for prace in praces:
            table.add_race(prace)
            horseform.add_race(prace)
    return per_day, arms


# ─────────────────────────────────────────────────────────────
# Reporting
# ─────────────────────────────────────────────────────────────

def roi(pairs):
    st = sum(p[0] for p in pairs)
    rt = sum(p[1] for p in pairs)
    return ((rt - st) / st * 100 if st else 0.0), st


def flat(day_pairs):
    return [p for _, ps in day_pairs for p in ps]


def bootstrap(per_day, arm, tier, nboot=2000):
    """Resample whole days; (mean, lo, hi, p_gt0) for arm−off ROI delta."""
    paired = list(zip(per_day[arm][tier], per_day["off"][tier]))
    deltas = []
    for _ in range(nboot):
        samp = [random.choice(paired) for _ in paired]
        ab = [p for (_, a_), _ in samp for p in a_]
        ob = [p for _, (_, o_) in samp for p in o_]
        deltas.append(roi(ab)[0] - roi(ob)[0])
    deltas.sort()
    m = sum(deltas) / len(deltas)
    return m, deltas[int(0.025 * len(deltas))], deltas[int(0.975 * len(deltas))], \
        sum(1 for d in deltas if d > 0) / len(deltas)


def report(per_day, arms, do_bootstrap):
    print("\n============  DISTANCE-FACTOR WALK-FORWARD ROI (out-of-sample)  ============")
    print(f'{"arm (DIST_WEIGHT)":18} | {"Strong Win Bet":>20} | {"All Win":>20}')
    print("-" * 66)
    for arm in arms:
        s_roi, s_st = roi(flat(per_day[arm]["swin"]))
        w_roi, w_st = roi(flat(per_day[arm]["win"]))
        tag = "off" if arm == "off" else arm
        print(f"{tag:18} | {s_roi:+6.1f}%  n{int(s_st):<5}      "
              f"| {w_roi:+6.1f}%  n{int(w_st):<5}")

    if do_bootstrap:
        print("\n----  significance: arm − off ROI delta (bootstrap over days)  ----")
        for arm in arms:
            if arm == "off":
                continue
            for tier, name in (("swin", "Strong Win Bet"), ("win", "All Win")):
                m, lo, hi, p = bootstrap(per_day, arm, tier)
                verdict = "significant" if (lo > 0 or hi < 0) else "not significant"
                print(f"w={arm} {name:16} | delta {m:+5.1f}pp  95% CI [{lo:+.1f}, {hi:+.1f}]"
                      f"  P(on>off)={p:.2f}  ({verdict})")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="horses", help="Directory holding history/")
    ap.add_argument("--burn-in", type=int, default=15,
                    help="Warm-up days excluded from ROI (default 15)")
    ap.add_argument("--weight", type=float, default=None,
                    help="Single DIST_WEIGHT to test (default: sweep)")
    ap.add_argument("--bootstrap", action="store_true", default=None)
    ap.add_argument("--seed", type=int, default=1)
    args = ap.parse_args()
    random.seed(args.seed)

    weights = [args.weight] if args.weight is not None else [0.04, 0.06, 0.08, 0.10, 0.12]
    do_boot = args.bootstrap if args.bootstrap is not None else (args.weight is not None)

    hist = os.path.join(os.path.abspath(args.out), "history")
    days = sr.iter_history(hist)
    if not days:
        print("No joined history found (need races_*.json + results_full_*.json).",
              file=sys.stderr)
        return
    print(f"{len(days)} days ({days[0][0]}..{days[-1][0]}), "
          f"burn-in {args.burn_in} -> {len(days) - args.burn_in} out-of-sample days")

    per_day, arms = walk_forward(days, args.burn_in, weights)
    report(per_day, arms, do_boot)


if __name__ == "__main__":
    main()
