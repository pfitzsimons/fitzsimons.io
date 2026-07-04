#!/usr/bin/env python3
"""
Walk-forward A/B for an EXPERIENCE (run-count) shrinkage of the final score.

Motivation: in 2yo / novice / maiden races the ability signals the model leans
on (recent-form %, consistency %) are built from one or two runs, yet are
trusted as much as a 20-run horse's. A confident Win can rest on almost no
evidence. This tests pulling each runner's FINAL score toward neutral (50) by
how many runs back its form line, exactly the Bayesian shrink the model already
uses for distance / strike-rate:

    score' = (n * score + k * 50) / (n + k)

n = runs in the form line (leak-free: form is as-of the racecard), k = prior
strength in "phantom neutral runs". Higher k => harder shrink. A proven horse
(large n) barely moves; a 2-run juvenile is dragged toward a coin-flip, which
can push an over-confident favourite below the Win threshold.

Two arms are scored on every out-of-sample race through the SHIPPED code path
(data-derived strike-rates, all captured factors at their production weight):

    off      : no shrink                (current production model)
    k=<val>  : experience shrink at k

Reported on two universes: ALL races, and YOUNG races (every runner <= 2yo, i.e.
juvenile fields) where the effect concentrates. A flip analysis shows exactly
which Win picks the shrink dropped or changed in young races and what those
bets actually did — i.e. whether the caution saved or cost money.

    python3 scripts/backtest_experience.py                # sweep k
    python3 scripts/backtest_experience.py --k 4 --bootstrap
    python3 scripts/backtest_experience.py --burn-in 15
"""

import argparse
import copy
import os
import random
import re
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scrape_races as s
import strike_rates as sr
import fetch_results as fr

NEUTRAL = 50.0
DNF = {"P", "F", "U", "B"}


def num_runs(form_str: str) -> int:
    """Runs in the (most-recent-season) form line — matches parse_form's view."""
    if not form_str:
        return 0
    recent = form_str.split("/")[-1].replace("-", "")
    return sum(1 for ch in recent if ch.isdigit() or ch in DNF)


def is_young_race(prace: dict) -> bool:
    """Juvenile field: every runner with a known age is 2yo or younger."""
    ages = [int(r["age"]) for r in prace["runners"]
            if str(r.get("age", "")).strip().isdigit()]
    return bool(ages) and max(ages) <= 2


# ─────────────────────────────────────────────────────────────
# Scoring one race through the real production code path, then shrink
# ─────────────────────────────────────────────────────────────

def score_and_recommend(prace, table, k):
    """Re-score a race with the shipped model; if k is not None, apply the
    experience shrink to each runner's final score before re-ranking and
    re-recommending. Returns runners (deep-copied), best-first."""
    runners = [copy.deepcopy(r) for r in prace["runners"]]
    n = len(runners)

    s.STRIKE_TABLE = table
    sr.JOCKEY_WEIGHT, sr.TRAINER_WEIGHT = 0.075, 0.075

    for run in runners:
        res = s.score_runner(run, n, prace["going"], prace["distance"], prace["title"])
        run["_score"] = res["_score"]
        run["_components"] = res["_components"]
    s.normalise_weight_scores(runners)
    s.normalise_class_scores(runners)

    cf = s.COURSE_COEFFICIENTS.get(prace["course"], 1.0)
    if cf != 1.0:
        for run in runners:
            run["_score"] = max(0.0, min(100.0, run["_score"] * cf))

    if k is not None:
        for run in runners:
            nr = num_runs(run.get("form", ""))
            run["_score"] = (nr * run["_score"] + k * NEUTRAL) / (nr + k)

    runners.sort(key=lambda r: r["_score"], reverse=True)
    for run in runners:
        form = s.parse_form(run.get("form", ""))
        run["recommendation"] = s.make_recommendation(
            run["_score"], run.get("odds_dec"), n, form)
    s._post_process_win_bets(runners, n)
    return runners


def primary_win(runners):
    for r in runners:
        if r["recommendation"]["type"] == "Win":
            return r
    return None


def won(run) -> bool:
    oc = run.get("_oc")
    return bool(oc and oc.get("status") == "finished" and oc.get("pos") == 1)


def pnl_for(run):
    """Flat £1 (stake, ret) for a Win pick, or None if unpriced/void."""
    oc = run.get("_oc")
    if not oc or oc["status"] == "non_runner":
        return None
    od = run.get("odds_dec")
    if not od or od <= 1:
        return None
    res = "correct" if won(run) else "incorrect"
    return fr.bet_pnl("Win", res, od)


# ─────────────────────────────────────────────────────────────
# Walk-forward driver
# ─────────────────────────────────────────────────────────────

def walk_forward(days, burn_in, ks):
    arms = {"off": None}
    arms.update({f"k={k:g}": k for k in ks})
    universes = ("all", "young")
    table = sr.StrikeTable()
    per_day = {a: {u: [] for u in universes} for a in arms}
    flips = []  # (date, course, off_pick, off_win, new_pick_or_None, new_win)

    for i, (date_str, praces) in enumerate(days):
        if i >= burn_in:
            day = {a: {u: [] for u in universes} for a in arms}
            for prace in praces:
                young = is_young_race(prace)
                picks = {}
                for arm, k in arms.items():
                    runners = score_and_recommend(prace, table, k)
                    run = primary_win(runners)
                    picks[arm] = run
                    if run is None:
                        continue
                    p = pnl_for(run)
                    if p is None:
                        continue
                    day[arm]["all"].append(p)
                    if young:
                        day[arm]["young"].append(p)
                # flip log: young races where off vs the reference k differ
                if young and ks:
                    ref = f"k={ks[0]:g}"
                    off_p, new_p = picks["off"], picks[ref]
                    off_h = off_p["horse"] if off_p else None
                    new_h = new_p["horse"] if new_p else None
                    if off_h != new_h:
                        flips.append((
                            date_str, prace["course"],
                            off_h, won(off_p) if off_p else None,
                            new_h, won(new_p) if new_p else None,
                            off_p.get("odds_str") if off_p else None,
                            new_p.get("odds_str") if new_p else None))
            for a in arms:
                for u in universes:
                    per_day[a][u].append((date_str, day[a][u]))
        for prace in praces:
            table.add_race(prace)
    return per_day, arms, flips


# ─────────────────────────────────────────────────────────────
# Reporting
# ─────────────────────────────────────────────────────────────

def roi(pairs):
    st = sum(p[0] for p in pairs)
    rt = sum(p[1] for p in pairs)
    return ((rt - st) / st * 100 if st else 0.0), st


def hit(day_pairs):
    """(strike_rate_pct, n_bets) — a bet is a win when return > stake."""
    ps = [p for _, dp in day_pairs for p in dp]
    if not ps:
        return 0.0, 0
    wins = sum(1 for st, rt in ps if rt > st)
    return wins / len(ps) * 100, len(ps)


def flat(day_pairs):
    return [p for _, ps in day_pairs for p in ps]


def bootstrap(per_day, arm, universe, nboot=2000):
    paired = list(zip(per_day[arm][universe], per_day["off"][universe]))
    deltas = []
    for _ in range(nboot):
        samp = [random.choice(paired) for _ in paired]
        ab = [p for (_, a_), _ in samp for p in a_]
        ob = [p for _, (_, o_) in samp for p in o_]
        deltas.append(roi(ab)[0] - roi(ob)[0])
    deltas.sort()
    m = sum(deltas) / len(deltas)
    return (m, deltas[int(0.025 * len(deltas))], deltas[int(0.975 * len(deltas))],
            sum(1 for d in deltas if d > 0) / len(deltas))


def report(per_day, arms, flips, ks, do_bootstrap):
    print("\n===========  EXPERIENCE-SHRINK WALK-FORWARD (out-of-sample)  ===========")
    for universe, label in (("young", "YOUNG (2yo) races"), ("all", "ALL races")):
        print(f"\n-- {label} --")
        print(f'{"arm":10} | {"ROI":>18} | {"strike rate":>16}')
        print("-" * 52)
        for arm in arms:
            r, st = roi(flat(per_day[arm][universe]))
            srate, nb = hit(per_day[arm][universe])
            print(f"{arm:10} | {r:+6.1f}%  n{int(st):<5}  "
                  f"| {srate:5.1f}%  ({nb} bets)")

    if do_bootstrap:
        print("\n----  significance: arm − off ROI delta (bootstrap over days)  ----")
        for arm in arms:
            if arm == "off":
                continue
            for u, name in (("young", "YOUNG"), ("all", "ALL")):
                m, lo, hi, p = bootstrap(per_day, arm, u)
                verdict = "significant" if (lo > 0 or hi < 0) else "not significant"
                print(f"{arm:6} {name:6} | delta {m:+5.1f}pp "
                      f"95% CI [{lo:+.1f}, {hi:+.1f}]  P(on>off)={p:.2f}  ({verdict})")

    if ks:
        ref = f"k={ks[0]:g}"
        print(f"\n----  young-race flip log: off  →  {ref}  (bet the shrink changed)  ----")
        if not flips:
            print("  (none — shrink never changed the young-race pick)")
        dropped = changed = 0
        drop_saved = drop_missed = 0
        for dt, crs, oh, ow, nh, nw, oo, no in flips:
            if nh is None:
                dropped += 1
                if ow:
                    drop_missed += 1
                else:
                    drop_saved += 1
                res = "WON" if ow else "lost"
                print(f"  {dt} {crs:12} DROP  {oh!r} ({oo}) would have {res}")
            else:
                changed += 1
                ores = "WON" if ow else "lost"
                nres = "WON" if nw else "lost"
                print(f"  {dt} {crs:12} SWAP  {oh!r}({oo}) {ores}  ->  {nh!r}({no}) {nres}")
        print(f"\n  summary: {dropped} dropped ({drop_saved} losing bets avoided, "
              f"{drop_missed} winners missed), {changed} swapped "
              f"({len(flips)} young races changed)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="horses")
    ap.add_argument("--burn-in", type=int, default=15)
    ap.add_argument("--k", type=float, default=None, help="single k (default: sweep)")
    ap.add_argument("--bootstrap", action="store_true", default=None)
    ap.add_argument("--seed", type=int, default=1)
    args = ap.parse_args()
    random.seed(args.seed)

    ks = [args.k] if args.k is not None else [2.0, 3.0, 4.0, 6.0]
    do_boot = args.bootstrap if args.bootstrap is not None else (args.k is not None)

    hist = os.path.join(os.path.abspath(args.out), "history")
    days = sr.iter_history(hist)
    if not days:
        print("No joined history found (need races_*.json + results_full_*.json).",
              file=sys.stderr)
        return
    print(f"{len(days)} days ({days[0][0]}..{days[-1][0]}), burn-in {args.burn_in}"
          f" -> {len(days) - args.burn_in} out-of-sample days")

    per_day, arms, flips = walk_forward(days, args.burn_in, ks)
    report(per_day, arms, flips, ks, do_boot)


if __name__ == "__main__":
    main()
