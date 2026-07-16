#!/usr/bin/env python3
"""
Walk-forward ROI backtest — proves (or rejects) scoring changes out-of-sample.

The 60-day archive is both training and test data, so a single in-sample ROI
number invites overfitting. This tool instead re-scores every archived race
with the CURRENT scrape_races code (stored scores came from older model
versions and are ignored) under two arms:

    baseline : the previous model — static JOCKEY_RATINGS at 15%, no trainer
    new      : data-derived jockey + trainer strike-rates (strike_rates.py)

and rolls the strike-rate table forward one day at a time (each race is scored
using only results from strictly earlier days — no leakage). The first
--burn-in days warm the table and are excluded; every later day is a genuine
out-of-sample bet the parameters never saw. ROI is broken out by tier, and a
day-resampling bootstrap gives a CI on the new−baseline delta.

Also reports the historical value-betting result for the record: EV-gated
betting (backing only where model prob beats the market price) was tested and
REJECTED — it degrades ROI, because the market is sharper than the model. Run
with --value-gate to reproduce that.

Data: horses/history/races_<date>.json joined to results_full_<date>.json.

    python3 scripts/backtest_value.py                 # walk-forward, per tier
    python3 scripts/backtest_value.py --bootstrap      # + significance CIs
    python3 scripts/backtest_value.py --value-gate     # + rejected EV-gate note
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

EV_MARGIN = 0.05  # edge required by the (rejected) value-gate variant


# ─────────────────────────────────────────────────────────────
# Scoring one race through the REAL production code path
# ─────────────────────────────────────────────────────────────

def score_and_recommend(prace, use_strike, table):
    """Re-score a race with scrape_races and return its runners (deep-copied)
    with a fresh recommendation, sorted best-first.

    use_strike toggles the arm: baseline = static jockey rating at 15% weight;
    new = data-derived strike-rates at 7.5% + 7.5%. Both paths run the identical
    score_runner / normalise / course-coefficient / make_recommendation code.
    """
    runners = [copy.deepcopy(r) for r in prace["runners"]]
    n = len(runners)

    # Flip the module knobs that score_runner reads, so both arms exercise the
    # same code. Baseline reproduces the old model exactly (jockey 15%, no
    # trainer); new uses the strike table with the shipped split.
    if use_strike:
        s.STRIKE_TABLE = table
        sr.JOCKEY_WEIGHT, sr.TRAINER_WEIGHT = 0.075, 0.075
    else:
        s.STRIKE_TABLE = None
        sr.JOCKEY_WEIGHT, sr.TRAINER_WEIGHT = 0.15, 0.0

    for run in runners:
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
        # Mirror the live pipeline: archived runners carry a stale (or null)
        # "score" which _post_process_win_bets reads for demotion confidence.
        run["score"] = round(run["_score"], 1)
        form = s.parse_form(run.get("form", ""))
        run["recommendation"] = s.make_recommendation(run["_score"], run.get("odds_dec"), n, form)
    s._post_process_win_bets(runners, n)
    return runners


def primaries(runners):
    """Yield (rec_type, runner) for the top-scored Win and EW pick."""
    seen = set()
    for r in runners:
        rt = r["recommendation"]["type"]
        if rt in ("Win", "EachWay") and rt not in seen:
            seen.add(rt)
            yield rt, r


def pnl_for(rt, run, strong_only=False):
    """Flat £1 (stake, ret) for a primary pick, or None if unpriced/void."""
    oc = run.get("_oc")
    if not oc or oc["status"] == "non_runner":
        return None
    od = run.get("odds_dec")
    if not od or od <= 1:
        return None
    if strong_only and run["recommendation"].get("label") != "Strong Win Bet":
        return None
    if rt == "Win":
        res = "correct" if (oc["status"] == "finished" and oc["pos"] == 1) else "incorrect"
    else:
        res = ("ew_win" if (oc["status"] == "finished" and oc["pos"] == 1)
               else "ew_placed" if oc["placed"] else "incorrect")
    return fr.bet_pnl(rt, res, od)


# ─────────────────────────────────────────────────────────────
# Walk-forward driver
# ─────────────────────────────────────────────────────────────

def walk_forward(days, burn_in, value_gate=False):
    """Roll the strike table forward. Return per-day (stake, ret) pairs for
    each arm/tier so ROI and bootstrap CIs can both be computed downstream."""
    table = sr.StrikeTable()
    # arm -> tier -> list of (date, [(stake,ret),...])
    per_day = {a: {t: [] for t in ("win", "swin", "ew", "gate")}
               for a in ("base", "new")}

    for i, (date, praces) in enumerate(days):
        live = i >= burn_in
        if live:
            day = {a: {t: [] for t in per_day[a]} for a in per_day}
            for prace in praces:
                for arm, use_strike in (("base", False), ("new", True)):
                    runners = score_and_recommend(prace, use_strike, table)
                    for rt, run in primaries(runners):
                        p = pnl_for(rt, run)
                        if p:
                            day[arm]["win" if rt == "Win" else "ew"].append(p)
                        if rt == "Win":
                            ps = pnl_for(rt, run, strong_only=True)
                            if ps:
                                day[arm]["swin"].append(ps)
                            if value_gate:
                                g = _gate_pnl(run)
                                if g:
                                    day[arm]["gate"].append(g)
            for a in per_day:
                for t in per_day[a]:
                    per_day[a][t].append((date, day[a][t]))
        # advance the table with this day's outcomes (after scoring it)
        for prace in praces:
            table.add_race(prace)
    return per_day


def _gate_pnl(run):
    """(stake, ret) for the rejected EV-gate: a Win pick kept only if the
    model's (non-normalised) prob beats the price by EV_MARGIN."""
    if run["recommendation"]["type"] != "Win":
        return None
    od = run.get("odds_dec")
    mp = s.score_to_winprob(run["_score"])
    ev = (mp * od - 1) if od else None
    if ev is None or ev < EV_MARGIN:
        return None
    return pnl_for("Win", run)


# ─────────────────────────────────────────────────────────────
# Reporting
# ─────────────────────────────────────────────────────────────

def roi(pairs):
    st = sum(p[0] for p in pairs)
    rt = sum(p[1] for p in pairs)
    return ((rt - st) / st * 100 if st else 0.0), st


def flat(day_pairs):
    return [p for _, ps in day_pairs for p in ps]


def bootstrap(per_day, tier, nboot=2000):
    """Resample whole days; return (mean, lo, hi, p_gt0) for new−base ROI."""
    paired = list(zip(per_day["new"][tier], per_day["base"][tier]))
    deltas = []
    for _ in range(nboot):
        samp = [random.choice(paired) for _ in paired]
        nb = [p for (_, np_), _ in samp for p in np_]
        bb = [p for _, (_, bp) in samp for p in bp]
        deltas.append(roi(nb)[0] - roi(bb)[0])
    deltas.sort()
    m = sum(deltas) / len(deltas)
    return m, deltas[int(0.025 * len(deltas))], deltas[int(0.975 * len(deltas))], \
        sum(1 for d in deltas if d > 0) / len(deltas)


TIERS = [("swin", "Strong Win Bet"), ("win", "All Win"), ("ew", "Each Way")]


def report(per_day, do_bootstrap, value_gate):
    print("\n================  WALK-FORWARD ROI (out-of-sample)  ================")
    print(f'{"tier":16} | {"baseline":>18} | {"new (strike-rate)":>18}')
    print("-" * 62)
    for key, name in TIERS:
        b_roi, b_st = roi(flat(per_day["base"][key]))
        n_roi, n_st = roi(flat(per_day["new"][key]))
        print(f"{name:16} | {b_roi:+6.1f}%  n{int(b_st):<5} "
              f"| {n_roi:+6.1f}%  n{int(n_st):<5}")

    # overall = win + ew
    for arm in ("base", "new"):
        pass
    b_all = roi(flat(per_day["base"]["win"]) + flat(per_day["base"]["ew"]))
    n_all = roi(flat(per_day["new"]["win"]) + flat(per_day["new"]["ew"]))
    print(f'{"overall":16} | {b_all[0]:+6.1f}%  n{int(b_all[1]):<5} '
          f'| {n_all[0]:+6.1f}%  n{int(n_all[1]):<5}')

    if do_bootstrap:
        print("\n----  significance: new − baseline ROI (bootstrap over days)  ----")
        for key, name in TIERS:
            m, lo, hi, p = bootstrap(per_day, key)
            verdict = "significant" if (lo > 0 or hi < 0) else "not significant"
            print(f"{name:16} | delta {m:+5.1f}pp  95% CI [{lo:+.1f}, {hi:+.1f}]  "
                  f"P(new>base)={p:.2f}  ({verdict})")

    if value_gate:
        b = roi(flat(per_day["new"]["gate"]))
        w = roi(flat(per_day["new"]["win"]))
        print("\n----  REJECTED value-gate (new model, Win tier only)  ----")
        print(f"  ungated Win : {w[0]:+6.1f}%  n{int(w[1])}")
        print(f"  EV-gated Win: {b[0]:+6.1f}%  n{int(b[1])}   "
              f"(gating removes bets and does not improve ROI)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="horses", help="Directory holding history/")
    ap.add_argument("--burn-in", type=int, default=15,
                    help="Warm-up days excluded from ROI (default 15)")
    ap.add_argument("--bootstrap", action="store_true",
                    help="Add day-resampling significance CIs")
    ap.add_argument("--value-gate", action="store_true",
                    help="Also show the (rejected) EV-gated Win variant")
    ap.add_argument("--seed", type=int, default=1)
    args = ap.parse_args()
    random.seed(args.seed)

    hist = os.path.join(os.path.abspath(args.out), "history")
    days = sr.iter_history(hist)
    if not days:
        print("No joined history found (need races_*.json + results_full_*.json).",
              file=sys.stderr)
        return
    print(f"{len(days)} days ({days[0][0]}..{days[-1][0]}), "
          f"burn-in {args.burn_in} -> {len(days) - args.burn_in} out-of-sample days")

    per_day = walk_forward(days, args.burn_in, value_gate=args.value_gate)
    report(per_day, args.bootstrap, args.value_gate)


if __name__ == "__main__":
    main()
