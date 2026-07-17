#!/usr/bin/env python3
"""
Walk-forward sweep of the retired "Win Bet" tier's score cutoff and odds
ceiling — validates the decision to drop it entirely rather than tighten it.

Context: scrape_races.py::make_recommendation used to fire a "Win Bet" at
score>=60, odds<=10.0 alongside the higher "Strong Win Bet" tier (score>=72,
odds<=5.0). A walk-forward backtest (scripts/backtest_value.py) showed Win
Bet alone at -8.9% ROI (n=878) dragging an otherwise roughly-breakeven Strong
Win Bet down to -6.0% overall — so the tier was retired outright rather than
re-tuned. This script re-tests that call: it re-scores the archive with the
CURRENT production model (same no-leakage rolling StrikeTable as
backtest_value.py) and sweeps a grid of (score cutoff, odds ceiling) pairs
for a hypothetical lower tier layered under Strong Win Bet, including "off"
(the shipped, tier-retired state) as one point in the grid. If "off" isn't
beaten by every intermediate cutoff tried, the retirement call should be
revisited.

    python3 scripts/backtest_win_threshold.py                # ROI per combo
    python3 scripts/backtest_win_threshold.py --bootstrap     # + significance
"""

import argparse
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scrape_races as s
import strike_rates as sr
import fetch_results as fr
import backtest_value as bv

SCORE_CUTOFFS = [60, 62, 64, 66, 68, 70]
ODDS_CEILINGS = [6.0, 8.0, 10.0, 12.0]


def make_recommendation_variant(score, odds_dec, form, cutoff, odds_ceiling):
    """Mirrors scrape_races.make_recommendation's Strong Win Bet branch
    exactly (unconditional — that tier isn't being swept), then layers a
    configurable lower tier under it. cutoff=None disables the lower tier
    entirely, reproducing the current shipped (tier-retired) behaviour.
    """
    high_dnf = form["dnf_rate"] > 0.40
    market_disagrees = bool(odds_dec and score > 62 and odds_dec > 8.0)

    if score >= 72 and not high_dnf and not market_disagrees and odds_dec and odds_dec <= 5.0:
        return "Strong Win Bet"
    if (cutoff is not None and score >= cutoff and not high_dnf and not market_disagrees
            and odds_dec and odds_dec <= odds_ceiling):
        return "Win Bet"
    return "Skip"


def pnl_for(run):
    """(stake, ret) for a flat £1 Win bet on this primary pick, or None if
    unpriced/void — identical rule to backtest_value.pnl_for for rt=='Win'."""
    oc = run.get("_oc")
    if not oc or oc["status"] == "non_runner":
        return None
    od = run.get("odds_dec")
    if not od or od <= 1:
        return None
    res = "correct" if (oc["status"] == "finished" and oc["pos"] == 1) else "incorrect"
    return fr.bet_pnl("Win", res, od)


def build_grid():
    grid = [("off (shipped)", None, None)]
    for cutoff in SCORE_CUTOFFS:
        for ceiling in ODDS_CEILINGS:
            grid.append((f"score>={cutoff} odds<={ceiling:g}", cutoff, ceiling))
    return grid


def walk_forward(days, burn_in, grid):
    """Roll the strike table forward. Return per-day (stake, ret) pairs for
    each grid combo's single primary Win pick per race."""
    table = sr.StrikeTable()
    per_day = {label: [] for label, _, _ in grid}

    for i, (date, praces) in enumerate(days):
        live = i >= burn_in
        if live:
            day = {label: [] for label, _, _ in grid}
            for prace in praces:
                runners = bv.score_and_recommend(prace, True, table)
                scored = [(run, s.parse_form(run.get("form", ""))) for run in runners]
                for label, cutoff, ceiling in grid:
                    primary = None
                    for run, form in scored:
                        tier = make_recommendation_variant(
                            run["_score"], run.get("odds_dec"), form, cutoff, ceiling)
                        if tier != "Skip":
                            primary = run
                            break
                    if primary is not None:
                        p = pnl_for(primary)
                        if p:
                            day[label].append(p)
            for label in per_day:
                per_day[label].append((date, day[label]))
        for prace in praces:
            table.add_race(prace)
    return per_day


def roi(pairs):
    st = sum(p[0] for p in pairs)
    rt = sum(p[1] for p in pairs)
    return ((rt - st) / st * 100 if st else 0.0), st


def flat(day_pairs):
    return [p for _, ps in day_pairs for p in ps]


def bootstrap(per_day, label, baseline_label, nboot=2000):
    """Resample whole days; return (mean, lo, hi, p_gt0) for
    label−baseline_label ROI delta."""
    paired = list(zip(per_day[label], per_day[baseline_label]))
    deltas = []
    for _ in range(nboot):
        samp = [random.choice(paired) for _ in paired]
        a = [p for (_, ap), _ in samp for p in ap]
        b = [p for _, (_, bp) in samp for p in bp]
        deltas.append(roi(a)[0] - roi(b)[0])
    deltas.sort()
    m = sum(deltas) / len(deltas)
    return m, deltas[int(0.025 * len(deltas))], deltas[int(0.975 * len(deltas))], \
        sum(1 for d in deltas if d > 0) / len(deltas)


def report(per_day, grid, do_bootstrap):
    print("\n================  WIN-TIER THRESHOLD SWEEP (out-of-sample)  ================")
    print(f'{"combo":24} | {"ROI":>8} | {"n":>6}')
    print("-" * 44)
    for label, _, _ in grid:
        r, st = roi(flat(per_day[label]))
        print(f"{label:24} | {r:+6.1f}%  | {int(st):>6}")

    if do_bootstrap:
        baseline = grid[0][0]  # "off (shipped)"
        print(f"\n----  significance: combo − {baseline!r} ROI (bootstrap over days)  ----")
        best_beats_off = 0
        for label, cutoff, ceiling in grid[1:]:
            m, lo, hi, p = bootstrap(per_day, label, baseline)
            verdict = "significant" if (lo > 0 or hi < 0) else "not significant"
            if hi < 0:
                best_beats_off += 1
            print(f"{label:24} | delta {m:+5.1f}pp  95% CI [{lo:+.1f}, {hi:+.1f}]  "
                  f"P(combo>off)={p:.2f}  ({verdict})")
        n_combos = len(grid) - 1
        print(f'\n{best_beats_off}/{n_combos} intermediate cutoffs significantly WORSE than '
              f'retiring the tier entirely.')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="horses", help="Directory holding history/")
    ap.add_argument("--burn-in", type=int, default=15,
                    help="Warm-up days excluded from ROI (default 15)")
    ap.add_argument("--bootstrap", action="store_true",
                    help="Add day-resampling significance CIs vs the shipped (off) state")
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

    grid = build_grid()
    per_day = walk_forward(days, args.burn_in, grid)
    report(per_day, grid, args.bootstrap)


if __name__ == "__main__":
    main()
