#!/usr/bin/env python3
"""
Walk-forward sweep of a field-size cap on the Strong Win Bet tier.

Context: the 2026-07-17 drift review (see memory note strong-win-bet-drift-
jul2026) found hit rate degrades smoothly with field size across the whole
archive (0-6 runners ~57-58%, 13+ runners ~31-37%), and flagged a field-size
cap on Strong Win Bet as a queued-but-unvalidated candidate. This script
re-scores the archive with the CURRENT production model (same no-leakage
rolling StrikeTable as backtest_value.py) and sweeps a grid of field-size
caps, including "off" (the shipped, uncapped state) as one point in the grid.

    python3 scripts/backtest_field_size.py                # ROI per combo
    python3 scripts/backtest_field_size.py --bootstrap     # + significance
"""

import argparse
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import strike_rates as sr
import fetch_results as fr
import backtest_value as bv

FIELD_CAPS = [6, 8, 10, 12, 14, 16]


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
    grid = [("off (shipped, uncapped)", None)]
    for cap in FIELD_CAPS:
        grid.append((f"field_size<={cap}", cap))
    return grid


def walk_forward(days, burn_in, grid):
    """Roll the strike table forward. Each grid combo takes the same Strong
    Win Bet primary pick as production, but drops it (bet skipped) if the
    race's field size exceeds the combo's cap."""
    table = sr.StrikeTable()
    per_day = {label: [] for label, _ in grid}

    for i, (date, praces) in enumerate(days):
        live = i >= burn_in
        if live:
            day = {label: [] for label, _ in grid}
            for prace in praces:
                runners = bv.score_and_recommend(prace, True, table)
                n = len(runners)
                for rt, run in bv.primaries(runners):
                    if rt != "Win" or run["recommendation"].get("label") != "Strong Win Bet":
                        continue
                    p = pnl_for(run)
                    if not p:
                        continue
                    for label, cap in grid:
                        if cap is None or n <= cap:
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
    print("\n================  STRONG WIN BET FIELD-SIZE CAP SWEEP (out-of-sample)  ================")
    print(f'{"combo":26} | {"ROI":>8} | {"hit%":>6} | {"n":>6}')
    print("-" * 55)
    for label, _ in grid:
        pairs = flat(per_day[label])
        r, st = roi(pairs)
        hit = (sum(1 for stake, ret in pairs if ret > stake) / len(pairs) * 100) if pairs else 0.0
        print(f"{label:26} | {r:+6.1f}%  | {hit:5.1f}% | {int(st):>6}")

    if do_bootstrap:
        baseline = grid[0][0]  # "off (shipped, uncapped)"
        print(f"\n----  significance: combo − {baseline!r} ROI (bootstrap over days)  ----")
        wins = 0
        for label, cap in grid[1:]:
            m, lo, hi, p = bootstrap(per_day, label, baseline)
            verdict = "significant" if (lo > 0 or hi < 0) else "not significant"
            if lo > 0:
                wins += 1
            print(f"{label:26} | delta {m:+5.1f}pp  95% CI [{lo:+.1f}, {hi:+.1f}]  "
                  f"P(cap>off)={p:.2f}  ({verdict})")
        n_combos = len(grid) - 1
        print(f'\n{wins}/{n_combos} caps significantly BETTER than the uncapped shipped state.')


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
