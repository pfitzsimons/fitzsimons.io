#!/usr/bin/env python3
"""
Walk-forward backtest of a flat-handicap Win-bet gate.

A race-type split of live accuracy (Jun-Jul 2026) showed flat handicaps are
both the weakest segment for Win picks and a growing share of them. This tool
asks the ship/no-ship question properly: over the whole archive, walked
forward leak-free with the CURRENT production scoring code, does skipping Win
bets in flat handicaps improve flat-stake ROI?

VERDICT (2026-07-16, 58 OOS days): REJECTED — do not ship the gate. Flat
handicaps have the LOWEST hit rate (28.5%) but the BEST ROI (-2.0%) of all
four segments, because their winners come at bigger prices; gating them out
made ROI worse (-9.6% vs -6.0% baseline), delta -3.5pp, 95% CI [-13.1, +5.7],
P(gated>base)=0.22. Hit rate and ROI disagree — optimise ROI. Kept for re-runs
as the archive grows (jumps hcp at -21.6% n=140 is the segment to re-check).

Arms share one scoring pass (the shipped model: strike-rate table rolled
forward day by day, as in backtest_value.py) and differ only in bet selection:

    baseline : every primary Win pick
    gated    : primary Win picks outside flat handicaps

Race typing is by title keywords — hurdle/chase/bumper/"flat race" => jumps;
handicap/nursery => handicap. A per-segment table shows where ROI actually
comes from, a 10-day window table shows whether the segment effect is stable
or a recent regime, and a day-resampling bootstrap gives a CI on the
gated−baseline ROI delta.

Data: horses/history/races_<date>.json joined to results_full_<date>.json.

    python3 scripts/backtest_handicap.py                # walk-forward + windows
    python3 scripts/backtest_handicap.py --bootstrap    # + significance CI
"""

import argparse
import os
import random
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scrape_races as s  # noqa: E402
import strike_rates as sr  # noqa: E402
import backtest_value as bv  # noqa: E402

_JUMPS = re.compile(r"hurdle|chase|bumper|national hunt|flat race", re.I)
_HCP = re.compile(r"handicap|nursery", re.I)

SEGMENTS = ("flat hcp", "flat non-hcp", "jumps hcp", "jumps non-hcp")


def segment(title: str) -> str:
    code = "jumps" if _JUMPS.search(title or "") else "flat"
    return f"{code} {'hcp' if _HCP.search(title or '') else 'non-hcp'}"


# ─────────────────────────────────────────────────────────────
# Walk-forward driver
# ─────────────────────────────────────────────────────────────

def walk_forward(days, burn_in):
    """One shipped-model scoring pass per race; collect per-day Win-pick
    (stake, ret, won) triples tagged by race segment."""
    table = sr.StrikeTable()
    per_day = []  # (date, [(segment, stake, ret, won), ...])

    for i, (date, praces) in enumerate(days):
        if i >= burn_in:
            picks = []
            for prace in praces:
                runners = bv.score_and_recommend(prace, True, table)
                for rt, run in bv.primaries(runners):
                    if rt != "Win":
                        continue
                    p = bv.pnl_for(rt, run)
                    if p:
                        oc = run["_oc"]
                        won = oc["status"] == "finished" and oc["pos"] == 1
                        picks.append((segment(prace["title"]), p[0], p[1], won))
            per_day.append((date, picks))
        for prace in praces:
            table.add_race(prace)
    return per_day


# ─────────────────────────────────────────────────────────────
# Reporting
# ─────────────────────────────────────────────────────────────

def stats(picks):
    st = sum(p[1] for p in picks)
    rt = sum(p[2] for p in picks)
    won = sum(1 for p in picks if p[3])
    roi = (rt - st) / st * 100 if st else 0.0
    hit = won / len(picks) * 100 if picks else 0.0
    return roi, hit, len(picks)


def gate(picks):
    return [p for p in picks if p[0] != "flat hcp"]


def bootstrap(per_day, nboot=2000):
    """Resample whole days; return (mean, lo, hi, p_gt0) for gated−baseline ROI."""
    deltas = []
    for _ in range(nboot):
        samp = [random.choice(per_day) for _ in per_day]
        allp = [p for _, ps in samp for p in ps]
        deltas.append(stats(gate(allp))[0] - stats(allp)[0])
    deltas.sort()
    m = sum(deltas) / len(deltas)
    return m, deltas[int(0.025 * len(deltas))], deltas[int(0.975 * len(deltas))], \
        sum(1 for d in deltas if d > 0) / len(deltas)


def report(per_day, window, do_bootstrap):
    allp = [p for _, ps in per_day for p in ps]

    print("\n================  PER-SEGMENT WIN-PICK ROI (out-of-sample)  ============")
    print(f'{"segment":14} | {"n":>4} | {"hit%":>6} | {"ROI":>7}')
    print("-" * 42)
    for seg in SEGMENTS:
        roi, hit, n = stats([p for p in allp if p[0] == seg])
        print(f"{seg:14} | {n:>4} | {hit:5.1f}% | {roi:+6.1f}%")

    print("\n================  BASELINE vs FLAT-HANDICAP GATE  ======================")
    for name, picks in (("baseline (all)", allp), ("gated (no flat hcp)", gate(allp))):
        roi, hit, n = stats(picks)
        print(f"{name:20} | n={n:<4} hit {hit:5.1f}%  ROI {roi:+6.1f}%")

    print(f"\n----  {window}-day windows: flat hcp vs rest (is the effect a regime?)  ----")
    print(f'{"window":24} | {"flat hcp":>22} | {"rest":>22}')
    for i in range(0, len(per_day), window):
        chunk = per_day[i:i + window]
        picks = [p for _, ps in chunk for p in ps]
        fh, rest = [p for p in picks if p[0] == "flat hcp"], gate(picks)
        fr_, fh_h, fh_n = stats(fh)
        rr, r_h, r_n = stats(rest)
        label = f"{chunk[0][0]}..{chunk[-1][0]}"
        print(f"{label:24} | {fr_:+6.1f}% {fh_h:4.0f}% n{fh_n:<4} "
              f"| {rr:+6.1f}% {r_h:4.0f}% n{r_n:<4}")

    if do_bootstrap:
        m, lo, hi, p = bootstrap(per_day)
        verdict = "significant" if (lo > 0 or hi < 0) else "NOT significant"
        print("\n----  significance: gated − baseline ROI (bootstrap over days)  ----")
        print(f"delta {m:+5.1f}pp  95% CI [{lo:+.1f}, {hi:+.1f}]  "
              f"P(gated>base)={p:.2f}  ({verdict})")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="horses", help="Directory holding history/")
    ap.add_argument("--burn-in", type=int, default=15,
                    help="Warm-up days excluded from ROI (default 15)")
    ap.add_argument("--window", type=int, default=10,
                    help="Days per drift window in the regime table (default 10)")
    ap.add_argument("--bootstrap", action="store_true",
                    help="Add day-resampling significance CI")
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

    per_day = walk_forward(days, args.burn_in)
    report(per_day, args.window, args.bootstrap)


if __name__ == "__main__":
    main()
