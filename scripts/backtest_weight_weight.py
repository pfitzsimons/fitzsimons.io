#!/usr/bin/env python3
"""
Walk-forward sensitivity sweep of the weight-component's score contribution
(currently a hardcoded 10% weight in scrape_races.py::score_runner /
normalise_weight_scores, "lower weight carried = advantage").

Context: the 2026-07-17 drift review (see memory note strong-win-bet-drift-
jul2026) found the weight factor mildly backwards within Strong Win Bet picks
— winners average a LOWER weight-advantage sub-score than losers, in both
handicap and non-handicap races, persistent across the whole archive and more
pronounced in the trailing drift window. Flagged as a queued-but-unvalidated
candidate: reweight (including to 0, i.e. drop it) and re-test.

This script re-scores the archive with the CURRENT production model (same
no-leakage rolling StrikeTable as backtest_value.py), then swaps the weight
sub-score's contribution from the shipped 10% to each candidate weight,
re-sorts runners by the adjusted score, and re-derives the recommendation —
mirroring scrape_races.py's real pipeline (make_recommendation +
_post_process_win_bets) exactly, just with one coefficient changed.

    python3 scripts/backtest_weight_weight.py                # ROI per weight
    python3 scripts/backtest_weight_weight.py --bootstrap     # + significance
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

SHIPPED_WEIGHT_WEIGHT = 0.10
CANDIDATE_WEIGHTS = [0.0, 0.05, 0.10, 0.15, 0.20]


def reweight(runners, praces, new_ww):
    """Return a deep-copied, re-scored/re-sorted/re-recommended runner list
    with the weight sub-score's contribution swapped from the shipped 10%
    to new_ww. Other components (odds, form, jockey/trainer, etc.) are held
    exactly as bv.score_and_recommend computed them."""
    cf = s.COURSE_COEFFICIENTS.get(praces["course"], 1.0)
    out = []
    for run in runners:
        r = dict(run)
        w_score = r["_components"]["weight"]  # already field-normalised, 0-100
        delta = cf * (new_ww - SHIPPED_WEIGHT_WEIGHT) * w_score
        new_score = max(0.0, min(100.0, r["_score"] + delta))
        r["_score"] = new_score
        r["score"] = round(new_score, 1)
        out.append(r)
    out.sort(key=lambda r: r["_score"], reverse=True)
    n = len(out)
    for r in out:
        form = s.parse_form(r.get("form", ""))
        r["recommendation"] = s.make_recommendation(r["_score"], r.get("odds_dec"), n, form)
    s._post_process_win_bets(out, n)
    return out


def pnl_for(run):
    oc = run.get("_oc")
    if not oc or oc["status"] == "non_runner":
        return None
    od = run.get("odds_dec")
    if not od or od <= 1:
        return None
    res = "correct" if (oc["status"] == "finished" and oc["pos"] == 1) else "incorrect"
    return fr.bet_pnl("Win", res, od)


def walk_forward(days, burn_in, weights, strong_only):
    table = sr.StrikeTable()
    per_day = {w: [] for w in weights}

    for i, (date, praces) in enumerate(days):
        live = i >= burn_in
        if live:
            day = {w: [] for w in weights}
            for prace in praces:
                runners = bv.score_and_recommend(prace, True, table)
                for w in weights:
                    adj = reweight(runners, prace, w)
                    for rt, run in bv.primaries(adj):
                        if rt != "Win":
                            continue
                        if strong_only and run["recommendation"].get("label") != "Strong Win Bet":
                            continue
                        p = pnl_for(run)
                        if p:
                            day[w].append(p)
            for w in per_day:
                per_day[w].append((date, day[w]))
        for prace in praces:
            table.add_race(prace)
    return per_day


def roi(pairs):
    st = sum(p[0] for p in pairs)
    rt = sum(p[1] for p in pairs)
    return ((rt - st) / st * 100 if st else 0.0), st


def flat(day_pairs):
    return [p for _, ps in day_pairs for p in ps]


def bootstrap(per_day, w, baseline_w, nboot=2000):
    paired = list(zip(per_day[w], per_day[baseline_w]))
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


def report(per_day, weights, do_bootstrap):
    print("\n================  WEIGHT-COMPONENT SENSITIVITY SWEEP (Strong Win Bet, out-of-sample)  ================")
    print(f'{"weight":>8} | {"ROI":>8} | {"hit%":>6} | {"n":>6}')
    print("-" * 40)
    for w in weights:
        pairs = flat(per_day[w])
        r, st = roi(pairs)
        hit = (sum(1 for stake, ret in pairs if ret > stake) / len(pairs) * 100) if pairs else 0.0
        tag = "  (shipped)" if w == SHIPPED_WEIGHT_WEIGHT else ""
        print(f"{w:8.2f} | {r:+6.1f}%  | {hit:5.1f}% | {int(st):>6}{tag}")

    if do_bootstrap:
        print(f"\n----  significance: weight − {SHIPPED_WEIGHT_WEIGHT} (shipped) ROI (bootstrap over days)  ----")
        for w in weights:
            if w == SHIPPED_WEIGHT_WEIGHT:
                continue
            m, lo, hi, p = bootstrap(per_day, w, SHIPPED_WEIGHT_WEIGHT)
            verdict = "significant" if (lo > 0 or hi < 0) else "not significant"
            print(f"{w:8.2f} | delta {m:+5.1f}pp  95% CI [{lo:+.1f}, {hi:+.1f}]  "
                  f"P(w>shipped)={p:.2f}  ({verdict})")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="horses", help="Directory holding history/")
    ap.add_argument("--burn-in", type=int, default=15,
                    help="Warm-up days excluded from ROI (default 15)")
    ap.add_argument("--bootstrap", action="store_true",
                    help="Add day-resampling significance CIs vs the shipped weight")
    ap.add_argument("--all-win", action="store_true",
                    help="Score all Win picks, not just Strong Win Bet (default: Strong only)")
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

    per_day = walk_forward(days, args.burn_in, CANDIDATE_WEIGHTS, strong_only=not args.all_win)
    report(per_day, CANDIDATE_WEIGHTS, args.bootstrap)


if __name__ == "__main__":
    main()
