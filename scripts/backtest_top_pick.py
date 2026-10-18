#!/usr/bin/env python3
"""
Walk-forward test of the per-race Top Pick: score vs favourite vs the
market-anchored win probability (scrape_races.ANCHOR_COEFS).

Every archived race is re-scored with the production pipeline
(backtest_value.score_and_recommend, strike table rolled one day at a time,
15-day burn-in). The field is the START-OF-DAY field: morning non-runners
are gone, and a pick that later withdrew is void (not counted). Only races
with exactly one matched winner are used.

Each market basis is tested on its own, over every race that has a full set
of that price (scrape_races.anchor_features):
  book     — morning best bookmaker price (archived from 2026-07-17)
  forecast — Sporting Life overnight forecast (odds_dec)
For each basis, two conditional logits are refit at the start of each
calendar month on every earlier race (expanding window) and tested on that
month: market only, and market + components (ANCHOR_FEATURES + runs). No
test race was ever seen by a fit. Testing starts in the first month with
MIN_TRAIN earlier races.

Reports top-pick win% and ROI (settled at best book, falling back to SP, via
fetch_results.settle_price) for the score, the favourite on that basis and
the components model, plus test log-loss and day-bootstrap 95% CIs on the
gains. ANCHOR_COMPONENTS in scrape_races.py records which basis ships with
components: only where they beat market-only here.

    python3 scripts/backtest_top_pick.py           # walk-forward comparison
    python3 scripts/backtest_top_pick.py --fit     # fit on everything, print
                                                   # ANCHOR_COEFS to paste
"""

import argparse
import math
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scrape_races as s
import strike_rates as sr
import fetch_results as fr
import backtest_value as bv

HIST = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "horses", "history")
BURN_IN = 15
L2 = 1e-3          # ridge on everything except the market coefficient
MIN_TRAIN = 300    # earlier races needed before a month is tested
BASES = ("book", "forecast")
PRICE = {"book": "best_odds_dec", "forecast": "odds_dec"}


def build_races():
    """{basis: [(date, [runner, ...], winner_index, feature_rows)]} — leak-free."""
    days = sr.iter_history(HIST)
    table = sr.StrikeTable()
    out = {b: [] for b in BASES}
    for i, (d, praces) in enumerate(days):
        if i >= BURN_IN:
            for pr in praces:
                rr = bv.score_and_recommend(pr, True, table)
                if len(rr) < 2 or any(r.get("_oc") is None for r in rr):
                    continue
                won = [r["_oc"]["status"] == "finished" and r["_oc"]["pos"] == 1 for r in rr]
                if sum(won) != 1:
                    continue
                for b in BASES:
                    if all((r.get(PRICE[b]) or 0) > 1 for r in rr):
                        out[b].append((d, rr, won.index(True), s.anchor_features(rr, b)))
        for pr in praces:
            table.add_race(pr)
    return out


def market_only(races):
    return [(d, rr, y, [x[:1] for x in X]) for d, rr, y, X in races]


# ── conditional logit (Newton's method, stdlib) ─────────────────

def _solve(A, b):
    n = len(b)
    M = [row[:] + [b[i]] for i, row in enumerate(A)]
    for c in range(n):
        p = max(range(c, n), key=lambda r: abs(M[r][c]))
        M[c], M[p] = M[p], M[c]
        for r in range(n):
            if r != c and M[c][c]:
                f = M[r][c] / M[c][c]
                M[r] = [x - f * y for x, y in zip(M[r], M[c])]
    return [M[i][n] / M[i][i] for i in range(n)]


def fit(races):
    k = len(races[0][3][0])
    b = [1.0] + [0.0] * (k - 1)
    for _ in range(30):
        g = [0.0] * k
        H = [[0.0] * k for _ in range(k)]
        for _, _, y, X in races:
            p = s.softmax_probs(X, b)
            mu = [sum(p[j] * X[j][a] for j in range(len(X))) for a in range(k)]
            for a in range(k):
                g[a] += X[y][a] - mu[a]
            for j, x in enumerate(X):
                d = [x[a] - mu[a] for a in range(k)]
                for a in range(k):
                    pa = p[j] * d[a]
                    for c in range(a, k):
                        H[a][c] += pa * d[c]
        n = len(races)
        for a in range(k):
            g[a] = g[a] / n - (2 * L2 * b[a] if a else 0)
            for c in range(a, k):
                H[a][c] = H[a][c] / n + (2 * L2 if a == c and a else 0)
                H[c][a] = H[a][c]
        step = _solve(H, g)
        b = [x + d for x, d in zip(b, step)]
        if max(abs(d) for d in step) < 1e-7:
            break
    return b


# ── evaluation ──────────────────────────────────────────────────

def pnl(r):
    """(won, £1 P&L) for a top pick, or None if void (late non-runner/unpriced)."""
    oc = r["_oc"]
    if oc["status"] == "non_runner":
        return None
    od = fr.settle_price(r, oc)[0]
    if not od or od <= 1:
        return None
    won = oc["status"] == "finished" and oc["pos"] == 1
    return won, (od - 1 if won else -1.0)


def boot(byday, idx, reps=2000):
    random.seed(7)
    days = list(byday)
    pts = []
    for _ in range(reps):
        vals = [v[idx] for _ in days for v in byday[random.choice(days)]]
        pts.append(sum(vals) / len(vals))
    pts.sort()
    return pts[int(0.025 * reps)], pts[int(0.975 * reps)]


def walk_forward(races, names, basis):
    months = sorted({r[0][:7] for r in races})
    first = next((m for m in months if sum(r[0][:7] < m for r in races) >= MIN_TRAIN), None)
    if first is None:
        print(f"\n{basis}: not enough races to test")
        return
    test = []
    for m in months[months.index(first):]:
        tr = [r for r in races if r[0][:7] < m]
        b, bm = fit(tr), fit(market_only(tr))
        print(f"  fit through {m} ({basis}): "
              + ", ".join(f"{nm} {v:+.3f}" for nm, v in zip(names, b)))
        for d, rr, y, X in races:
            if d[:7] == m:
                test.append((d, rr, y, s.softmax_probs(X, b),
                             s.softmax_probs([x[:1] for x in X], bm)))

    key = PRICE[basis]
    picks = {
        "score top": [max(range(len(rr)), key=lambda j: rr[j]["_score"]) for _, rr, *_ in test],
        "favourite": [min(range(len(rr)), key=lambda j: rr[j][key]) for _, rr, *_ in test],
        "components": [max(range(len(rr)), key=lambda j: p[j]) for _, rr, _, p, _ in test],
    }
    print(f"\n{basis} prices: {len(test)} test races ({first} onward)")
    for a, pk in picks.items():
        n = w = 0
        ret = 0.0
        for (_, rr, *_), j in zip(test, pk):
            res = pnl(rr[j])
            if res:
                n += 1; w += res[0]; ret += res[1]
        print(f"  {a:11s} top  n={n:5d}  win% {100 * w / n:5.2f}  ROI {100 * ret / n:+6.1f}%")
    ll = sum(-math.log(p[y]) for _, _, y, p, _ in test) / len(test)
    llm = sum(-math.log(pm[y]) for _, _, y, _, pm in test) / len(test)
    print(f"  log-loss: market only {llm:.4f}, market + components {ll:.4f}")

    # Paired per-race differences, voiding a race if any compared pick withdrew.
    byday = {}
    for k, (d, rr, y, p, pm) in enumerate(test):
        c, f, sc = picks["components"][k], picks["favourite"][k], picks["score top"][k]
        if any(rr[j]["_oc"]["status"] == "non_runner" for j in (c, f, sc)):
            continue
        byday.setdefault(d, []).append(((f == y) - (sc == y), (c == y) - (f == y),
                                        -math.log(pm[y]) + math.log(p[y])))
    allv = [v for vs in byday.values() for v in vs]
    print("  gain (95% day-bootstrap CI)")
    for i, (lab, k) in enumerate([("favourite vs score top, win%", 100),
                                  ("components vs favourite, win%", 100),
                                  ("components vs market, log-loss", 1)]):
        lo, hi = boot(byday, i)
        pt = sum(v[i] for v in allv) / len(allv)
        sig = "significant" if lo > 0 else "not significant"
        print(f"  {lab:31s} {k * pt:+.4f}  [{k * lo:+.4f}, {k * hi:+.4f}]  {sig}")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--fit", action="store_true", help="fit on all races and print ANCHOR_COEFS")
    args = ap.parse_args()

    races = build_races()
    names = ["market", *s.ANCHOR_FEATURES, "runs"]
    for b in BASES:
        rs = races[b]
        print(f"{b}: {len(rs)} races, {rs[0][0]}..{rs[-1][0]}")

    if args.fit:
        # Bases shipped without components get market-only coefficients.
        print("ANCHOR_COEFS = {")
        for basis in BASES:
            if s.ANCHOR_COMPONENTS[basis]:
                b = fit(races[basis])
            else:
                b = fit(market_only(races[basis])) + [0.0] * (len(names) - 1)
            print(f'    "{basis}": {{')
            for nm, v in zip(names, b):
                print(f'        "{nm}": {v:.4f},')
            print("    },")
        print("}")
        return

    for b in BASES:
        walk_forward(races[b], names, b)


if __name__ == "__main__":
    main()
