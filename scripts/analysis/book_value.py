#!/usr/bin/env python3
"""
The open lead: a market-anchored logit on the best BOOKMAKER price, betting
EV > edge at odds <= 10 and settling at that same (bettable) price.
(reads rows.json; needs numpy + scipy; data since 2026-07-17 only)

Arms: book (log implied prob from best_odds_dec), steam (+ log forecast/book
gap), all (+ jockey/trainer/consistency/recent_form). Then a robustness check
on the book arm: forward and reversed train/test split with day-bootstrap CIs.
Treat any positive result as a paper-trading hypothesis, not a finding.

    python3 scripts/analysis/book_value.py
"""
import json, math, os, random
import numpy as np
from scipy.optimize import minimize

HERE = os.path.dirname(os.path.abspath(__file__))
rows = [(d, rr) for d, rr in json.load(open(os.path.join(HERE, 'rows.json')))
        if d >= '2026-07-17' and all(x.get('bo') for x in rr)]
F = ['recent_form', 'consistency', 'jockey', 'trainer']


def build(rs, arm):
    X, Y = [], []
    for _, rr in rs:
        q = np.array([1 / x['bo'] for x in rr]); cols = [np.log(q / q.sum())]
        if arm in ('steam', 'all'):
            g = np.log(np.array([x['od'] / x['bo'] for x in rr])); cols.append(g - g.mean())
        if arm == 'all':
            for f in F:
                v = np.array([float(x['comp'].get(f) or 50) for x in rr]) / 10
                cols.append(v - v.mean())
        X.append(np.stack(cols, 1)); Y.append([x['won'] for x in rr].index(True))
    return X, Y


def nll(b, X, Y):
    s = 0.0
    for x, y in zip(X, Y):
        u = x @ b; u -= u.max(); s -= u[y] - math.log(np.exp(u).sum())
    return s / len(X)


def fit(X, Y):
    b0 = np.zeros(X[0].shape[1]); b0[0] = 1
    return minimize(nll, b0, args=(X, Y), method='L-BFGS-B').x


ds = sorted({d for d, _ in rows}); cut = ds[len(ds) // 2]
tr = [r for r in rows if r[0] < cut]; te = [r for r in rows if r[0] >= cut]
print('races', len(rows), 'train', len(tr), 'test', len(te), 'cut', cut)
for arm in ('book', 'steam', 'all'):
    Xt, Yt = build(tr, arm); Xe, Ye = build(te, arm); b = fit(Xt, Yt)
    acc = np.mean([np.argmax(x @ b) == y for x, y in zip(Xe, Ye)])
    line = f'{arm:6s} test log-loss {nll(b, Xe, Ye):.4f} top-pick win% {acc * 100:.1f} coefs {np.round(b, 3)}'
    for edge in (0.05, 0.15):
        n = ret = 0
        for (_, rr), x, y in zip(te, Xe, Ye):
            u = x @ b; p = np.exp(u - u.max()); p /= p.sum()
            for j, r in enumerate(rr):
                if p[j] * r['bo'] > 1 + edge and r['bo'] <= 10:
                    n += 1; ret += r['bo'] if j == y else 0
        line += f' | EV>{edge:.0%} n={n} ROI={(ret - n) / max(n, 1) * 100:+.1f}%'
    print(line)

random.seed(3)
for name, (a, b_) in {'forward': (tr, te), 'reverse': (te, tr)}.items():
    Xt, Yt = build(a, 'book'); Xe, Ye = build(b_, 'book'); b = fit(Xt, Yt)
    byd = {}
    for (d, rr), x, y in zip(b_, Xe, Ye):
        u = x @ b; p = np.exp(u - u.max()); p /= p.sum()
        for j, r in enumerate(rr):
            if p[j] * r['bo'] > 1.05 and r['bo'] <= 10:
                byd.setdefault(d, []).append(r['bo'] if j == y else 0)
    days = list(byd); st = sum(len(v) for v in byd.values()); rt = sum(sum(v) for v in byd.values())
    bs = []
    for _ in range(2000):
        s = t = 0
        for _ in days:
            v = byd[random.choice(days)]; s += len(v); t += sum(v)
        bs.append((t - s) / s * 100)
    bs.sort()
    print(f'{name:8s}: coef {b[0]:.3f} n={st} ROI={(rt - st) / st * 100:+.1f}% CI[{bs[50]:+.0f},{bs[1950]:+.0f}]')
