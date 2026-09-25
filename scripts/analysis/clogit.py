#!/usr/bin/env python3
"""
Conditional logit: does the model add information on top of the market?
(reads rows.json; needs numpy + scipy — analysis-only, not a pipeline dep)

Fits P(win) ∝ exp(b·x) per race on races before the median date, tests after.
Arms: market only (log implied prob from the forecast price), market + model
score, market + components. Reports test log-loss, a day-block bootstrap CI of
the components-vs-market gain, and value-bet ROI at the forecast price (which
overstates returns — see prices.py / robust.py).

    python3 scripts/analysis/clogit.py
"""
import json, math, os, random
import numpy as np
from scipy.optimize import minimize

HERE = os.path.dirname(os.path.abspath(__file__))
rows = json.load(open(os.path.join(HERE, 'rows.json')))
dates = sorted({d for d, _ in rows}); cut = dates[len(dates) // 2]
FEATS = ['recent_form', 'consistency', 'jockey', 'trainer', 'distance',
         'freshness', 'class_or', 'timeform', 'weight']
KEEP = ['recent_form', 'consistency', 'jockey', 'trainer']


def build(rs, feats, use_score=False, price='od'):
    X, Y = [], []
    for _, rr in rs:
        q = np.array([1 / x[price] for x in rr]); cols = [np.log(q / q.sum())]
        if use_score:
            cols.append(np.array([x['score'] for x in rr]) / 10)
        for f in feats:
            v = np.array([float(x['comp'].get(f) if x['comp'].get(f) is not None else 50)
                          for x in rr]) / 10
            cols.append(v - v.mean())
        X.append(np.stack(cols, 1)); Y.append([x['won'] for x in rr].index(True))
    return X, Y


def nll(b, X, Y, l2=0.0):
    s = 0.0
    for x, y in zip(X, Y):
        u = x @ b; u -= u.max(); s -= u[y] - math.log(np.exp(u).sum())
    return s / len(X) + l2 * np.sum(b[1:] ** 2)


def fit(X, Y, l2=0.001):
    b0 = np.zeros(X[0].shape[1]); b0[0] = 1
    return minimize(nll, b0, args=(X, Y, l2), method='L-BFGS-B').x


if __name__ == '__main__':
    tr = [r for r in rows if r[0] < cut]; te = [r for r in rows if r[0] >= cut]
    print('train races', len(tr), 'test races', len(te), 'split', cut)
    for name, feats, us in [('market only', [], False), ('market+score', [], True),
                            ('market+components', FEATS, False)]:
        Xtr, Ytr = build(tr, feats, us); Xte, Yte = build(te, feats, us)
        b = fit(Xtr, Ytr)
        acc = np.mean([np.argmax(x @ b) == y for x, y in zip(Xte, Yte)])
        print(f'{name:20s} test log-loss {nll(b, Xte, Yte):.4f}  top-pick win% {acc * 100:.1f}  coefs {np.round(b, 3)}')

    random.seed(1)
    per = {}
    for name, f in [('m', []), ('c', KEEP)]:
        Xtr, Ytr = build(tr, f); Xte, Yte = build(te, f)
        b = fit(Xtr, Ytr); ll = []
        for x, y in zip(Xte, Yte):
            u = x @ b; u -= u.max(); ll.append(-(u[y] - math.log(np.exp(u).sum())))
        per[name] = (b, ll, Xte, Yte)
    diff = np.array(per['m'][1]) - np.array(per['c'][1])  # positive = components help
    byday = {}
    for (d, _), v in zip(te, diff):
        byday.setdefault(d, []).append(v)
    ud = list(byday); bs = []
    for _ in range(2000):
        s = []
        for _ in ud:
            s.extend(byday[random.choice(ud)])
        bs.append(np.mean(s))
    print('log-loss gain (market - components) %.4f  95%% CI [%.4f, %.4f]'
          % (diff.mean(), np.percentile(bs, 2.5), np.percentile(bs, 97.5)))

    b, _, Xte, Yte = per['c']
    for edge in (0.0, 0.1, 0.2):
        n = ret = w = 0
        for (_, rr), x, y in zip(te, Xte, Yte):
            u = x @ b; p = np.exp(u - u.max()); p /= p.sum()
            for j, r in enumerate(rr):
                if p[j] * r['od'] > 1 + edge and r['od'] <= 10:
                    n += 1; w += j == y; ret += r['od'] if j == y else 0
        print(f'value bets EV>{edge:.0%} odds<=10 @forecast: n={n} win%={w / max(n, 1) * 100:.1f} ROI={(ret - n) / max(n, 1) * 100:+.1f}%')
