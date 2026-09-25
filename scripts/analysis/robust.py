#!/usr/bin/env python3
"""
Rolling monthly refit of the value-bet strategy, settled at three prices.
(reads rows.json; needs numpy + scipy)

For each month, fit market-only and market+components logits on all earlier
months, back runners with EV > edge at odds <= 10, and settle each bet at the
forecast price, at SP, and at the best bookmaker price. Prints ROI with a
day-block bootstrap 95% CI and the per-month split. This is the check that
showed the forecast-price "profit" vanishes at bettable prices.

    python3 scripts/analysis/robust.py
"""
import random
import numpy as np
from clogit import rows, build, fit, KEEP

months = sorted({d[:7] for d, _ in rows})
bets = {}
for m in months[1:]:
    tr = [r for r in rows if r[0][:7] < m]; te = [r for r in rows if r[0][:7] == m]
    if len(tr) < 800:
        continue
    for arm, f in [('market', []), ('combined', KEEP)]:
        Xtr, Ytr = build(tr, f); Xte, Yte = build(te, f)
        b = fit(Xtr, Ytr)
        if arm == 'combined':
            print('coefs', m, np.round(b, 3))
        for (d, rr), x, y in zip(te, Xte, Yte):
            u = x @ b; p = np.exp(u - u.max()); p /= p.sum()
            for j, r in enumerate(rr):
                for edge in (0.0, 0.1, 0.2):
                    if p[j] * r['od'] > 1 + edge and r['od'] <= 10:
                        for lab, key in (('forecast', 'od'), ('SP', 'sp'), ('bestbook', 'bo')):
                            if r.get(key):
                                bets.setdefault((arm, edge, lab), []).append((d, m, j == y, r[key]))

random.seed(2)
for k, v in sorted(bets.items()):
    n = len(v); ret = sum(o for _, _, w, o in v if w)
    byd = {}
    for d, _, w, o in v:
        byd.setdefault(d, []).append(o if w else 0)
    ds = list(byd); bs = []
    for _ in range(2000):
        st = rt = 0
        for _ in ds:
            x = byd[random.choice(ds)]; st += len(x); rt += sum(x)
        bs.append((rt - st) / st * 100)
    bym = {}
    for _, m, w, o in v:
        bym.setdefault(m, [0, 0]); bym[m][0] += 1; bym[m][1] += o if w else 0
    mstr = ' '.join(f'{m[5:]}:{(r - s) / s * 100:+.0f}%({s})' for m, (s, r) in sorted(bym.items()))
    print(f'{k[0]:8s} EV>{k[1]:.0%} @{k[2]:8s}: n={n} ROI={(ret - n) / n * 100:+.1f}% '
          f'CI[{np.percentile(bs, 2.5):+.1f},{np.percentile(bs, 97.5):+.1f}] | {mstr}')
