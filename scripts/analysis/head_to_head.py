#!/usr/bin/env python3
"""
Model vs market, over every race (reads rows.json from build_rows.py).

  • model top pick vs forecast favourite, overall and where they disagree
  • multinomial log-loss of the market's implied probabilities (overround
    removed) vs a softmax of the model score at several temperatures

    python3 scripts/analysis/head_to_head.py
"""
import json, math, os

HERE = os.path.dirname(os.path.abspath(__file__))
rows = json.load(open(os.path.join(HERE, 'rows.json')))

top = lambda rr: max(rr, key=lambda x: x['score'])
fav = lambda rr: min(rr, key=lambda x: x['od'])

def stats(races, pick):
    n = w = 0; ret = 0.0
    for _, rr in races:
        p = pick(rr); n += 1; w += p['won']; ret += p['od'] if p['won'] else 0
    return f'n={n} win%={w / n * 100:.1f} ROI={(ret - n) / n * 100:+.1f}%'

print('model top :', stats(rows, top))
print('favourite :', stats(rows, fav))
dis = [(d, rr) for d, rr in rows if top(rr) is not fav(rr)]
print(f'disagree in {len(dis) / len(rows) * 100:.1f}% of races')
print('  model (disagree):', stats(dis, top))
print('  fav   (disagree):', stats(dis, fav))

def logloss(probfn):
    s = 0.0
    for _, rr in rows:
        p = probfn(rr)
        s -= math.log(max(1e-9, p[[x['won'] for x in rr].index(True)]))
    return s / len(rows)

norm = lambda v: [x / sum(v) for x in v]
print('log-loss market  %.4f' % logloss(lambda rr: norm([1 / x['od'] for x in rr])))
print('log-loss uniform %.4f' % logloss(lambda rr: [1 / len(rr)] * len(rr)))
for T in (5, 10, 15, 20):
    print('log-loss model softmax T=%-2d %.4f'
          % (T, logloss(lambda rr, T=T: norm([math.exp(x['score'] / T) for x in rr]))))
