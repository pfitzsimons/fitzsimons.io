#!/usr/bin/env python3
"""
What is `current_odds` (odds_dec)? Compare it with the best bookmaker price
(best_odds_dec, captured since 2026-07-17) and SP, split by winners/losers,
and re-settle the model / favourite / Strong Win Bet at each price.
(reads rows.json)

A bettable price should not systematically favour winners; the forecast
price does (winners ~25% bigger than the book price, losers ~3%).

    python3 scripts/analysis/prices.py
"""
import glob, json, os, random, statistics as st
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
rows = json.load(open(os.path.join(HERE, 'rows.json')))

b = [x for _, rr in rows for x in rr if x.get('bo') and x.get('sp')]
for lab, f in [('winners', lambda x: x['won']), ('losers', lambda x: not x['won'])]:
    v = [x for x in b if f(x)]
    print(f'{lab:8s} n={len(v):6d}  mean forecast/book {st.mean(x["od"] / x["bo"] for x in v):.3f}  '
          f'mean book/SP {st.mean(x["bo"] / x["sp"] for x in v):.3f}')

full = [rr for d, rr in rows if d >= '2026-07-17' and all(x.get('bo') for x in rr)]
print('median overround  forecast %.3f  best-book %.3f  SP %.3f' % (
    st.median(sum(1 / x['od'] for x in rr) for rr in full),
    st.median(sum(1 / x['bo'] for x in rr) for rr in full),
    st.median(sum(1 / x['sp'] for x in rr if x['sp']) for rr in full)))

books = Counter()
for f in glob.glob(os.path.join(ROOT, 'horses', 'history', 'races_*.json')):
    for r in json.load(open(f)).get('races', []):
        for x in r['runners']:
            if x.get('priced_books') is not None:
                books[x.get('best_odds_book')] += 1
print('best-price bookmaker:', books.most_common(5))


def settle(pick, price, since=''):
    v = [(d, p['won'], p[price]) for d, rr in rows if d >= since
         for p in [pick(rr)] if p and p.get(price)]
    n = len(v); r = sum(o for _, w, o in v if w)
    byd = {}
    for d, w, o in v:
        byd.setdefault(d, []).append(o if w else 0)
    ds = list(byd); random.seed(0); bs = []
    for _ in range(2000):
        s = t = 0
        for _ in ds:
            x = byd[random.choice(ds)]; s += len(x); t += sum(x)
        bs.append((t - s) / s * 100)
    bs.sort()
    return f'n={n:5d} win%={100 * sum(w for _, w, _ in v) / n:5.1f} ROI={(r - n) / n * 100:+6.1f}% CI[{bs[50]:+.0f},{bs[1950]:+.0f}]'


top = lambda rr: max(rr, key=lambda x: x['score'])
fav = lambda rr: min(rr, key=lambda x: x['od'])
swb = lambda rr: next((x for x in rr if x['rec'] == 'Strong Win Bet'), None)
for nm, f in [('Strong Win Bet', swb), ('model top', top), ('favourite', fav)]:
    for lab, key in (('forecast', 'od'), ('best book', 'bo'), ('SP', 'sp')):
        print(f'{nm:15s} @{lab:9s} (since 07-17) {settle(f, key, "2026-07-17")}')
