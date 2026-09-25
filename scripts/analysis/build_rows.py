#!/usr/bin/env python3
"""
Build the runner-level dataset every other script in scripts/analysis/ reads.

Re-scores the whole archive walk-forward with the CURRENT production model
(backtest_value.score_and_recommend, same no-leakage StrikeTable roll as
calibrate.py --rescore, burn-in 15 days) and writes one row per runner:

    score  – model score        od – stored forecast price (current_odds)
    sp     – starting price     bo – best bookmaker price (from 2026-07-17)
    won    – finished 1st       comp – score components   rec – rec label

Only races with >=2 priced runners and exactly one matched winner are kept.
Output: scripts/analysis/rows.json (gitignored). Also prints the forecast
price vs SP comparison and the headline model / favourite / Strong Win Bet
hit-rates and ROI at forecast price and at SP.

    python3 scripts/analysis/build_rows.py
"""
import json, os, statistics, sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(ROOT, 'scripts'))
import strike_rates as sr, backtest_value as bv, scrape_races as s

BURN_IN = 15

# Extend the outcome join so each runner also carries its SP from results_full.
_orig = sr._outcome
def _outcome(horse, result, ewp):
    oc = _orig(horse, result, ewp)
    if oc is None:
        return oc
    key = sr.fr.normalise_name(horse)
    runners = result.get('runners', [])
    names = [sr.fr.normalise_name(r.get('name', '')) for r in runners]
    # Exact name first; substring only as a fallback, so "Sea" can't take
    # "Sea Legend"'s SP.
    hit = next((r for r, rn in zip(runners, names) if rn == key), None) or \
          next((r for r, rn in zip(runners, names) if key in rn or rn in key), None)
    if hit:
        oc['sp'] = s.parse_odds(hit.get('odds') or '')
    return oc
sr._outcome = _outcome

days = sr.iter_history(os.path.join(ROOT, 'horses', 'history'))
table = sr.StrikeTable()
rows = []
for i, (d, praces) in enumerate(days):
    if i >= BURN_IN:
        for pr in praces:
            rr = []
            for r in bv.score_and_recommend(pr, True, table):
                oc, od = r.get('_oc'), r.get('odds_dec')
                if not oc or oc['status'] == 'non_runner' or not od or od <= 1:
                    continue
                rr.append(dict(score=r['_score'], od=od, sp=oc.get('sp'),
                               won=oc['status'] == 'finished' and oc['pos'] == 1,
                               comp=r.get('_components'),
                               rec=r['recommendation'].get('label'),
                               bo=r.get('best_odds_dec')))
            if len(rr) >= 2 and sum(x['won'] for x in rr) == 1:
                rows.append((d, rr))
    for pr in praces:
        table.add_race(pr)

json.dump(rows, open(os.path.join(HERE, 'rows.json'), 'w'), default=str)

allr = [x for _, rr in rows for x in rr]
print(f'{len(days) - BURN_IN} days · {len(rows)} races · {len(allr)} runners')
hs = [x for x in allr if x['sp']]
print('forecast == SP %.1f%%' % (100 * sum(abs(x['od'] - x['sp']) < 1e-6 for x in hs) / len(hs)))
for lab, f in [('winners', lambda x: x['won']), ('losers', lambda x: not x['won'])]:
    v = [x['od'] / x['sp'] for x in hs if f(x)]
    print(f'  {lab}: mean forecast/SP {statistics.mean(v):.3f}')

def roi(pick, price):
    n = ret = w = 0
    for _, rr in rows:
        p = pick(rr)
        if not p or not p[price]:
            continue
        n += 1; w += p['won']; ret += p[price] if p['won'] else 0
    return f'n={n} win%={w / n * 100:.1f} ROI={(ret - n) / n * 100:+.1f}%'

top = lambda rr: max(rr, key=lambda x: x['score'])
fav = lambda rr: min(rr, key=lambda x: x['od'])
swb = lambda rr: next((x for x in rr if x['rec'] == 'Strong Win Bet'), None)
for nm, f in [('model top', top), ('favourite', fav), ('Strong Win Bet', swb)]:
    print(f'{nm:15s} @forecast: {roi(f, "od")}   @SP: {roi(f, "sp")}')
