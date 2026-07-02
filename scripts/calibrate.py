#!/usr/bin/env python3
"""
Calibration & ROI analysis — is the prediction score honest, and does it pay?

Joins every archived prediction (horses/history/races_*.json) with the FULL
finishing order (re-fetched via the same race API fetch_results.py uses) and
reports three things:

  1. Calibration curve — for horses the model scored in each band, how often
     did they actually WIN and PLACE? A well-calibrated score rises smoothly.
  2. Flat-stake ROI per band — profit/loss from blindly backing every runner
     in a band £1 to win (and £1 to place). Calibration says the ranking is
     right; ROI says whether the market has already priced it in.
  3. Recommendation hit-rates & ROI — how each recommendation label actually
     performed on primary picks, as strike-rate AND return per £1 staked.

Betting assumptions (see --place-fraction):
  • Win bet: £1 → profit (odds-1) if won, else -£1.
  • Each-Way bet: £1 total = £0.50 win + £0.50 place; place part pays at
    `place_fraction` of the odds (default 1/5). Non-runners are treated as
    void (no bet); DNFs lose the stake.

Results are cached per-date so re-runs are cheap:
    python3 scripts/calibrate.py --out horses --cache /tmp/cal_cache

This is an offline analysis tool — it does not touch accuracy.json or the site.
"""

import argparse
import glob
import json
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_results as fr  # reuse the full-field fetcher + matching


def load_results_for_date(date_str: str, cache_dir: str) -> list:
    """Full-field results for a date, cached to disk to avoid re-fetching."""
    os.makedirs(cache_dir, exist_ok=True)
    cache = os.path.join(cache_dir, f'results_full_{date_str}.json')
    if os.path.exists(cache):
        with open(cache, encoding='utf-8') as f:
            return json.load(f)
    results = fr.fetch_sl_results(date_str)
    with open(cache, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False)
    return results


def horse_outcome(pred_name: str, result: dict):
    """Return ('finished', pos) | ('dnf', None) | ('non_runner', None) | None."""
    key = fr.normalise_name(pred_name)
    for r in result.get('runners', []):
        rn = fr.normalise_name(r.get('name', ''))
        if key == rn or key in rn or rn in key:
            status = r.get('status', 'finished')
            if status == 'finished':
                return ('finished', r.get('position'))
            return (status, None)
    return None


def bucket(score: float) -> str:
    lo = int(score // 10) * 10
    return f'{lo:>3}-{lo + 9}'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', default='horses', help='Directory holding history/')
    ap.add_argument('--cache', default='', help='Results cache dir (default: <out>/.cal_cache)')
    ap.add_argument('--min-date', default='', help='Only dates >= this (YYYY-MM-DD)')
    ap.add_argument('--place-fraction', type=float, default=0.2,
                    help='Place-part payout as a fraction of odds (default 1/5)')
    args = ap.parse_args()
    pf_frac = args.place_fraction

    out_dir = os.path.abspath(args.out)
    cache_dir = os.path.abspath(args.cache) if args.cache else os.path.join(out_dir, '.cal_cache')

    pred_files = sorted(glob.glob(os.path.join(out_dir, 'history', 'races_*.json')))
    if args.min_date:
        pred_files = [p for p in pred_files if os.path.basename(p)[6:16] >= args.min_date]
    if not pred_files:
        print('No prediction archives found', file=sys.stderr)
        return

    def new_band():
        return {'n': 0, 'win': 0, 'place': 0, 'dnf': 0, 'nr': 0, 'unmatched': 0,
                'win_stake': 0.0, 'win_ret': 0.0, 'plc_stake': 0.0, 'plc_ret': 0.0}

    def new_rec():
        return {'n': 0, 'win': 0, 'place': 0, 'dnf': 0, 'stake': 0.0, 'ret': 0.0}

    bands = defaultdict(new_band)   # score band -> counts + flat-stake ROI
    recs = defaultdict(new_rec)     # rec label -> counts + tier-appropriate ROI
    n_dates = n_races_matched = n_races_total = 0

    for pf in pred_files:
        with open(pf, encoding='utf-8') as f:
            pred = json.load(f)
        date_str = pred.get('date', '')
        if not date_str:
            continue
        n_dates += 1
        print(f'  {date_str} …', file=sys.stderr)
        results = load_results_for_date(date_str, cache_dir)

        for prace in pred.get('races', []):
            n_races_total += 1
            result = fr.match_race(prace, results)
            if not result:
                continue
            n_races_matched += 1
            ewp = prace.get('ew_places', 3)
            primary_seen = set()

            for runner in prace.get('runners', []):
                score = runner.get('score')
                if score is None:
                    continue
                od = runner.get('odds_dec')
                oc = horse_outcome(runner.get('horse', ''), result)
                b = bands[bucket(score)]
                b['n'] += 1
                if oc is None:
                    b['unmatched'] += 1
                    continue
                kind, pos = oc
                won = kind == 'finished' and pos == 1
                placed = kind == 'finished' and pos is not None and pos <= ewp
                if kind == 'dnf':
                    b['dnf'] += 1
                elif kind == 'non_runner':
                    b['nr'] += 1
                if won:
                    b['win'] += 1
                if placed:
                    b['place'] += 1

                # Flat-stake ROI for the band (non-runners are void → no bet).
                if od and kind != 'non_runner':
                    b['win_stake'] += 1.0
                    b['win_ret'] += od if won else 0.0
                    b['plc_stake'] += 1.0
                    b['plc_ret'] += (1 + (od - 1) * pf_frac) if placed else 0.0

                # Recommendation hit-rate + ROI — primary (top-scored) pick per type.
                rec = runner.get('recommendation', {})
                rtype, label = rec.get('type'), rec.get('label', '')
                if rtype in ('Win', 'EachWay') and rtype not in primary_seen and kind != 'non_runner':
                    primary_seen.add(rtype)
                    rr = recs[label or rtype]
                    rr['n'] += 1
                    if won:
                        rr['win'] += 1
                    if placed:
                        rr['place'] += 1
                    if kind == 'dnf':
                        rr['dnf'] += 1
                    if od:
                        rr['stake'] += 1.0
                        if rtype == 'Win':
                            rr['ret'] += od if won else 0.0
                        else:  # Each-Way: £0.50 win + £0.50 place
                            rr['ret'] += (0.5 * od if won else 0.0)
                            rr['ret'] += (0.5 * (1 + (od - 1) * pf_frac)) if placed else 0.0

    def pct(a, b):
        return f'{a / b * 100:5.1f}%' if b else '   — '

    def roi(ret, stake):
        return f'{(ret - stake) / stake * 100:+6.1f}%' if stake else '    — '

    print('\n================  CALIBRATION CURVE + FLAT-STAKE ROI  ================')
    print(f'{n_dates} days · {n_races_matched}/{n_races_total} races matched · '
          f'place-fraction 1/{round(1 / pf_frac)}\n')
    print(f'{"score band":>10} | {"n":>6} | {"win%":>6} | {"place%":>7} '
          f'| {"win ROI":>8} | {"place ROI":>9} | {"dnf":>4} | {"nr":>4}')
    print('-' * 82)
    for band in sorted(bands):
        c = bands[band]
        ran = c['n'] - c['nr'] - c['unmatched']
        print(f'{band:>10} | {c["n"]:>6} | {pct(c["win"], ran)} | {pct(c["place"], ran)} '
              f'| {roi(c["win_ret"], c["win_stake"])} | {roi(c["plc_ret"], c["plc_stake"])} '
              f'| {c["dnf"]:>4} | {c["nr"]:>4}')

    print('\n================  RECOMMENDATION HIT-RATES + ROI  (primary picks)  ============')
    print(f'{"label":>26} | {"n":>5} | {"win%":>6} | {"place%":>7} | {"ROI/£1":>8} | {"dnf":>4}')
    print('-' * 78)
    order = ['Strong Win Bet', 'Win Bet', 'Each Way', 'Each Way (Speculative)']
    for label in order + [l for l in recs if l not in order]:
        if label not in recs:
            continue
        c = recs[label]
        print(f'{label:>26} | {c["n"]:>5} | {pct(c["win"], c["n"])} | {pct(c["place"], c["n"])} '
              f'| {roi(c["ret"], c["stake"])} | {c["dnf"]:>4}')


if __name__ == '__main__':
    main()
