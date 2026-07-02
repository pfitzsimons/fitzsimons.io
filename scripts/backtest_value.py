#!/usr/bin/env python3
"""
Backtest: does EV-gated *value* betting beat the current model on ROI?

This documents why value betting was rejected (see the commit "Track ROI, and
add a model-vs-market readout"). It reconstructs recommendations from the
stored per-runner score + odds in the archived predictions, applies the
current make_recommendation, and compares flat-stake ROI against the OLD
stored recommendations — then, with --value-gate, against an EV-gated variant.

Data source: horses/history/races_<date>.json (predictions) joined with
horses/history/results_full_<date>.json (full finishing order, written by
fetch_results.py). Only dates present in BOTH are used. Same period the model
was calibrated on, so treat as an in-sample sanity check, not proof.

    python3 scripts/backtest_value.py                # current model vs old
    python3 scripts/backtest_value.py --value-gate   # + EV-gated variant
"""

import argparse
import copy
import glob
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scrape_races as s
import fetch_results as fr

EV_MARGIN = 0.05  # edge required by the (rejected) value-gate variant


def outcome_for(horse, result, ewp, rec_type):
    key = fr.normalise_name(horse)
    oc = None
    for r in result.get('runners', []):
        rn = fr.normalise_name(r.get('name', ''))
        if key == rn or key in rn or rn in key:
            oc = r
            break
    if oc is None or oc.get('status') == 'non_runner':
        return None
    if oc.get('status') == 'dnf':
        return 'incorrect'
    pos = oc.get('position')
    if rec_type == 'Win':
        return 'correct' if pos == 1 else 'incorrect'
    if pos == 1:
        return 'ew_win'
    if pos <= ewp:
        return 'ew_placed'
    return 'incorrect'


def tally(pnl, rec_type, oc, od):
    pl = fr.bet_pnl(rec_type, oc, od)
    if pl:
        k = 'win' if rec_type == 'Win' else 'ew'
        pnl[k]['stake'] += pl[0]
        pnl[k]['ret'] += pl[1]


def blank():
    return {'win': {'stake': 0.0, 'ret': 0.0}, 'ew': {'stake': 0.0, 'ret': 0.0}}


def primaries(runners):
    """Yield (rec_type, runner) for the top-scored Win and EW pick."""
    seen = set()
    for r in runners:
        rt = r['recommendation']['type']
        if rt in ('Win', 'EachWay') and rt not in seen:
            seen.add(rt)
            yield rt, r


def run(out_dir, value_gate):
    hist = os.path.join(out_dir, 'history')
    old, new, gated = blank(), blank(), blank()

    for pf in sorted(glob.glob(os.path.join(hist, 'races_*.json'))):
        pred = json.load(open(pf))
        date = pred.get('date', '')
        rf = os.path.join(hist, f'results_full_{date}.json')
        if not os.path.exists(rf):
            continue
        raw = json.load(open(rf))
        results = raw.get('races', []) if isinstance(raw, dict) else raw

        for prace in pred.get('races', []):
            result = fr.match_race(prace, results)
            if not result:
                continue
            ewp = prace.get('ew_places', 3)
            n = prace.get('num_runners') or len(prace.get('runners', []))

            # OLD: stored recommendations
            for rt, runner in primaries([
                    r for r in prace.get('runners', [])
                    if r.get('recommendation')]):
                oc = outcome_for(runner['horse'], result, ewp, rt)
                if oc:
                    tally(old, rt, oc, runner.get('odds_dec'))

            # Recompute value metrics + current recommendations from stored score
            rs = copy.deepcopy(prace.get('runners', []))
            for r in rs:
                r['_score'] = r.get('score', 0)
            s.compute_value(rs)
            rs.sort(key=lambda r: r['_score'], reverse=True)
            for variant, pnl, gate in (('new', new, False), ('gate', gated, True)):
                if variant == 'gate' and not value_gate:
                    continue
                rv = copy.deepcopy(rs)
                for r in rv:
                    form = s.parse_form(r.get('form', ''))
                    rec = s.make_recommendation(r['_score'], r.get('odds_dec'), n, form)
                    if gate and rec['type'] == 'Win':
                        mp = s.score_to_winprob(r['_score'])  # non-normalised proxy
                        od = r.get('odds_dec')
                        ev = (mp * od - 1) if od else None
                        if ev is None or ev < EV_MARGIN:
                            rec = {'type': 'Skip'}
                    r['recommendation'] = rec
                s._post_process_win_bets(rv, n)
                for rt, runner in primaries(rv):
                    oc = outcome_for(runner['horse'], result, ewp, rt)
                    if oc:
                        tally(pnl, rt, oc, runner.get('odds_dec'))

    def roi(b):
        st = b['stake']
        return (b['ret'] - st) / st * 100 if st else 0.0

    def show(tag, p):
        allst = p['win']['stake'] + p['ew']['stake']
        allrt = p['win']['ret'] + p['ew']['ret']
        ar = (allrt - allst) / allst * 100 if allst else 0.0
        print(f'{tag:12} | Win {int(p["win"]["stake"]):4} ROI {roi(p["win"]):+6.1f}% '
              f'| EW {int(p["ew"]["stake"]):4} ROI {roi(p["ew"]):+6.1f}% '
              f'| overall {int(allst):4} ROI {ar:+6.1f}%')

    print('Flat £1 stakes (place 1/5):\n')
    show('OLD stored', old)
    show('CURRENT', new)
    if value_gate:
        show('VALUE-GATED', gated)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', default='horses', help='Directory holding history/')
    ap.add_argument('--value-gate', action='store_true',
                    help='Also show the (rejected) EV-gated variant')
    args = ap.parse_args()
    run(os.path.abspath(args.out), args.value_gate)


if __name__ == '__main__':
    main()
