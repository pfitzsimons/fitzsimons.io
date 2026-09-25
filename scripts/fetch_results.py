#!/usr/bin/env python3
"""
Results fetcher — UK & Ireland Horse Racing
Scrapes yesterday's actual results from Sporting Life,
compares them to our saved predictions, and writes accuracy.json.

Run after scrape_races.py each morning:
    python3 scripts/fetch_results.py --out /path/to/horses
"""

import argparse
import glob
import gzip
import html
import json
import os
import random
import re
import sys
import time
import urllib.request
import urllib.error
from datetime import date, timedelta, datetime, time as dt_time, timezone
from zoneinfo import ZoneInfo

# Sporting Life returns race off-times in UTC; the site displays them
# unconverted, which is an hour out during British/Irish Summer Time.
# UK and Ireland share the same clock (both UTC+1 in summer), so a
# single Europe/London conversion is correct for both. Predictions in
# races.json are stored in local time (see scrape_races.py), so results
# must match to stay comparable.
UK_IRE_TZ = ZoneInfo("Europe/London")


def to_local_time(time_str: str, date_str: str) -> str:
    """Convert an 'HH:MM' UTC off-time to local UK/IRE wall-clock time."""
    m = re.match(r"^(\d{1,2}):(\d{2})$", (time_str or "").strip())
    if not m:
        return time_str
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return time_str
    utc_dt = datetime.combine(d, dt_time(int(m.group(1)), int(m.group(2))), tzinfo=timezone.utc)
    local_dt = utc_dt.astimezone(UK_IRE_TZ)
    return local_dt.strftime("%H:%M")

UK_IRE_COUNTRIES = {
    'england', 'scotland', 'wales', 'ireland', 'eire',
    'northern ireland', 'republic of ireland'
}

# Each-Way place-part payout as a fraction of the win odds. 1/5 is the most
# common UK term; used for ROI accounting (not for accuracy classification).
EW_PLACE_FRACTION = 0.2

BASE_URL = 'https://www.sportinglife.com'
# Per-race result API — returns the FULL finishing order (every runner,
# non-runner and casualty), not just the top 3-4 placed horses the
# results-index page exposes. This is what makes losing bets countable.
RACE_API = BASE_URL + '/api/horse-racing/race/{id}'

# Casualty reasons Sporting Life reports for horses that started but did
# not complete (finish_position == 0 while ride_status == RUNNER).
DNF_REASONS = {
    'PulledUp', 'Fell', 'UnseatedRider', 'BroughtDown',
    'RanOut', 'RefusedToRace', 'Refused', 'SlippedUp', 'Carried Out',
}

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/124.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-GB,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}


def log(msg):
    print(f'  {msg}', file=sys.stderr)


def fetch(url, accept=None, retries=3):
    headers = dict(HEADERS)
    if accept:
        headers['Accept'] = accept
    req = urllib.request.Request(url, headers=headers)
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                if resp.headers.get('Content-Encoding', '') == 'gzip':
                    raw = gzip.decompress(raw)
                return raw.decode('utf-8', errors='replace')
        except urllib.error.HTTPError as e:
            log(f'HTTP {e.code} -> {url} (attempt {attempt}/{retries})')
            if e.code not in (429, 500, 502, 503, 504) or attempt == retries:
                return None
        except Exception as e:
            log(f'{type(e).__name__} -> {url}: {e} (attempt {attempt}/{retries})')
            if attempt == retries:
                return None
        time.sleep(random.uniform(1.5, 3.0) * attempt)
    return None


def fetch_json(url):
    body = fetch(url, accept='application/json')
    if not body:
        return None
    try:
        return json.loads(body)
    except json.JSONDecodeError as e:
        log(f'JSON parse error for {url}: {e}')
        return None


def normalise_name(name: str) -> str:
    """Normalise horse name for fuzzy matching — uppercase, strip punctuation.

    Predictions can carry HTML entities (e.g. "D&#39;ALBONI") that the scraper
    left unescaped, so decode those first or apostrophe'd names never match.
    """
    name = html.unescape(name or '')
    name = name.upper().strip()
    name = re.sub(r"['\-\s]+", ' ', name)
    name = re.sub(r'[^A-Z0-9 ]', '', name)
    return name.strip()


def _next_data(body: str) -> dict | None:
    """Extract the __NEXT_DATA__ JSON blob from a Sporting Life HTML page."""
    m = re.search(
        r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', body, re.DOTALL
    )
    if not m:
        # Fallback for older markup — grab the first props script.
        m = re.search(r'<script[^>]*>(\{"props":.+?})</script>', body, re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError as e:
        log(f'JSON parse error: {e}')
        return None


def fetch_result_index(date_str: str) -> list:
    """
    Fetch the UK/IRE race stubs for a date from the results index page.
    Returns list of {course, time, race_name, date, race_id, declared_runners}.
    The index only lists the top 3-4 placed horses, so we use it purely to
    discover which races ran — full fields come from fetch_race_result().
    """
    url = f'{BASE_URL}/racing/results/{date_str}'
    log(f'Fetching results index: {url}')
    body = fetch(url)
    if not body:
        return []

    data = _next_data(body)
    if not data:
        log('Could not find result data in Sporting Life page')
        return []

    meetings = data.get('props', {}).get('pageProps', {}).get('meetings', [])
    stubs = []
    for mtg in meetings:
        summary = mtg.get('meeting_summary', {})
        course_obj = summary.get('course', {})
        course = course_obj.get('name', '')
        country = course_obj.get('country', {}).get('long_name', '').lower()

        # UK & Ireland only
        if not any(c in country for c in UK_IRE_COUNTRIES):
            continue

        for race in mtg.get('races', []):
            race_id = race.get('race_summary_reference', {}).get('id')
            if not race_id:
                continue
            stubs.append({
                'course':            course,
                'time':              to_local_time(race.get('time', ''), date_str),
                'race_name':         race.get('name', ''),
                'date':              date_str,
                'race_id':           race_id,
                'declared_runners':  race.get('ride_count', 0),
            })

    log(f'Found {len(stubs)} UK/IRE races in results index')
    return stubs


def classify_ride(ride: dict) -> dict:
    """
    Turn one Sporting Life result ride into a normalised runner record.

    status is one of:
      finished    — completed the race (position >= 1)
      dnf         — started but did not finish (pulled up, fell, etc.)
      non_runner  — declared but withdrawn / did not start
    """
    horse = ride.get('horse', {})
    ride_status = (ride.get('ride_status') or '').upper()
    position = ride.get('finish_position') or 0
    casualty = (ride.get('casualty') or {}).get('reason', '')
    betting = ride.get('betting') or {}

    if ride_status in ('NONRUNNER', 'WITHDRAWN') or (position == 0 and not casualty and ride_status != 'RUNNER'):
        status = 'non_runner'
    elif position >= 1:
        status = 'finished'
    else:
        # Ran but no finishing position — a casualty (PU/F/UR/BD/RO…).
        status = 'dnf'

    return {
        'name':      horse.get('name', ''),
        'position':  position,
        'status':    status,
        'casualty':  casualty,
        'odds':      betting.get('current_odds', '') or '',
        'favourite': bool(betting.get('favourite')),
    }


def fetch_race_result(stub: dict) -> dict | None:
    """
    Fetch the full finishing order for one race from the race API and merge
    it onto the stub. Returns None if the race has no usable result yet.
    """
    url = RACE_API.format(id=stub['race_id'])
    data = fetch_json(url)
    if not data:
        return None

    rides = data.get('rides') or []
    runners = [classify_ride(r) for r in rides if (r.get('horse') or {}).get('name')]
    if not runners:
        return None

    # Finishers first (by position), then DNFs/non-runners.
    runners.sort(key=lambda r: r['position'] if r['position'] >= 1 else 9999)

    return {
        'course':    stub['course'],
        'time':      stub['time'],
        'race_name': stub['race_name'],
        'date':      stub['date'],
        'runners':   runners,
    }


def fetch_sl_results(date_str: str) -> list:
    """
    Fetch full-field results for every UK/IRE race on a date.

    Returns a list of race dicts, each with the COMPLETE finishing order:
      { course, time, race_name, date,
        runners: [{name, position, status, casualty, odds, favourite}] }
    """
    stubs = fetch_result_index(date_str)
    results = []
    for i, stub in enumerate(stubs):
        if i > 0:
            time.sleep(random.uniform(0.8, 1.6))
        race = fetch_race_result(stub)
        if race:
            results.append(race)
        else:
            log(f'No result yet for {stub["time"]} {stub["course"]} '
                f'(id {stub["race_id"]})')

    log(f'Fetched full fields for {len(results)}/{len(stubs)} UK/IRE races')
    return results


def match_race(pred_race: dict, result_races: list) -> dict | None:
    """
    Match a predicted race to a result race by course + off-time.

    Courses stage several races a day, so we pick the CLOSEST time match at
    the course rather than the first within a wide window — an earlier
    version's 70-minute window silently matched the wrong race, so no horse
    names lined up and every pick was scored as a non-runner. A small
    tolerance still absorbs races that go off a few minutes late.
    """
    pred_course = normalise_name(pred_race.get('course', ''))
    pred_time   = pred_race.get('time', '')
    try:
        ph, pm = map(int, pred_time.split(':'))
        pred_mins = ph * 60 + pm
    except Exception:
        return None

    best, best_delta = None, None
    for res in result_races:
        res_course = normalise_name(res.get('course', ''))

        # Course match (allow partial — e.g. "Stratford On Avon" vs "Stratford")
        if pred_course not in res_course and res_course not in pred_course:
            continue

        try:
            rh, rm = map(int, res.get('time', '').split(':'))
        except Exception:
            continue
        delta = abs(pred_mins - (rh * 60 + rm))
        if delta <= 15 and (best_delta is None or delta < best_delta):
            best, best_delta = res, delta

    return best


def match_runner(horse: str, result: dict) -> dict | None:
    """Find a horse in a result's full field. Exact name first; substring only
    as a fallback, so a short name can't pick up a longer one's result
    ("Sea" vs "Sea Legend")."""
    key = normalise_name(horse)
    field = [(normalise_name(r.get('name', '')), r) for r in result.get('runners', [])]
    return next((r for n, r in field if n == key), None) or \
           next((r for n, r in field if key in n or n in key), None)


def evaluate_prediction(runner: dict, result: dict, ew_places: int) -> dict:
    """
    Compare a single runner's prediction to the actual result.
    Returns an outcome dict.

    Because `result` now carries the COMPLETE finishing order, a horse that
    is not in the placed positions is a genuine LOSER (finished out of the
    places), not an assumed non-runner. That is the fix that makes losing
    Win/Each-Way bets countable instead of silently dropped.

    Outcomes:
      correct     — Win pick won
      incorrect   — pick lost (finished out of the places)
      ew_win      — Each-Way pick won outright
      ew_placed   — Each-Way pick placed (within EW terms) but did not win
      non_runner  — horse withdrawn / did not start (excluded from accuracy)
      unmatched   — horse not found in a full result (name mismatch; excluded)
      no_result   — race abandoned or result unavailable (excluded)

    A DNF (pulled up / fell / unseated) is scored as `incorrect`: the horse
    started, so the bet lost. The result carries `dnf`/`casualty` flags for
    the UI to distinguish "beaten" from "did not complete".
    """
    pred_name = normalise_name(runner.get('horse', ''))
    rec_type  = runner.get('recommendation', {}).get('type', 'Skip')

    # A complete result has a winner (position 1). If none, the race was
    # abandoned/void or the result has not been published.
    has_winner = any(r.get('position') == 1 for r in result.get('runners', []))
    if not has_winner:
        return {'rec': rec_type, 'actual_pos': None, 'outcome': 'no_result'}

    # Locate this horse in the full field.
    matched = match_runner(runner.get('horse', ''), result)

    if matched is None:
        # Full field is known but the horse isn't in it — almost always a
        # name-normalisation mismatch. Excluded and surfaced for auditing
        # rather than silently scored as a loss or a win.
        return {'rec': rec_type, 'actual_pos': None, 'outcome': 'unmatched'}

    status = matched.get('status', 'finished')
    sp = parse_sp(matched.get('odds'))
    if status == 'non_runner':
        return {'rec': rec_type, 'actual_pos': None, 'outcome': 'non_runner'}
    if status == 'dnf':
        # Started but did not finish — the bet lost. Skip picks (informational)
        # count as "correct" since the horse did not win.
        outcome = 'correct' if rec_type == 'Skip' else 'incorrect'
        return {'rec': rec_type, 'actual_pos': None, 'outcome': outcome,
                'dnf': True, 'casualty': matched.get('casualty', ''), 'sp': sp}

    actual_pos = matched.get('position')

    if rec_type == 'Skip':
        # Skip predictions are informational — "correct" == did not win.
        outcome = 'correct' if actual_pos != 1 else 'incorrect'
    elif rec_type == 'Win':
        outcome = 'correct' if actual_pos == 1 else 'incorrect'
    elif rec_type == 'EachWay':
        if actual_pos == 1:
            outcome = 'ew_win'
        elif actual_pos <= ew_places:
            outcome = 'ew_placed'
        else:
            outcome = 'incorrect'
    else:
        outcome = 'skip'

    return {'rec': rec_type, 'actual_pos': actual_pos, 'outcome': outcome, 'sp': sp}


def load_predictions(out_dir: str, date_str: str) -> dict | None:
    """Load saved predictions for a given date."""
    path = os.path.join(out_dir, 'history', f'races_{date_str}.json')
    if os.path.exists(path):
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    # Fallback: check if races.json has that date
    path2 = os.path.join(out_dir, 'races.json')
    if os.path.exists(path2):
        with open(path2, encoding='utf-8') as f:
            d = json.load(f)
            if d.get('date') == date_str:
                return d
    return None


def parse_sp(raw) -> float | None:
    """Decimal price from a result's SP string ('11/8', 'EVS', '4.5')."""
    raw = (raw or '').strip().replace(',', '')
    if raw.upper() in ('EVS', 'EVENS'):
        return 2.0
    m = re.match(r'^(\d+)/(\d+)$', raw)
    if m:
        return round(int(m.group(1)) / int(m.group(2)) + 1, 2)
    try:
        v = float(raw)
    except ValueError:
        return None
    return v if v > 1 else None


def settle_price(runner: dict, outcome: dict) -> tuple:
    """(price, basis) a bet is settled at: a price you could actually get.

    `odds_dec` (Sporting Life `current_odds`) is the overnight betting
    forecast, not a bookmaker price — it runs ~25% long on winners, so
    settling there flatters ROI. Use the best bookmaker price captured in the
    start-of-day scrape ('book'); before that capture existed (2026-07-17)
    or when no book priced the horse, fall back to SP ('sp').
    """
    if runner.get('best_odds_dec') and runner['best_odds_dec'] > 1:
        return runner['best_odds_dec'], 'book'
    if outcome.get('sp'):
        return outcome['sp'], 'sp'
    return None, None


def bet_pnl(rec_type: str, outcome: str, odds_dec) -> tuple | None:
    """
    Return (stake, ret) for a £1 bet on a primary pick, or None if the bet
    can't be priced (no odds). Profit is ret - stake.

    Win bet: £1 → odds_dec back if it won, else 0.
    Each-Way: £1 = £0.50 win + £0.50 place; the place half pays at
    EW_PLACE_FRACTION of the odds. A DNF simply loses (won/placed are False).
    """
    if not odds_dec or odds_dec <= 1:
        return None
    if rec_type == 'Win':
        won = outcome == 'correct'
        return (1.0, odds_dec if won else 0.0)
    # Each-Way
    won    = outcome == 'ew_win'
    placed = outcome in ('ew_win', 'ew_placed')
    ret = 0.0
    if won:
        ret += 0.5 * odds_dec
    if placed:
        ret += 0.5 * (1 + (odds_dec - 1) * EW_PLACE_FRACTION)
    return (1.0, ret)


def favourite_bet(pred_race: dict, result: dict | None) -> tuple | None:
    """(stake, return, horse) for £1 to win on the morning favourite.

    The favourite is the shortest forecast price among runners not already
    withdrawn at the start-of-day scrape — known before the off, so this is
    a fair baseline. Settled like our own picks (settle_price). None when
    the race has no result or the favourite didn't run / can't be priced.
    """
    if result is None:
        return None
    field = [r for r in pred_race.get('runners', [])
             if not r.get('non_runner') and (r.get('odds_dec') or 0) > 1]
    if not field:
        return None
    runner = min(field, key=lambda r: r['odds_dec'])
    oc = evaluate_prediction({**runner, 'recommendation': {'type': 'Win'}}, result, 0)
    if oc['outcome'] not in ('correct', 'incorrect'):
        return None
    pl = bet_pnl('Win', oc['outcome'], settle_price(runner, oc)[0])
    return (pl[0], pl[1], runner.get('horse', '')) if pl else None


def compare_predictions_to_results(predictions: dict, results: list) -> dict:
    """
    Compare all predicted races to actual results.
    Returns a structured accuracy report.

    Only the HIGHEST-SCORED runner per recommendation type per race
    counts toward accuracy. Additional recommendations are shown in the
    UI as context but marked secondary=True and excluded from totals.
    This prevents inflating accuracy by recommending multiple horses
    in the same race.
    """
    race_outcomes = []
    # non_runner, no_result, and secondary outcomes excluded from totals
    totals = {'win':  {'correct': 0, 'incorrect': 0},
              'ew':   {'correct': 0, 'incorrect': 0, 'ew_win': 0, 'ew_placed': 0}}
    # Primary picks that were excluded from the accuracy denominator, kept
    # for transparency so a headline % is always shown with its context.
    excluded = {'non_runner': 0, 'unmatched': 0, 'no_result': 0}
    # DNFs are counted as incorrect (a losing bet) but tallied separately so
    # the UI can show how many "losses" were non-completions.
    dnf_count = 0
    # Flat £1-stake profit & loss on primary picks, settled at a bettable
    # price (see settle_price), for ROI reporting.
    pnl = {'win': {'stake': 0.0, 'ret': 0.0}, 'ew': {'stake': 0.0, 'ret': 0.0}}
    # How many bets settled at each basis ('book' / 'sp').
    basis_count = {'book': 0, 'sp': 0}
    # The same bets at the overnight forecast price the site used to settle
    # at — kept so the old (flattering) figure stays visible for comparison.
    pnl_forecast = {'stake': 0.0, 'ret': 0.0}
    # Baseline: back the morning favourite to win in every race where one of
    # our picks was settled, at the same price basis.
    fav = {'stake': 0.0, 'ret': 0.0, 'wins': 0}

    for pred_race in predictions.get('races', []):
        result = match_race(pred_race, results)

        ew_places = pred_race.get('ew_places', 3)
        race_result = {
            'course':    pred_race['course'],
            'time':      pred_race['time'],
            'race_name': pred_race.get('title', ''),
            'date':      pred_race.get('date', ''),
            'winner':    result['runners'][0]['name'] if (result and result['runners']) else '',
            'runners':   [],
            'abandoned': result is None,
        }

        # Runners are already sorted by score descending from scraper.
        # Track whether we've already used the primary pick for each rec type.
        primary_used = {'Win': False, 'EachWay': False}
        race_bet = False

        for runner in pred_race.get('runners', []):
            rec_type = runner.get('recommendation', {}).get('type', 'Skip')
            if rec_type == 'Skip':
                continue

            if result is None:
                outcome = {
                    'rec':        rec_type,
                    'actual_pos': None,
                    'outcome':    'no_result',
                }
            else:
                outcome = evaluate_prediction(runner, result, ew_places)

            outcome['horse']     = runner.get('horse', '')
            outcome['score']     = runner.get('score', 0)
            outcome['odds']      = runner.get('odds_str', '')
            outcome['label']     = runner.get('recommendation', {}).get('label', '')
            if runner.get('best_odds_dec') is not None:
                outcome['best_odds'] = runner['best_odds_dec']

            # Mark as primary or secondary
            is_primary = not primary_used.get(rec_type, False)
            outcome['primary'] = is_primary
            if is_primary:
                primary_used[rec_type] = True

            race_result['runners'].append(outcome)

            # Only tally primary picks that actually ran to a finish.
            # non_runner / dnf / unmatched / no_result are excluded — the
            # horse never got a fair, completed, matchable run.
            if not is_primary:
                continue
            if outcome['outcome'] in excluded:
                excluded[outcome['outcome']] += 1
                continue
            if outcome.get('dnf'):
                dnf_count += 1
            key = 'win' if rec_type == 'Win' else 'ew'

            # Flat-stake P&L for ROI (skips picks with no bettable price).
            price, basis = settle_price(runner, outcome)
            pl = bet_pnl(rec_type, outcome['outcome'], price)
            if pl:
                pnl[key]['stake'] += pl[0]
                pnl[key]['ret']   += pl[1]
                basis_count[basis] += 1
                outcome['settle_odds'], outcome['settle_basis'] = price, basis
                race_bet = True
                plf = bet_pnl(rec_type, outcome['outcome'], runner.get('odds_dec'))
                if plf:
                    pnl_forecast['stake'] += plf[0]
                    pnl_forecast['ret']   += plf[1]

            ew_correct_outcomes = {'correct', 'ew_win', 'ew_placed'}
            # For EW: ew_win and ew_placed both count as correct
            if key == 'ew':
                if outcome['outcome'] in ew_correct_outcomes:
                    totals['ew']['correct'] += 1
                elif outcome['outcome'] == 'incorrect':
                    totals['ew']['incorrect'] += 1
                if outcome['outcome'] in ('ew_win', 'ew_placed'):
                    totals['ew'][outcome['outcome']] += 1
            elif outcome['outcome'] in totals.get(key, {}):
                totals[key][outcome['outcome']] += 1

        if race_bet:
            fb = favourite_bet(pred_race, result)
            if fb:
                fav['stake'] += fb[0]
                fav['ret']   += fb[1]
                fav['wins']  += fb[1] > 0
                race_result['favourite'] = fb[2]

        if race_result['runners']:
            race_outcomes.append(race_result)

    # Calculate summary stats
    win_total = totals['win']['correct'] + totals['win']['incorrect']
    ew_total  = totals['ew']['correct']  + totals['ew']['incorrect']

    win_pct = round(totals['win']['correct'] / win_total * 100, 1) if win_total > 0 else None
    ew_pct  = round(totals['ew']['correct']  / ew_total  * 100, 1) if ew_total  > 0 else None

    all_correct = totals['win']['correct'] + totals['ew']['correct']
    all_total   = win_total + ew_total
    overall_pct = round(all_correct / all_total * 100, 1) if all_total > 0 else None

    def roi_pct(bucket):
        s = bucket['stake']
        return round((bucket['ret'] - s) / s * 100, 1) if s > 0 else None

    tot_stake = pnl['win']['stake'] + pnl['ew']['stake']
    tot_ret   = pnl['win']['ret']   + pnl['ew']['ret']
    overall_roi = round((tot_ret - tot_stake) / tot_stake * 100, 1) if tot_stake > 0 else None

    return {
        'date':          predictions.get('date', ''),
        'summary': {
            'overall_pct':  overall_pct,
            'win_pct':      win_pct,
            'ew_pct':       ew_pct,
            'win_correct':  totals['win']['correct'],
            'win_total':    win_total,
            'ew_correct':      totals['ew']['correct'],
            'ew_total':        ew_total,
            'ew_win_count':    totals['ew']['ew_win'],
            'ew_placed_count': totals['ew']['ew_placed'],
            'excluded':        excluded,
            'excluded_total':  sum(excluded.values()),
            'dnf_count':       dnf_count,
            # ROI on flat £1 stakes. `roi` carries raw stake/return so the UI
            # can aggregate a true multi-day ROI (not an average of averages).
            'overall_roi':  overall_roi,
            'win_roi':      roi_pct(pnl['win']),
            'ew_roi':       roi_pct(pnl['ew']),
            'roi': {
                'win_stake': round(pnl['win']['stake'], 2),
                'win_ret':   round(pnl['win']['ret'], 2),
                'ew_stake':  round(pnl['ew']['stake'], 2),
                'ew_ret':    round(pnl['ew']['ret'], 2),
            },
            'settle_basis': basis_count,
            # The same bets at the overnight forecast price (old basis).
            'roi_forecast': {
                'stake': round(pnl_forecast['stake'], 2),
                'ret':   round(pnl_forecast['ret'], 2),
            },
            # Favourite-to-win baseline in the races we bet.
            'fav': {
                'stake': round(fav['stake'], 2),
                'ret':   round(fav['ret'], 2),
                'wins':  fav['wins'],
            },
        },
        'races': race_outcomes,
    }


# Per-race detail is kept for this many recent days; older days keep only
# their summary so the full history stays small enough for the page to load.
DETAIL_DAYS = 30


def save_accuracy_log(out_dir: str, log_data: list):
    log_data = sorted(log_data, key=lambda e: e['date'])
    for e in log_data[:-DETAIL_DAYS]:
        e['races'] = []
    path = os.path.join(out_dir, 'accuracy.json')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(log_data, f, ensure_ascii=False, indent=2)
    log(f'Written: {path}')


def save_prediction_archive(out_dir: str, races_data: dict):
    """Fallback writer for the start-of-day prediction archive.

    The scraper's first run of the day owns this file (the frozen, leak-free
    accuracy record). This only creates it if it is somehow missing and NEVER
    overwrites an existing one — later intraday data must never reach the
    graded record.
    """
    date_str = races_data.get('date', '')
    if not date_str:
        return
    hist_dir = os.path.join(out_dir, 'history')
    os.makedirs(hist_dir, exist_ok=True)
    path = os.path.join(hist_dir, f'races_{date_str}.json')
    if os.path.exists(path):
        log(f'Start-of-day archive for {date_str} already frozen — leaving untouched')
        return
    if not races_data.get('races'):
        return
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(races_data, f, ensure_ascii=False, indent=2)
    log(f'Fallback-archived predictions: {path}')


def rebuild_accuracy_log(out_dir: str):
    """Regrade every day that has both a start-of-day archive and a saved
    full result, and rewrite accuracy.json from scratch. Offline and
    deterministic — use it after changing how bets are graded or settled."""
    hist_dir = os.path.join(out_dir, 'history')
    acc_log = []
    for res_path in sorted(glob.glob(os.path.join(hist_dir, 'results_full_*.json'))):
        d = os.path.basename(res_path)[len('results_full_'):-len('.json')]
        pred_path = os.path.join(hist_dir, f'races_{d}.json')
        if not os.path.exists(pred_path):
            continue
        with open(pred_path, encoding='utf-8') as f:
            predictions = json.load(f)
        with open(res_path, encoding='utf-8') as f:
            results = json.load(f).get('races', [])
        if not predictions.get('races') or not results:
            continue
        acc_log.append(compare_predictions_to_results(predictions, results))
    log(f'Regraded {len(acc_log)} days from {hist_dir}')
    save_accuracy_log(out_dir, acc_log)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', default='', help='Output directory (horses/ folder)')
    parser.add_argument('--results-date', default='',
                        help='Date to fetch results for (default: yesterday)')
    parser.add_argument('--rebuild', action='store_true',
                        help='Regrade every archived day from history/ (no network) '
                             'and rewrite accuracy.json')
    args = parser.parse_args()

    if args.out:
        out_dir = os.path.abspath(args.out)
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        out_dir = os.path.abspath(os.path.join(script_dir, '..', 'horses'))

    if args.rebuild:
        rebuild_accuracy_log(out_dir)
        return

    yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    results_date = args.results_date or yesterday

    print(f'Fetching results for {results_date}…', file=sys.stderr)

    # 1. Archive today's predictions before scraper overwrites them
    races_path = os.path.join(out_dir, 'races.json')
    if os.path.exists(races_path):
        with open(races_path, encoding='utf-8') as f:
            current = json.load(f)
        if current.get('date') == results_date:
            save_prediction_archive(out_dir, current)

    # 2. Fetch actual results
    results = fetch_sl_results(results_date)
    if not results:
        log('No results found — skipping accuracy update')
        return

    # 2b. Persist the full finishing order as a committed dataset. This is the
    #     clean, growing record calibrate.py / ROI analysis build on, so they
    #     never need to re-fetch from Sporting Life.
    hist_dir = os.path.join(out_dir, 'history')
    os.makedirs(hist_dir, exist_ok=True)
    full_path = os.path.join(hist_dir, f'results_full_{results_date}.json')
    with open(full_path, 'w', encoding='utf-8') as f:
        json.dump({'date': results_date, 'races': results}, f, ensure_ascii=False, indent=2)
    log(f'Saved full results: {full_path}')

    # 3. Load predictions for that date
    predictions = load_predictions(out_dir, results_date)
    if not predictions:
        log(f'No saved predictions found for {results_date}')
        # Still save raw results for reference
        raw_path = os.path.join(out_dir, 'history', f'results_{results_date}.json')
        os.makedirs(os.path.dirname(raw_path), exist_ok=True)
        with open(raw_path, 'w', encoding='utf-8') as f:
            json.dump({'date': results_date, 'races': results}, f, indent=2)
        return

    if not predictions.get('races'):
        log(f'Saved predictions for {results_date} have 0 races — '
            f'archive was likely created from a late-evening scrape. '
            f'Skipping accuracy update to avoid a blank entry.')
        return

    # 4. Compare
    report = compare_predictions_to_results(predictions, results)
    s = report['summary']
    log(f'Results: {s["win_correct"]}/{s["win_total"]} Win, '
        f'{s["ew_correct"]}/{s["ew_total"]} EW, '
        f'Overall {s["overall_pct"]}% · '
        f'ROI win {s["win_roi"]}% ew {s["ew_roi"]}% overall {s["overall_roi"]}%')

    # 5. Regrade the whole log from history/. accuracy.json is fully derived
    #    from the frozen archives + saved results, so it never drifts, and a
    #    merge conflict on it resolves itself on the next run.
    rebuild_accuracy_log(out_dir)


if __name__ == '__main__':
    main()
