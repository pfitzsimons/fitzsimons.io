from __future__ import annotations
#!/usr/bin/env python3
"""
Results fetcher — UK & Ireland Horse Racing
Scrapes yesterday's actual results from Sporting Life,
compares them to our saved predictions, and writes accuracy.json.

Run after scrape_races.py each morning:
    python3 scripts/fetch_results.py --out /path/to/horses
"""

import argparse
import gzip
import json
import os
import re
import sys
import urllib.request
import urllib.error
from datetime import date, timedelta, datetime

UK_IRE_COUNTRIES = {
    'england', 'scotland', 'wales', 'ireland', 'eire',
    'northern ireland', 'republic of ireland'
}

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/122.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-GB,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate',
}


def log(msg):
    print(f'  {msg}', file=sys.stderr)


def fetch(url):
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            if resp.headers.get('Content-Encoding', '') == 'gzip':
                raw = gzip.decompress(raw)
            return raw.decode('utf-8', errors='replace')
    except urllib.error.HTTPError as e:
        log(f'HTTP {e.code} -> {url}')
    except Exception as e:
        log(f'{type(e).__name__} -> {url}: {e}')
    return None


def normalise_name(name: str) -> str:
    """Normalise horse name for fuzzy matching — uppercase, strip punctuation."""
    name = name.upper().strip()
    name = re.sub(r"['\-\s]+", ' ', name)
    name = re.sub(r'[^A-Z0-9 ]', '', name)
    return name.strip()


def fetch_sl_results(date_str: str) -> list:
    """
    Fetch results from Sporting Life for a given date.
    Returns list of dicts:
      { course, time, race_name, runners: [{name, position, odds, favourite}] }
    """
    url = f'https://www.sportinglife.com/racing/results/{date_str}'
    log(f'Fetching results: {url}')
    body = fetch(url)
    if not body:
        return []

    # Extract the __NEXT_DATA__ JSON blob
    m = re.search(r'<script[^>]*>({"props":.+?})</script>', body, re.DOTALL)
    if not m:
        # fallback — find any large script
        scripts = re.findall(r'<script[^>]*>(\{"props".*?)</script>', body, re.DOTALL)
        if not scripts:
            log('Could not find result data in Sporting Life page')
            return []
        m_text = scripts[0]
    else:
        m_text = m.group(1)

    try:
        data = json.loads(m_text)
    except json.JSONDecodeError as e:
        log(f'JSON parse error: {e}')
        return []

    meetings = data.get('props', {}).get('pageProps', {}).get('meetings', [])
    results = []

    for mtg in meetings:
        summary = mtg.get('meeting_summary', {})
        course_obj = summary.get('course', {})
        course = course_obj.get('name', '')
        country = course_obj.get('country', {}).get('long_name', '').lower()

        # UK & Ireland only
        if not any(c in country for c in UK_IRE_COUNTRIES):
            continue

        for race in mtg.get('races', []):
            top_horses = race.get('top_horses', [])
            if not top_horses:
                continue

            results.append({
                'course':    course,
                'time':      race.get('time', ''),
                'race_name': race.get('name', ''),
                'date':      date_str,
                'runners':   [
                    {
                        'name':      h.get('name', ''),
                        'position':  h.get('position', 0),
                        'odds':      h.get('odds', ''),
                        'favourite': h.get('favourite', False),
                    }
                    for h in top_horses
                ],
            })

    log(f'Found {len(results)} UK/IRE races with results')
    return results


def match_race(pred_race: dict, result_races: list) -> dict | None:
    """
    Match a predicted race to a result race by course + approximate time.
    Returns the result race dict or None.
    """
    pred_course = normalise_name(pred_race.get('course', ''))
    pred_time   = pred_race.get('time', '')

    for res in result_races:
        res_course = normalise_name(res.get('course', ''))
        res_time   = res.get('time', '')

        # Course match (allow partial — e.g. "Stratford On Avon" vs "Stratford")
        if pred_course not in res_course and res_course not in pred_course:
            continue

        # Time match — within 5 minutes
        try:
            ph, pm = map(int, pred_time.split(':'))
            rh, rm = map(int, res_time.split(':'))
            if abs((ph * 60 + pm) - (rh * 60 + rm)) <= 70:
                return res
        except Exception:
            continue

    return None


def evaluate_prediction(runner: dict, result: dict, ew_places: int) -> dict:
    """
    Compare a single runner's prediction to the actual result.
    Returns an outcome dict.
    """
    pred_name  = normalise_name(runner.get('horse', ''))
    rec_type   = runner.get('recommendation', {}).get('type', 'Skip')

    # Find this horse in the results
    actual_pos = None
    for res_runner in result.get('runners', []):
        res_name = normalise_name(res_runner.get('name', ''))
        if pred_name == res_name or pred_name in res_name or res_name in pred_name:
            actual_pos = res_runner.get('position')
            break

    # Determine outcome
    if rec_type == 'Skip':
        # A skip is "correct" if the horse didn't win
        outcome = 'correct' if actual_pos != 1 else 'incorrect'
        return {'rec': rec_type, 'actual_pos': actual_pos, 'outcome': outcome}

    if actual_pos is None:
        # Horse not in results — likely DNF/non-runner
        outcome = 'dnf'
        return {'rec': rec_type, 'actual_pos': None, 'outcome': outcome}

    if rec_type == 'Win':
        outcome = 'correct' if actual_pos == 1 else 'incorrect'
    elif rec_type == 'EachWay':
        outcome = 'correct' if actual_pos <= ew_places else 'incorrect'
    else:
        outcome = 'skip'

    return {'rec': rec_type, 'actual_pos': actual_pos, 'outcome': outcome}


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


def compare_predictions_to_results(predictions: dict, results: list) -> dict:
    """
    Compare all predicted races to actual results.
    Returns a structured accuracy report.
    """
    race_outcomes = []
    totals = {'win': {'correct': 0, 'incorrect': 0, 'dnf': 0},
              'ew':  {'correct': 0, 'incorrect': 0, 'dnf': 0},
              'skip':{'correct': 0, 'incorrect': 0}}

    for pred_race in predictions.get('races', []):
        result = match_race(pred_race, results)
        if not result:
            log(f'  No result found for {pred_race["course"]} {pred_race["time"]}')
            continue

        ew_places = pred_race.get('ew_places', 3)
        race_result = {
            'course':    pred_race['course'],
            'time':      pred_race['time'],
            'race_name': pred_race.get('title', ''),
            'date':      pred_race.get('date', ''),
            'winner':    result['runners'][0]['name'] if result['runners'] else '',
            'runners':   [],
        }

        for runner in pred_race.get('runners', []):
            rec_type = runner.get('recommendation', {}).get('type', 'Skip')
            # Only evaluate non-Skip recommendations (and top Skip picks)
            if rec_type == 'Skip':
                continue

            outcome = evaluate_prediction(runner, result, ew_places)
            outcome['horse']   = runner.get('horse', '')
            outcome['score']   = runner.get('score', 0)
            outcome['odds']    = runner.get('odds_str', '')
            outcome['label']   = runner.get('recommendation', {}).get('label', '')

            race_result['runners'].append(outcome)

            # Tally
            key = 'win' if rec_type == 'Win' else 'ew' if rec_type == 'EachWay' else 'skip'
            if outcome['outcome'] in totals[key]:
                totals[key][outcome['outcome']] += 1

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

    return {
        'date':          predictions.get('date', ''),
        'summary': {
            'overall_pct':  overall_pct,
            'win_pct':      win_pct,
            'ew_pct':       ew_pct,
            'win_correct':  totals['win']['correct'],
            'win_total':    win_total,
            'ew_correct':   totals['ew']['correct'],
            'ew_total':     ew_total,
        },
        'races': race_outcomes,
    }


def load_accuracy_log(out_dir: str) -> list:
    path = os.path.join(out_dir, 'accuracy.json')
    if os.path.exists(path):
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    return []


def save_accuracy_log(out_dir: str, log_data: list):
    path = os.path.join(out_dir, 'accuracy.json')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(log_data, f, ensure_ascii=False, indent=2)
    log(f'Written: {path}')


def save_prediction_archive(out_dir: str, races_data: dict):
    """Archive today's predictions before they get overwritten tomorrow."""
    date_str = races_data.get('date', '')
    if not date_str:
        return
    hist_dir = os.path.join(out_dir, 'history')
    os.makedirs(hist_dir, exist_ok=True)
    path = os.path.join(hist_dir, f'races_{date_str}.json')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(races_data, f, ensure_ascii=False, indent=2)
    log(f'Archived predictions: {path}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', default='', help='Output directory (horses/ folder)')
    parser.add_argument('--results-date', default='',
                        help='Date to fetch results for (default: yesterday)')
    args = parser.parse_args()

    if args.out:
        out_dir = os.path.abspath(args.out)
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        out_dir = os.path.abspath(os.path.join(script_dir, '..', 'horses'))

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

    # 4. Compare
    report = compare_predictions_to_results(predictions, results)
    s = report['summary']
    log(f'Results: {s["win_correct"]}/{s["win_total"]} Win, '
        f'{s["ew_correct"]}/{s["ew_total"]} EW, '
        f'Overall {s["overall_pct"]}%')

    # 5. Append to running accuracy log
    acc_log = load_accuracy_log(out_dir)
    # Remove any existing entry for this date
    acc_log = [e for e in acc_log if e.get('date') != results_date]
    acc_log.append(report)
    # Keep last 30 days
    acc_log = sorted(acc_log, key=lambda x: x['date'])[-30:]
    save_accuracy_log(out_dir, acc_log)


if __name__ == '__main__':
    main()