#!/usr/bin/env python3
"""
Horse Racing Scraper — UK & Ireland
Source: Sporting Life

Multi-factor scoring model:
  1. Recent form score     — weighted last 5 runs, recency-boosted
  2. Consistency score     — how often horse finishes placed
  3. Odds value score      — implied prob vs field average
  4. Weight-for-age score  — lower weight = advantage, especially in handicaps
  5. Going suitability     — heavier going favours stayers/jumpers
  6. Jockey quality        — top jockeys rated by known record
  7. Recency penalty       — long abs gaps (/) penalised
  8. DNF penalty           — P (pulled up), F (fell), U (unseated), B (bolted)
  9. Distance suitability  — proven at today's trip (off by default; see
                             DIST_WEIGHT / scripts/backtest_distance.py)

Final score 0–100 → label + confidence + Win/Skip
"""

import argparse
import gzip
import html
import json
import math
import os
import re
import sys
import time
import random
from datetime import date, datetime, time as dt_time, timezone
from typing import Optional
from zoneinfo import ZoneInfo
import urllib.request
import urllib.error

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import strike_rates

# Data-derived jockey/trainer strike-rate table, built once per run from the
# accumulating results in horses/history (see build_strike_table). None until
# built; score_runner falls back to the static JOCKEY_RATINGS table when it is
# unpopulated (e.g. a cold start with no results yet).
STRIKE_TABLE = None

# Sporting Life returns race off-times in UTC; the site displays them
# unconverted, which is an hour out during British/Irish Summer Time.
# UK and Ireland share the same clock (both UTC+1 in summer), so a
# single Europe/London conversion is correct for both.
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

BASE_URL = "https://www.sportinglife.com"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Referer": "https://www.sportinglife.com/",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Upgrade-Insecure-Requests": "1",
}

# ─────────────────────────────────────────────────────────────
# JOCKEY RATINGS  (0–10 scale, based on known UK/IRE records)
# Top jump/flat jockeys rated by historical win rate & quality
# ─────────────────────────────────────────────────────────────
JOCKEY_RATINGS = {
    # Flat — elite
    "Ryan Moore":             9.5,
    "Frankie Dettori":        9.2,
    "William Buick":          9.0,
    "Oisin Murphy":           9.0,
    "James Doyle":            8.8,
    "Tom Marquand":           8.5,
    "Hollie Doyle":           8.3,
    "Jim Crowley":            8.2,
    "Adam Kirby":             8.0,
    "Silvestre De Sousa":     7.8,
    "Daniel Tudhope":         8.0,
    "Paul Hanagan":           7.8,
    "Cieren Fallon":          7.5,
    "David Probert":          7.5,
    "Richard Kingscote":      7.5,
    "Rossa Ryan":             7.8,
    "Tom Eaves":              7.2,
    "Saffie Osborne":         7.0,
    "Jason Hart":             7.2,
    "Kevin Stott":            7.0,
    "P.J. McDonald":          7.5,
    "Franny Norton":          7.0,
    "Andrew Mullen":          6.8,
    "Rob Hornby":             7.2,
    # Flat — Ireland
    "Colin Keane":            9.0,
    "C. T. Keane":            9.0,
    "Shane Foley":            8.5,
    "Seamie Heffernan":       8.2,
    "Wayne Lordan":           8.0,
    "Declan McDonogh":        7.8,
    "Chris Hayes":            7.5,
    "Billy Lee":              7.5,
    "Ben Martin Coen":        7.8,
    "Dylan Browne McMonagle": 8.0,
    "Rory Cleary":            7.2,
    "L. F. Roche":            8.0,
    "Ryan Sexton":            7.0,
    "G. F. Carroll":          7.0,
    "R. P. Whelan":           6.8,
    # Jump — UK
    "Harry Skelton":          9.0,
    "Sean Bowen":             9.2,
    "Tom Cannon":             8.5,
    "Sam Twiston-Davies":     8.8,
    "Brian Hughes":           8.5,
    "Jonjo O'Neill Jr":       8.0,
    "Harry Cobden":           8.8,
    "Nick Scholfield":        7.5,
    "Brendan Powell":         7.0,
    "Marc Goldstein":         6.5,
    "James Bowen":            8.0,
    "Bryan Carver":           6.5,
    "Daniel Sansom":          6.0,
    "Chad Bament":            6.0,
    "James Davies":           7.0,
    "Freddie Mitchell":       7.5,
    "Harry Reed":             7.0,
    "Tabitha Worsley":        6.0,
    "Jay Tidball":            6.5,
    "Paul O'Brien":           7.2,
    # Jump — Ireland
    "Paul Townend":           9.5,
    "Rachael Blackmore":      9.3,
    "Jack Kennedy":           9.0,
    "J. W. Kennedy":          9.0,
    "Davy Russell":           8.8,
    "Danny Mullins":          8.5,
    "D. E. Mullins":          8.5,
    "Keith Donoghue":         8.2,
    "Robbie Power":           8.5,
    "Mark Walsh":             8.8,
    "Patrick Mullins":        8.0,
    "Donagh Meyler":          7.5,
    "Liam McKenna":           7.2,
    "Sean Flanagan":          7.8,
    "Tiernan Power Roche":    7.5,
    "D. J. O'Keeffe":         7.0,
    "G. B. Noonan":           6.8,
    "R. C. Colgan":           6.8,
    "Danny Gilligan":         6.5,
    "Dylan Robinson":         6.5,
    "N. M. Crosse":           6.0,
    "N. G. McCullagh":        6.5,
    "William James Lee":      7.0,
    "Adam Caffrey":           6.8,
    "M. J. Kenneally":        6.5,
    "J. P. Shinnick":         6.0,
    "D. W. O'Connor":         6.5,
    "J. M. Sheridan":         6.5,
    "Caoilin Quinn":          6.8,
    "Eoin Walsh":             6.5,
    "Jack Cleary":            6.5,
    "Daire McConville":       7.0,
    "Wesley Joyce":           6.5,
    "Siobhan Rutledge":       6.0,
    "H. E. Sexton":           6.5,
    "P. T. Smithers":         6.0,
    "D. King":                6.0,
    "C. M. Quirke":           6.0,
    "James Smith":            6.5,
    "Shane O'Callaghan":      6.5,
    "P. A. Harnett":          6.5,
    "Sadhbh Tormey":          6.0,
    "Enola Pollet":           6.0,
    "Isabelle Ryder":         6.0,
    "Miss Daisy White":       6.0,
    "Mr Dale Peters":         6.0,
    "Mr Sean O'Connor":       6.0,
    "Miss Megan Bevan":       6.0,
    "Miss Amber Jackson-Fennell": 6.0,
    "L. T. McAteer":          6.5,
    "Andrew Joseph Slattery": 6.8,
    "C. Geerdharry":          6.0,
    "Liam Harrison":          6.5,
    "Dylan Johnston":         6.5,
    "Jack Sheridan":          6.5,
}

DEFAULT_JOCKEY_RATING = 6.0  # unknown jockeys


def _norm_jockey(name: str) -> str:
    """Normalise a jockey name for rating lookup.

    The feed and the rating table disagree on punctuation — e.g. the feed
    sends "Jonjo O'Neill Jr." (trailing period) while the table has
    "Jonjo O'Neill Jr". Stripping periods and collapsing whitespace lets
    initial-style names ("C. T. Keane") match too, so these no longer
    silently fall through to the default rating.
    """
    return re.sub(r"\s+", " ", re.sub(r"\.", "", html.unescape(name or ""))).strip().lower()


# Normalised view of the rating table, built once for fast lookup.
JOCKEY_RATINGS_NORM = {_norm_jockey(k): v for k, v in JOCKEY_RATINGS.items()}

# ─────────────────────────────────────────────────────────────
# COURSE ACCURACY COEFFICIENTS  (per-course score multiplier)
#
# Intentionally EMPTY. The previous hand-set table was "derived from historical
# prediction accuracy per course" on the very 60 days we backtest against, so
# testing it there is circular. When re-derived HONESTLY — a leak-free
# walk-forward estimate that only uses each course's prior races — per-course
# multipliers do not hold up: out-of-sample they drop the profitable Strong Win
# Bet tier from +11% to around 0-5% ROI. Removing them also improves overall
# out-of-sample ROI (toggle this dict and re-run scripts/backtest_value.py to
# reproduce: keeping it empty gives overall -4.9% vs -5.5% with the old values).
# With ~40 courses and only a handful of strong bets each
# over 60 days, there is not enough data to justify any course multiplier, so we
# keep the mechanism but ship no coefficients. Add one here only if it survives
# walk-forward validation.
# ─────────────────────────────────────────────────────────────
COURSE_COEFFICIENTS = {}


# ─────────────────────────────────────────────────────────────
# MODEL-VS-MARKET READOUT
# Turn the raw score into a calibrated win-probability and show it next to
# the market's implied probability. This is INFORMATIONAL only: a 61-day
# backtest (scripts/backtest_value.py) showed that betting on the model's
# "value" disagreements with the market DEGRADES ROI — the market's prices
# are sharper than the model's probabilities, so the model's edge lies in
# agreeing with the market on strong favourites, not in beating the price.
# The readout is kept so the model's confidence vs the market is visible.
# ─────────────────────────────────────────────────────────────

# Score→win-probability calibration: band centre → observed win rate,
# from the 61-day backtest. Linearly interpolated. The 90-99 point is held
# at the 80-89 value (its own sample was tiny and noisy).
_CALIB = [
    (5, 0.037), (15, 0.047), (25, 0.048), (35, 0.082), (45, 0.127),
    (55, 0.156), (65, 0.231), (75, 0.316), (85, 0.401), (95, 0.401),
]


def score_to_winprob(score: float) -> float:
    """Map a 0–100 score to a calibrated win probability."""
    if score <= _CALIB[0][0]:
        return _CALIB[0][1]
    if score >= _CALIB[-1][0]:
        return _CALIB[-1][1]
    for (x0, y0), (x1, y1) in zip(_CALIB, _CALIB[1:]):
        if x0 <= score <= x1:
            return y0 + (y1 - y0) * (score - x0) / (x1 - x0)
    return _CALIB[-1][1]


def compute_value(runners: list) -> None:
    """
    Attach model-vs-market metrics to each runner (mutates in place):
      _model_prob  — calibrated win prob, normalised so the field sums to 1
      _market_prob — implied prob from odds, overround-removed (sums to 1)

    Runners must already carry a "_score" and an "odds_dec". These feed the
    informational readout only — recommendations are not gated on them.
    """
    raw = [score_to_winprob(r.get("_score", 0)) for r in runners]
    tot = sum(raw) or 1.0

    mkt = []
    for r in runners:
        od = r.get("odds_dec")
        mkt.append(1.0 / od if od and od > 1 else 0.0)
    mtot = sum(mkt) or 1.0

    for r, rw, mk in zip(runners, raw, mkt):
        r["_model_prob"]  = rw / tot
        r["_market_prob"] = mk / mtot if mtot else 0.0


# ─────────────────────────────────────────────────────────────
# HTTP
# ─────────────────────────────────────────────────────────────

def fetch(url, retries=3):
    req = urllib.request.Request(url, headers=HEADERS)
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                if resp.headers.get("Content-Encoding", "") == "gzip":
                    raw = gzip.decompress(raw)
                return raw.decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", errors="replace")[:300]
            except Exception:
                pass
            log(f"HTTP {e.code} -> {url} (attempt {attempt}/{retries}) {body!r}")
            if e.code not in (403, 429, 500, 502, 503, 504) or attempt == retries:
                return None
        except Exception as e:
            log(f"{type(e).__name__} -> {url}: {e} (attempt {attempt}/{retries})")
            if attempt == retries:
                return None
        time.sleep(random.uniform(2, 4) * attempt)
    return None


def log(msg):
    print(f"  {msg}", file=sys.stderr)


def clean(s):
    s = html.unescape(s or "")
    s = re.sub(r"<[^>]+>", "", s)
    return re.sub(r"\s+", " ", s).strip()


def parse_odds(raw):
    raw = (raw or "").strip().replace(",", "")
    if not raw or raw in ("-", "—", "SP"):
        return None
    if raw == "EVS":
        return 2.0
    m = re.match(r"^(\d+)/(\d+)$", raw)
    if m:
        return round(int(m.group(1)) / int(m.group(2)) + 1, 2)
    try:
        return round(float(raw), 2)
    except ValueError:
        return None


# ─────────────────────────────────────────────────────────────
# FORM ANALYSIS
# ─────────────────────────────────────────────────────────────

def parse_form(form_str: str) -> dict:
    """
    Parse a form string into structured signals.

    Characters:
      1-9  = finishing position
      0    = finished 10th or worse
      P    = pulled up
      F    = fell
      U    = unseated rider
      B    = bolted / brought down
      -    = season separator
      /    = year separator (long absence)

    Returns dict with:
      runs          - list of results newest-last (each: int 1-10, or 'DNF')
      recent_score  - 0-100, weighted recency score from last 5 runs
      consistency   - 0-100, % of runs finishing 1st-3rd
      dnf_rate      - 0-1, proportion of non-completions
      long_absence  - bool, had a '/' in form (missed a season+)
      last_run      - most recent result character
    """
    if not form_str:
        return _empty_form()

    # Split on '/' to detect long absences, keep most recent season
    has_long_absence = '/' in form_str
    # Take runs after last '/' (most recent season)
    recent_part = form_str.split('/')[-1]
    # Remove season separators within the season
    recent_part = recent_part.replace('-', '')

    if not recent_part:
        return _empty_form()

    # Map each character to a score
    DNF_CHARS = {'P', 'F', 'U', 'B'}
    runs = []
    for ch in recent_part:
        if ch.isdigit():
            pos = int(ch)
            runs.append(10 if pos == 0 else pos)  # 0 = 10th or worse
        elif ch in DNF_CHARS:
            runs.append('DNF')
        # ignore anything else

    if not runs:
        return _empty_form()

    last_run = runs[-1]

    # Most recent 5 runs (newest last, so take last 5)
    recent = runs[-5:]

    # Weighted score: most recent = weight 5, oldest = weight 1
    weights = list(range(1, len(recent) + 1))
    total_weight = sum(weights)
    weighted_score = 0.0
    for i, r in enumerate(recent):
        w = weights[i]
        if r == 'DNF':
            pos_score = 0
        elif r == 1:
            pos_score = 100
        elif r == 2:
            pos_score = 80
        elif r == 3:
            pos_score = 65
        elif r == 4:
            pos_score = 50
        elif r == 5:
            pos_score = 40
        elif r <= 7:
            pos_score = 25
        else:
            pos_score = 10
        weighted_score += pos_score * w

    recent_score = weighted_score / total_weight if total_weight > 0 else 0

    # Consistency: % of all runs finishing 1st-3rd
    placed = sum(1 for r in runs if isinstance(r, int) and r <= 3)
    dnfs   = sum(1 for r in runs if r == 'DNF')
    consistency = (placed / len(runs)) * 100 if runs else 0
    dnf_rate    = dnfs / len(runs) if runs else 0

    return {
        "runs":          runs,
        "recent_score":  round(recent_score, 1),
        "consistency":   round(consistency, 1),
        "dnf_rate":      round(dnf_rate, 2),
        "long_absence":  has_long_absence,
        "last_run":      last_run,
        "num_runs":      len(runs),
    }


def _empty_form():
    return {
        "runs": [], "recent_score": 40.0, "consistency": 0.0,
        "dnf_rate": 0.0, "long_absence": False, "last_run": None, "num_runs": 0
    }


# ─────────────────────────────────────────────────────────────
# WEIGHT PARSING
# ─────────────────────────────────────────────────────────────

def parse_weight_lbs(weight_str: str) -> Optional[int]:
    """Convert '11-7' (stones-pounds) to total lbs."""
    m = re.match(r"(\d+)-(\d+)", weight_str or "")
    if m:
        return int(m.group(1)) * 14 + int(m.group(2))
    return None


# ─────────────────────────────────────────────────────────────
# GOING SUITABILITY
# ─────────────────────────────────────────────────────────────

def going_factor(going: str, distance: str) -> float:
    """
    Returns a multiplier 0.8–1.1 based on going.
    Heavy/Soft going increases uncertainty (wider field = more each way value).
    Firm going favours speed horses.
    This is a field-level factor, not horse-specific (we don't have horse going prefs).
    """
    going_lower = (going or "").lower()
    if any(x in going_lower for x in ["heavy", "soft"]):
        return 0.92  # more uncertain, reduce win confidence
    if any(x in going_lower for x in ["yielding", "good to soft"]):
        return 0.96
    if any(x in going_lower for x in ["good to firm", "firm", "hard"]):
        return 1.05  # faster ground = more predictable pace
    return 1.0  # good / standard


# ─────────────────────────────────────────────────────────────
# DISTANCE SUITABILITY  ("proven at the trip")
#
# The form string carries only finishing positions — it has no idea a horse's
# wins came at 5f while today's race is 1m6f. Sporting Life's racecard JSON does
# expose each past run's distance (horse.previous_results), so we can reward a
# horse whose good runs came at a distance near today's and discount form set at
# a very different trip.
#
# DIST_WEIGHT is the fraction of the 0–100 score this factor carries; the same
# fraction is taken out of the recent-form factor so the weights still sum to
# their original total (form is the distance-blind factor most in need of the
# correction). DIST_WEIGHT = 0 makes the whole factor a no-op, reproducing the
# pre-distance model exactly — it is turned on only at a weight that survives the
# walk-forward backtest (scripts/backtest_distance.py).
# ─────────────────────────────────────────────────────────────
DIST_WEIGHT = 0.0
FORM_WEIGHT_BASE = 0.30

# Result → quality in 0–1, mirroring the recent-form position scale (/100).
_DIST_POS_QUALITY = {1: 1.00, 2: 0.80, 3: 0.65, 4: 0.50, 5: 0.40}


def _pos_quality(pos: int) -> float:
    if pos in _DIST_POS_QUALITY:
        return _DIST_POS_QUALITY[pos]
    return 0.25 if pos <= 7 else 0.10


def distance_to_furlongs(dist_str: str) -> Optional[float]:
    """Parse a distance like '5f', '6f 111y', '1m 7f 110y', '2m 4f' to furlongs.

    1 mile = 8 furlongs, 1 furlong = 220 yards. Returns None if nothing parses.
    """
    if not dist_str:
        return None
    d = dist_str.strip().lower()
    m = re.search(r"(\d+)\s*m", d)
    f = re.search(r"(\d+)\s*f", d)
    y = re.search(r"(\d+)\s*y", d)
    if not (m or f or y):
        return None
    miles = int(m.group(1)) if m else 0
    fur   = int(f.group(1)) if f else 0
    yards = int(y.group(1)) if y else 0
    return miles * 8 + fur + yards / 220.0


def distance_suitability(cur_f: Optional[float], prev_runs: list) -> tuple:
    """Score how well a horse's past results suit today's distance (0–100).

    Each prior run is weighted by a Gaussian on its distance gap from today's
    trip (so exact-trip runs dominate and very different trips fade out), and by
    how well the horse ran. The distance-weighted average result quality is then
    shrunk toward a neutral 50 by how much trip-relevant evidence exists, so a
    horse with no runs near today's distance — or none at all — sits at neutral
    rather than being rewarded or penalised on irrelevant form.

    Returns (score, meta) where meta explains the components for transparency.
    """
    if not cur_f or not prev_runs:
        return 50.0, {"n": 0}

    # Tolerance grows with trip: sprinters are more distance-sensitive (in
    # absolute furlongs) than stayers.
    scale = max(1.0, 0.20 * cur_f)
    num = den = 0.0
    used = 0
    for pr in prev_runs:
        df = pr.get("dist_f")
        pos = pr.get("pos")
        if not df or not isinstance(pos, int) or pos < 1:
            continue
        w = math.exp(-((abs(cur_f - df) / scale) ** 2))
        if w < 0.01:  # effectively a different discipline of trip — ignore
            continue
        num += w * _pos_quality(pos)
        den += w
        used += 1

    if den <= 0:
        return 50.0, {"n": 0}

    quality = num / den
    # Bayesian-style shrinkage toward neutral by evidence strength (den behaves
    # like an effective count of trip-relevant runs), matching strike_rates.py.
    k = 1.0
    sub01 = (den * quality + k * 0.5) / (den + k)
    return round(100 * sub01, 1), {
        "n": used, "relevance": round(den, 2), "quality": round(quality, 2),
    }


# ─────────────────────────────────────────────────────────────
# EACH WAY TERMS
# ─────────────────────────────────────────────────────────────

def ew_places(field_size: int) -> int:
    """Standard each-way place terms by field size."""
    if field_size <= 4:   return 1  # win only
    if field_size <= 7:   return 2  # 1st & 2nd
    if field_size <= 11:  return 3  # 1st, 2nd, 3rd
    return 4                         # 1st–4th (16+ runners, some races)


# ─────────────────────────────────────────────────────────────
# MULTI-FACTOR SCORING ENGINE
# ─────────────────────────────────────────────────────────────

def score_runner(runner: dict, field_size: int, going: str,
                 distance: str, race_title: str) -> dict:
    """
    Score a runner 0–100 across multiple factors and return
    a recommendation dict.

    Factors & weights:
      A) Odds value          25%  — implied prob vs field average
      B) Recent form    30%−DIST_WEIGHT — weighted last 5 runs
      C) Consistency         15%  — placed% across all known runs
      D) Jockey strike-rate 7.5%  — data-derived (see strike_rates.py)
      D') Trainer strike-rate 7.5% — data-derived (see strike_rates.py)
      E) Weight advantage    10%  — relative weight vs field average
      F) Absence penalty      5%  — penalise '/' in form (long gap)
      G) Distance suitability DIST_WEIGHT — form set near today's trip

    (D)+(D') keep the same 15% total the static jockey rating used to carry, so
    score bands and recommendation thresholds are unchanged. (G) is carved out
    of (B) and ships at DIST_WEIGHT = 0 (a no-op) until it clears the
    walk-forward bar — see the DIST_WEIGHT note above distance_suitability().
    """
    odds_dec  = runner.get("odds_dec")
    form_str  = runner.get("form", "")
    jockey    = runner.get("jockey", "")
    weight_s  = runner.get("weight", "")
    age_s     = runner.get("age", "")

    form = parse_form(form_str)

    # ── A) Odds value score (0–100) ──────────────────────────
    if odds_dec and odds_dec > 1:
        implied_prob = 1 / odds_dec
        avg_prob = 1 / field_size
        # ratio > 1 means trading shorter than average (market thinks it has a chance)
        ratio = implied_prob / avg_prob
        # Scale: ratio 3.0 = 100, ratio 0.5 = 0
        odds_score = min(100, max(0, (ratio - 0.3) / 2.7 * 100))
    else:
        odds_score = 40.0  # SP or unknown — neutral

    # ── B) Recent form score (0–100) ─────────────────────────
    form_score = form["recent_score"]

    # Bonus for winning last time
    if form["last_run"] == 1:
        form_score = min(100, form_score * 1.15)
    # Penalty for DNF last time
    elif form["last_run"] == "DNF":
        form_score *= 0.7

    # ── C) Consistency score (0–100) ─────────────────────────
    consistency_score = form["consistency"]
    # Penalise high DNF rate heavily
    consistency_score *= (1 - form["dnf_rate"] * 1.5)
    consistency_score = max(0, consistency_score)

    # ── D/D') Jockey & trainer strike-rate (0–100 each) ──────
    # Prefer data-derived strike-rates from the accumulating results; fall back
    # to the static jockey table (trainer neutral) only before any results have
    # accumulated, so a cold start still runs.
    trainer = runner.get("trainer", "")
    if STRIKE_TABLE is not None and STRIKE_TABLE.is_populated():
        jockey_score  = STRIKE_TABLE.jockey_sub(jockey)
        trainer_score = STRIKE_TABLE.trainer_sub(trainer)
    else:
        jockey_raw = JOCKEY_RATINGS_NORM.get(_norm_jockey(jockey), DEFAULT_JOCKEY_RATING)
        jockey_score = (jockey_raw / 10) * 100
        trainer_score = 50.0  # neutral

    # ── E) Weight score (0–100) ──────────────────────────────
    # Lower weight = advantage; compare to field
    # We'll fill in field_avg_weight after all runners parsed — use placeholder
    weight_lbs = parse_weight_lbs(weight_s)
    # Store raw for normalisation later
    runner["_weight_lbs"] = weight_lbs

    # Placeholder — will be normalised across field after all runners scored
    weight_score = 50.0

    # ── F) Absence penalty ────────────────────────────────────
    absence_penalty = 15.0 if form["long_absence"] else 0.0

    # ── G) Distance suitability ───────────────────────────────
    # Reward form set near today's trip; discount form at a very different
    # distance. Weight is carved out of the recent-form factor so the totals
    # are unchanged; DIST_WEIGHT = 0 makes this a no-op (neutral 50 → cancels).
    cur_f = distance_to_furlongs(distance)
    dist_score, dist_meta = distance_suitability(cur_f, runner.get("_prev_runs") or [])
    form_weight = FORM_WEIGHT_BASE - DIST_WEIGHT

    # ── Combine ───────────────────────────────────────────────
    raw_score = (
        odds_score        * 0.25 +
        form_score        * form_weight +
        consistency_score * 0.15 +
        jockey_score      * strike_rates.JOCKEY_WEIGHT +
        trainer_score     * strike_rates.TRAINER_WEIGHT +
        weight_score      * 0.10 +
        dist_score        * DIST_WEIGHT
    ) - absence_penalty

    # Apply going factor (field-level uncertainty modifier)
    gf = going_factor(going, distance)
    raw_score *= gf

    raw_score = max(0, min(100, raw_score))

    return {
        "_score": raw_score,
        "_components": {
            "odds_value":    round(odds_score, 1),
            "recent_form":   round(form_score, 1),
            "consistency":   round(consistency_score, 1),
            "jockey":        round(jockey_score, 1),
            "trainer":       round(trainer_score, 1),
            "weight":        round(weight_score, 1),
            "absence_pen":   round(absence_penalty, 1),
            "going_factor":  round(gf, 2),
            "distance":      round(dist_score, 1),
            "distance_meta": dist_meta,
        },
        "_form_analysis": form,
    }


def normalise_weight_scores(runners: list) -> None:
    """
    Normalise weight scores across the field.
    Lower weight = higher score. Mutates runners in place.
    """
    weights = [r.get("_weight_lbs") for r in runners if r.get("_weight_lbs")]
    if not weights:
        return
    min_w = min(weights)
    max_w = max(weights)
    rng = max_w - min_w if max_w > min_w else 1

    for r in runners:
        wlbs = r.get("_weight_lbs")
        if wlbs:
            # Lower weight = 100, highest weight = 0, scaled
            w_score = ((max_w - wlbs) / rng) * 100
            # Recalculate score with real weight component
            old_score = r["_score"]
            old_w = r["_components"]["weight"]
            # Replace 50 placeholder with real value
            r["_score"] = old_score - (50 * 0.10) + (w_score * 0.10)
            r["_score"] = max(0, min(100, r["_score"]))
            r["_components"]["weight"] = round(w_score, 1)


def _post_process_win_bets(runners: list, field_size: int) -> None:
    """Ensure at most one Win recommendation per race — the highest-scoring qualifier.

    Runners must already be sorted by score descending. Extra Win picks are
    demoted to Skip (the Each-Way tier was retired — see make_recommendation).
    """
    win_given = False
    for runner in runners:
        rec = runner["recommendation"]
        if rec["type"] != "Win":
            continue
        if win_given:
            score = runner["score"]
            runner["recommendation"] = {
                "type":       "Skip",
                "confidence": max(5, int(score * 0.4)),
                "label":      "Skip",
                "reasoning":  rec["reasoning"],
            }
        else:
            win_given = True


def make_recommendation(score: float, odds_dec: Optional[float],
                         field_size: int, form: dict,
                         ev: Optional[float] = None) -> dict:
    """
    Convert final score + context into a recommendation.

    NB: recommendations are NOT value-gated. A 61-day backtest
    (scripts/backtest_value.py) showed that betting only where the model's
    probability beats the market price *degraded* ROI: the model's edge is in
    AGREEING with the market on strong favourites, and its "value"
    disagreements are mostly the model over-rating a horse. The `ev` argument
    is accepted for the informational model-vs-market readout only.
    """
    # Suppress win bets on horses with high DNF rate (>40%)
    high_dnf = form["dnf_rate"] > 0.40

    # No score compression: 61-day calibration (scripts/calibrate.py) shows
    # win-rate rises monotonically with score — the 60-69 / 70-79 / 80-89
    # bands hit 23% / 32% / 40%, the model's BEST picks. An earlier rule
    # compressed scores above 65 on the opposite (and incorrect) assumption,
    # which demoted genuine Strong Win Bets.
    effective_score = score

    # Longshot guardrail: a high score at long odds means the model disagrees
    # with the market — and the backtest shows the market is usually right, so
    # don't fire a Win bet there.
    market_disagrees = bool(odds_dec and score > 62 and odds_dec > 8.0)

    if effective_score >= 72 and not high_dnf and not market_disagrees and odds_dec and odds_dec <= 5.0:
        return {
            "type":       "Win",
            "confidence": min(95, int(score)),
            "label":      "Strong Win Bet",
            "reasoning":  _reasoning(score, odds_dec, form, "win"),
        }
    elif effective_score >= 60 and not high_dnf and not market_disagrees and odds_dec and odds_dec <= 10.0:
        return {
            "type":       "Win",
            "confidence": min(85, int(score)),
            "label":      "Win Bet",
            "reasoning":  _reasoning(score, odds_dec, form, "win"),
        }
    else:
        # Each-Way tier retired entirely. The 60-day walk-forward backtest
        # (scripts/backtest_value.py --bootstrap) shows the EW tier returned
        # -9.1% ROI out-of-sample [90% CI -16.5%, -1.0%], profitable in only
        # ~3% of day-resamples — a reliable money-loser. Every attempt to
        # rescue a sub-segment failed OOS: no odds band (-5% to -11%), score
        # band (-4% to -16%), nor the "short odds + high score" combination
        # (-9.0%) was profitable. Dropping EW and betting Win only lifts
        # overall staked ROI from -5.8% to -3.8% without regressing any
        # retained tier (Strong Win Bet remains the only OOS-profitable tier,
        # +5.6%). These selections are therefore Skipped, not recommended.
        return {
            "type":       "Skip",
            "confidence": max(5, int(score * 0.4)),
            "label":      "Skip",
            "reasoning":  _reasoning(score, odds_dec, form, "skip"),
        }


def _reasoning(score: float, odds_dec: Optional[float],
               form: dict, rec_type: str) -> str:
    """Generate a short plain-English reasoning string."""
    parts = []

    # Form comment
    rs = form["recent_score"]
    if rs >= 75:
        parts.append("strong recent form")
    elif rs >= 55:
        parts.append("solid recent form")
    elif rs >= 35:
        parts.append("moderate form")
    else:
        parts.append("poor recent form")

    # Last run
    last = form["last_run"]
    if last == 1:
        parts.append("won last time out")
    elif last == 2:
        parts.append("placed last time out")
    elif last == "DNF":
        parts.append("did not finish last time")

    # Consistency
    if form["consistency"] >= 50:
        parts.append(f"consistent placer ({int(form['consistency'])}% placed)")

    # Absence
    if form["long_absence"]:
        parts.append("returning from long absence")

    # DNF warning
    if form["dnf_rate"] > 0.25:
        parts.append(f"notable non-completion rate ({int(form['dnf_rate']*100)}%)")

    # Value
    if odds_dec and rec_type == "win":
        if odds_dec <= 3.0:
            parts.append("short price reflects market confidence")
        elif odds_dec <= 6.0:
            parts.append("fair price for a win bet")

    return "; ".join(parts) if parts else "insufficient data"


# ─────────────────────────────────────────────────────────────
# SPORTING LIFE SCRAPER
# ─────────────────────────────────────────────────────────────
#
# Sporting Life is a Next.js app that embeds the full page data as JSON
# in a <script id="__NEXT_DATA__"> tag — no HTML-table scraping needed.
# Race detail URLs only need the numeric race id; the course/name slug
# segments in the path are ignored by the site, so we don't need to
# reproduce their exact slugging.

UK_IRE_COUNTRIES = {
    'england', 'scotland', 'wales', 'ireland', 'eire',
    'northern ireland', 'republic of ireland',
}


def _next_data(body):
    m = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        body, re.DOTALL
    )
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return None


def _slugify(text):
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", text.lower())).strip("-") or "x"


def get_race_links(today):
    url = f"{BASE_URL}/racing/racecards"
    log(f"Fetching index: {url}")
    body = fetch(url)
    if not body:
        return []

    data = _next_data(body)
    if not data:
        log("Could not find race data on Sporting Life racecards page")
        return []

    meetings = data.get("props", {}).get("pageProps", {}).get("meetings", [])
    races = []
    for mtg in meetings:
        summary = mtg.get("meeting_summary", {})
        course_obj = summary.get("course", {})
        course = course_obj.get("name", "")
        country = course_obj.get("country", {}).get("long_name", "").lower()

        # UK & Ireland only — matches fetch_results.py's filtering so
        # predictions and results stay comparable.
        if not any(c in country for c in UK_IRE_COUNTRIES):
            continue

        meeting_going = summary.get("going", "")
        venue_slug = _slugify(course)

        for race in mtg.get("races", []):
            race_id = race.get("race_summary_reference", {}).get("id")
            if not race_id:
                continue
            name = race.get("name", "")
            races.append({
                "id":                race_id,
                "path":              f"/racing/racecards/{today}/{venue_slug}/racecard/{race_id}/{_slugify(name)}",
                "venue":             course,
                "venue_slug":        venue_slug,
                "date":              today,
                "time":              to_local_time(race.get("time", ""), today),
                "name":              name,
                "distance":          race.get("distance", ""),
                "race_class":        race.get("race_class", ""),
                "going":             race.get("going", meeting_going),
                "declared_runners":  race.get("ride_count", 0),
            })

    log(f"Found {len(races)} race links")
    return races


def scrape_race(meta):
    url = BASE_URL + meta["path"]
    log(f"Scraping {meta['time']} {meta['venue']}: {meta['name']}")
    body = fetch(url)
    if not body:
        return None

    data = _next_data(body)
    if not data:
        log("  No race data found — skipping")
        return None

    race = data.get("props", {}).get("pageProps", {}).get("race", {})
    race_name  = race.get("name") or meta["name"]
    distance   = race.get("distance") or meta["distance"]
    going      = race.get("going") or meta["going"]
    race_class = str(race.get("race_class") or meta["race_class"] or "")
    prizes     = race.get("prizes", {}).get("prize", [])
    prize      = prizes[0]["prize"] if prizes else ""

    runners = []
    for ride in race.get("rides", []):
        # Declared non-runners (withdrawn horses) are KEPT for display but flag-
        # ged so they are never scored, never count toward the field size the
        # odds/each-way factors divide by, and never recommended as a bet — the
        # site greys them out. Absent status is treated as a runner so older or
        # partial feeds still parse.
        is_non_runner = ride.get("ride_status") == "NONRUNNER"

        horse = ride.get("horse", {})
        # Sporting Life's embedded JSON HTML-encodes horse names (e.g.
        # "D&#39;Alboni"); decode so stored names are clean and match the
        # results feed next-day. Jockey/trainer names arrive already decoded.
        horse_raw = html.unescape(horse.get("name", "") or "")
        country_m  = re.search(r"\(([A-Z]{2,3})\)\s*$", horse_raw)
        country    = country_m.group(1) if country_m else "GB"
        horse_name = re.sub(r"\s*\([A-Z]{2,3}\)\s*$", "", horse_raw).strip()

        odds_str = ride.get("betting", {}).get("current_odds") or "SP"

        # Per-run distance history for the distance-suitability factor. Each
        # entry carries the trip (in furlongs) and finishing position of a past
        # run; non-finishers and unparseable distances are dropped. Held under a
        # transient "_prev_runs" key that is stripped before the race is written.
        prev_runs = []
        for pr in (horse.get("previous_results") or []):
            df = distance_to_furlongs(pr.get("distance"))
            pos = pr.get("position")
            if df and isinstance(pos, int) and pos >= 1:
                prev_runs.append({"dist_f": round(df, 2), "pos": pos})

        runners.append({
            "horse":    horse_name,
            "country":  country,
            "jockey":   ride.get("jockey", {}).get("name", ""),
            "trainer":  ride.get("trainer", {}).get("name", ""),
            "form":     horse.get("formsummary", {}).get("display_text", "") or "",
            "age":      str(horse.get("age", "") or ""),
            "weight":   ride.get("handicap", "") or "",
            "draw":     str(ride.get("draw_number", "") or ""),
            "odds_str": odds_str,
            "odds_dec": parse_odds(odds_str),
            "silk_url": ride.get("silk_filename", "") or "",
            "_prev_runs": prev_runs,
            "_non_runner": is_non_runner,
        })

    if not runners:
        log(f"  No runners found — skipping")
        return None

    # Only actual runners are scored and set the field size; non-runners are
    # finalised separately for display and appended at the bottom.
    active = [r for r in runners if not r["_non_runner"]]
    nonrunners = [r for r in runners if r["_non_runner"]]

    # ── Score the active field ─────────────────────────────────
    n = len(active)
    for runner in active:
        result = score_runner(runner, n, going, distance, race_name)
        runner["_score"]      = result["_score"]
        runner["_components"] = result["_components"]
        runner["_form_analysis"] = result["_form_analysis"]

    # Normalise weight scores across field
    normalise_weight_scores(active)

    # Apply per-course accuracy coefficient
    course_factor = COURSE_COEFFICIENTS.get(meta["venue"], 1.0)
    if course_factor != 1.0:
        for runner in active:
            runner["_score"] = max(0, min(100, runner["_score"] * course_factor))

    # Value metrics (model prob vs market price) — needs the whole field.
    compute_value(active)

    # Sort by score descending (best recommendation first)
    active.sort(key=lambda r: r["_score"], reverse=True)

    # Generate final recommendations & clean up internal fields
    for runner in active:
        form_analysis = runner.pop("_form_analysis")
        score         = runner.pop("_score")
        components    = runner.pop("_components")
        weight_lbs    = runner.pop("_weight_lbs", None)
        model_prob    = runner.pop("_model_prob", None)
        market_prob   = runner.pop("_market_prob", None)
        runner.pop("_prev_runs", None)
        runner.pop("_non_runner", None)

        runner["score"]      = round(score, 1)
        runner["components"] = components
        runner["value"] = {
            "model_prob":  round(model_prob, 3) if model_prob is not None else None,
            "market_prob": round(market_prob, 3) if market_prob is not None else None,
            "edge":        round((model_prob - market_prob), 3)
                           if (model_prob is not None and market_prob is not None) else None,
        }
        runner["recommendation"] = make_recommendation(
            score, runner["odds_dec"], n, form_analysis
        )

    # At most one Win bet per race — demote extras to Skip
    _post_process_win_bets(active, n)

    # Non-runners: kept for display, never scored, never a bet.
    for runner in nonrunners:
        _finalise_non_runner(runner)

    return {
        "id":          f"{meta['venue_slug']}-{meta['id']}",
        "course":      meta["venue"],
        "time":        meta["time"],
        "title":       race_name,
        "distance":    distance,
        "going":       going,
        "race_class":  race_class,
        "prize":       prize,
        "num_runners": n,
        "ew_places":   ew_places(n),
        "date":        meta["date"],
        "runners":     active + nonrunners,
    }


def _finalise_non_runner(runner: dict) -> None:
    """Attach display fields to a withdrawn horse (mutates in place).

    A non-runner is shown greyed out with no score and a recommendation that is
    a Skip (so every "is this a bet?" check treats it as not backed) carrying a
    distinct "Non-runner" label and the public `non_runner` flag the site reads.
    """
    runner.pop("_prev_runs", None)
    runner.pop("_non_runner", None)
    runner["score"]      = None
    runner["components"] = {}
    runner["value"]      = {"model_prob": None, "market_prob": None, "edge": None}
    runner["non_runner"] = True
    runner["recommendation"] = {
        "type":       "Skip",
        "confidence": 0,
        "label":      "Non-runner",
        "reasoning":  "withdrawn — will not run",
    }


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="", help="Date YYYY-MM-DD (default: today)")
    parser.add_argument("--out",  default="", help="Output directory (default: ../docs relative to script)")
    args = parser.parse_args()

    today_str = args.date or date.today().strftime("%Y-%m-%d")
    print(f"Scraping races for {today_str}…", file=sys.stderr)

    if args.out:
        out_dir = os.path.abspath(args.out)
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        out_dir = os.path.abspath(os.path.join(script_dir, "..", "docs"))

    os.makedirs(out_dir, exist_ok=True)

    # Build the data-derived jockey/trainer strike-rate table from committed
    # history (only dates strictly before today, so no result can leak in).
    global STRIKE_TABLE
    hist_dir = os.path.join(out_dir, "history")
    STRIKE_TABLE = strike_rates.StrikeTable.build_from_history(hist_dir, before_date=today_str)
    if STRIKE_TABLE.is_populated():
        g = STRIKE_TABLE.glob["jockey"]
        print(f"Strike-rate table: {len(STRIKE_TABLE.rec['jockey'])} jockeys, "
              f"{len(STRIKE_TABLE.rec['trainer'])} trainers from {g[0]} runs",
              file=sys.stderr)
    else:
        print("Strike-rate table empty — falling back to static jockey ratings",
              file=sys.stderr)

    race_metas = get_race_links(today_str)
    races = []
    for i, meta in enumerate(race_metas):
        if i > 0:
            time.sleep(random.uniform(1.5, 2.5))
        race = scrape_race(meta)
        if race:
            races.append(race)

    races.sort(key=lambda r: (r["time"], r["course"]))

    output = {
        "date":         today_str,
        "generated_at": datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + "Z",
        "races":        races,
    }

    out_path = os.path.join(out_dir, "races.json")

    # Never overwrite a richer file for the same date with fewer races.
    # Late-evening re-runs find no upcoming races and would clobber the
    # morning predictions that are needed for next-day accuracy tracking.
    if os.path.exists(out_path):
        try:
            with open(out_path, encoding="utf-8") as f:
                existing = json.load(f)
            if (existing.get("date") == today_str
                    and len(existing.get("races", [])) > len(races)):
                print(
                    f"Keeping existing races.json ({len(existing['races'])} races)"
                    f" — new scrape has only {len(races)}",
                    file=sys.stderr,
                )
                return
        except Exception:
            pass

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"Written: {out_path}", file=sys.stderr)
    print(f"Result:  {len(races)} races, {sum(r['num_runners'] for r in races)} runners", file=sys.stderr)


if __name__ == "__main__":
    main()
