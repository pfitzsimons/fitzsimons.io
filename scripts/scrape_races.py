#!/usr/bin/env python3
"""
Horse Racing Scraper — UK & Ireland
Source: Timeform

Multi-factor scoring model:
  1. Recent form score     — weighted last 5 runs, recency-boosted
  2. Consistency score     — how often horse finishes placed
  3. Odds value score      — implied prob vs field average
  4. Weight-for-age score  — lower weight = advantage, especially in handicaps
  5. Going suitability     — heavier going favours stayers/jumpers
  6. Jockey quality        — top jockeys rated by known record
  7. Recency penalty       — long abs gaps (/) penalised
  8. DNF penalty           — P (pulled up), F (fell), U (unseated), B (bolted)

Final score 0–100 → label + confidence + Win/EachWay/Skip
"""

import argparse
import gzip
import html
import json
import os
import re
import sys
import time
import random
from datetime import date, datetime
from typing import Optional
import urllib.request
import urllib.error

BASE_URL = "https://www.timeform.com"

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


# ─────────────────────────────────────────────────────────────
# HTTP
# ─────────────────────────────────────────────────────────────

def fetch(url):
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            if resp.headers.get("Content-Encoding", "") == "gzip":
                raw = gzip.decompress(raw)
            return raw.decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        log(f"HTTP {e.code} -> {url}")
    except Exception as e:
        log(f"{type(e).__name__} -> {url}: {e}")
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
    Parse Timeform form string into structured signals.

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
      B) Recent form         30%  — weighted last 5 runs
      C) Consistency         15%  — placed% across all known runs
      D) Jockey quality      15%  — rated jockey list
      E) Weight advantage    10%  — relative weight vs field average
      F) Absence penalty      5%  — penalise '/' in form (long gap)
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

    # ── D) Jockey quality (0–100) ────────────────────────────
    jockey_raw = JOCKEY_RATINGS.get(jockey, DEFAULT_JOCKEY_RATING)
    jockey_score = (jockey_raw / 10) * 100

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

    # ── Combine ───────────────────────────────────────────────
    raw_score = (
        odds_score        * 0.25 +
        form_score        * 0.30 +
        consistency_score * 0.15 +
        jockey_score      * 0.15 +
        weight_score      * 0.10
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
            "weight":        round(weight_score, 1),
            "absence_pen":   round(absence_penalty, 1),
            "going_factor":  round(gf, 2),
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


def make_recommendation(score: float, odds_dec: Optional[float],
                         field_size: int, form: dict) -> dict:
    """
    Convert final score + context into a recommendation.
    """
    places = ew_places(field_size)

    # Suppress win bets on horses with high DNF rate (>40%)
    high_dnf = form["dnf_rate"] > 0.40

    if score >= 72 and not high_dnf and odds_dec and odds_dec <= 6.0:
        return {
            "type":       "Win",
            "confidence": min(95, int(score)),
            "label":      "Strong Win Bet",
            "reasoning":  _reasoning(score, odds_dec, form, "win"),
        }
    elif score >= 60 and not high_dnf and odds_dec and odds_dec <= 10.0:
        return {
            "type":       "Win",
            "confidence": min(85, int(score)),
            "label":      "Win Bet",
            "reasoning":  _reasoning(score, odds_dec, form, "win"),
        }
    elif score >= 50 and places >= 2 and odds_dec and odds_dec <= 16.0:
        label = "Each Way" if score >= 58 else "Each Way (Speculative)"
        return {
            "type":       "EachWay",
            "confidence": min(75, int(score)),
            "label":      label,
            "reasoning":  _reasoning(score, odds_dec, form, "ew"),
        }
    else:
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
            parts.append("each way option also viable")

    return "; ".join(parts) if parts else "insufficient data"


# ─────────────────────────────────────────────────────────────
# TIMEFORM SCRAPER
# ─────────────────────────────────────────────────────────────

def get_race_links(today):
    url = f"{BASE_URL}/horse-racing/racecards"
    log(f"Fetching index: {url}")
    body = fetch(url)
    if not body:
        return []

    pattern = re.compile(
        r'href="(/horse-racing/racecards/(?!meeting-summary)([^/"]+)/('
        + re.escape(today)
        + r')/(\d{4})/(\d+)/(\d+)/([^"]+))"'
    )

    seen = set()
    races = []
    for m in pattern.finditer(body):
        path       = m.group(1)
        venue_slug = m.group(2)
        time_code  = m.group(4)
        runners_n  = m.group(6)
        name_slug  = m.group(7)

        if path in seen:
            continue
        seen.add(path)

        venue = venue_slug.replace("-", " ").title()
        races.append({
            "path":              path,
            "venue":             venue,
            "venue_slug":        venue_slug,
            "date":              today,
            "time_code":         time_code,
            "time":              f"{time_code[:2]}:{time_code[2:]}",
            "declared_runners":  int(runners_n),
            "name_slug":         name_slug,
        })

    log(f"Found {len(races)} race links")
    return races


def scrape_race(meta):
    url = BASE_URL + meta["path"]
    log(f"Scraping {meta['time']} {meta['venue']}: {meta['name_slug']}")
    body = fetch(url)
    if not body:
        return None

    race_name = meta["name_slug"].replace("-", " ").title()
    dist_m    = re.search(r"(\d+m\s*(?:\d+f)?|\d+f)", body[40000:80000])
    going_m   = re.search(r"Going[:\s]+([A-Za-z][\w\s]*?)[\r\n<(]", body[40000:80000])
    prize_m   = re.search(r"[£€][\d,]+", body[40000:80000])
    class_m   = re.search(r"\((\d)\)\s*$", race_name)

    distance   = dist_m.group(1).strip()  if dist_m   else ""
    going      = going_m.group(1).strip() if going_m  else ""
    prize      = prize_m.group(0)         if prize_m  else ""
    race_class = class_m.group(1)         if class_m  else ""

    horse_rows = re.findall(
        r'<tbody[^>]*class="[^"]*rp-horse-row[^"]*"[^>]*>(.*?)</tbody>',
        body, re.DOTALL
    )

    runners = []
    for row in horse_rows:
        name_m = re.search(r'class="rp-horse"[^>]*>([^<]+)</a>', row)
        if not name_m:
            continue
        horse_raw  = name_m.group(1).strip()
        horse_name = re.sub(r"\s*\([A-Z]{2,3}\)\s*$", "", horse_raw).strip()
        country_m  = re.search(r"\(([A-Z]{2,3})\)\s*$", horse_raw)
        country    = country_m.group(1) if country_m else "GB"

        jockey_m  = re.search(r'rp-td-horse-jockey[^>]*>.*?href="[^"]+"[^>]*>([^<]+)</a>', row, re.DOTALL)
        trainer_m = re.search(r'rp-td-horse-trainer.*?href="[^"]+"[^>]*>([^<]+)</a>', row, re.DOTALL)
        form_m    = re.search(r'rp-td-horse-form"[^>]*>([^<]+)<', row)
        age_m     = re.search(r'rp-td-horse-age[^"]*"[^>]*>([^<]+)<', row)
        wgt_m     = re.search(r'rp-td-horse-weight"[^>]*>([^<]+)<', row)
        draw_m    = re.search(r'rp-td-horse-draw[^"]*"[^>]*>([^<]+)<', row)
        silk_m    = re.search(r'rp-silks"[^>]+src="([^"]+)"', row)
        frac_m    = re.search(r'price-fractional">([^<]+)</span>', row)
        dec_m     = re.search(r'data-price="([^"]+)"', row)

        jockey  = clean(jockey_m.group(1))  if jockey_m  else ""
        trainer = clean(trainer_m.group(1)) if trainer_m else ""
        form    = clean(form_m.group(1))    if form_m    else ""
        age     = clean(age_m.group(1))     if age_m     else ""
        weight  = clean(wgt_m.group(1))     if wgt_m     else ""
        draw    = clean(draw_m.group(1))    if draw_m    else ""
        silk_url = silk_m.group(1)          if silk_m    else ""
        odds_str = clean(frac_m.group(1))   if frac_m    else "SP"

        if dec_m:
            try:
                odds_dec = round(float(dec_m.group(1)), 2)
            except ValueError:
                odds_dec = parse_odds(odds_str)
        else:
            odds_dec = parse_odds(odds_str)

        runners.append({
            "horse":    horse_name,
            "country":  country,
            "jockey":   jockey,
            "trainer":  trainer,
            "form":     form,
            "age":      age,
            "weight":   weight,
            "draw":     draw,
            "odds_str": odds_str,
            "odds_dec": odds_dec,
            "silk_url": silk_url,
        })

    if not runners:
        log(f"  No runners found — skipping")
        return None

    # ── Score all runners ──────────────────────────────────────
    n = len(runners)
    for runner in runners:
        result = score_runner(runner, n, going, distance, race_name)
        runner["_score"]      = result["_score"]
        runner["_components"] = result["_components"]
        runner["_form_analysis"] = result["_form_analysis"]

    # Normalise weight scores across field
    normalise_weight_scores(runners)

    # Sort by score descending (best recommendation first)
    runners.sort(key=lambda r: r["_score"], reverse=True)

    # Generate final recommendations & clean up internal fields
    for i, runner in enumerate(runners):
        form_analysis = runner.pop("_form_analysis")
        score         = runner.pop("_score")
        components    = runner.pop("_components")
        weight_lbs    = runner.pop("_weight_lbs", None)

        runner["score"]      = round(score, 1)
        runner["components"] = components
        runner["recommendation"] = make_recommendation(
            score, runner["odds_dec"], n, form_analysis
        )

    return {
        "id":          f"{meta['venue_slug']}-{meta['time_code']}",
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
        "runners":     runners,
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
        "generated_at": datetime.now(datetime.timezone.utc if hasattr(datetime, 'timezone') else None).isoformat() + "Z"
                        if False else datetime.utcnow().isoformat() + "Z",
        "races":        races,
    }

    out_path = os.path.join(out_dir, "races.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"Written: {out_path}", file=sys.stderr)
    print(f"Result:  {len(races)} races, {sum(r['num_runners'] for r in races)} runners", file=sys.stderr)


if __name__ == "__main__":
    main()
