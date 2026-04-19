#!/usr/bin/env python3
"""
Horse Racing Scraper — UK & Ireland
Source: Timeform (confirmed accessible, real horse names, silk images, odds)

Usage:
    python3 scripts/scrape_races.py
    python3 scripts/scrape_races.py --date 2026-04-20
    python3 scripts/scrape_races.py --out /path/to/docs
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


def recommend(odds_dec, position, field_size):
    if odds_dec is None or field_size == 0:
        return {"type": "Skip", "confidence": 0, "label": "No price available"}
    implied_prob = 1 / odds_dec
    avg_prob = 1 / field_size
    ratio = implied_prob / avg_prob
    if odds_dec <= 2.5 and ratio >= 1.5:
        return {"type": "Win", "confidence": 88, "label": "Strong Win Bet"}
    if odds_dec <= 4.0 and ratio >= 1.25:
        return {"type": "Win", "confidence": 72, "label": "Win Bet"}
    if odds_dec <= 6.0 and field_size >= 5:
        return {"type": "EachWay", "confidence": 62, "label": "Each Way"}
    if odds_dec <= 10.0 and field_size >= 6:
        return {"type": "EachWay", "confidence": 50, "label": "Each Way"}
    if odds_dec <= 16.0 and field_size >= 8:
        return {"type": "EachWay", "confidence": 38, "label": "Each Way (Speculative)"}
    return {"type": "Skip", "confidence": 18, "label": "Skip / Saver only"}


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
            "path": path,
            "venue": venue,
            "venue_slug": venue_slug,
            "date": today,
            "time_code": time_code,
            "time": f"{time_code[:2]}:{time_code[2:]}",
            "declared_runners": int(runners_n),
            "name_slug": name_slug,
        })

    log(f"Found {len(races)} race links")
    return races


def scrape_race(meta):
    url = BASE_URL + meta["path"]
    log(f"Scraping {meta['time']} {meta['venue']}: {meta['name_slug']}")
    body = fetch(url)
    if not body:
        return None

    # Race name from slug (prettified)
    race_name = meta["name_slug"].replace("-", " ").title()

    # Distance
    dist_m = re.search(r"(\d+m\s*(?:\d+f)?|\d+f)", body[40000:80000])
    distance = dist_m.group(1).strip() if dist_m else ""

    # Going
    going_m = re.search(r"Going[:\s]+([A-Za-z][\w\s]*?)[\r\n<(]", body[40000:80000])
    going = going_m.group(1).strip() if going_m else ""

    # Prize
    prize_m = re.search(r"[£€][\d,]+", body[40000:80000])
    prize = prize_m.group(0) if prize_m else ""

    # Class from race name
    class_m = re.search(r"\((\d)\)\s*$", race_name)
    race_class = class_m.group(1) if class_m else ""

    # Runner rows
    horse_rows = re.findall(
        r'<tbody[^>]*class="[^"]*rp-horse-row[^"]*"[^>]*>(.*?)</tbody>',
        body, re.DOTALL
    )

    runners = []
    for row in horse_rows:
        name_m = re.search(r'class="rp-horse"[^>]*>([^<]+)</a>', row)
        if not name_m:
            continue
        horse_raw = name_m.group(1).strip()
        # Strip country suffix like (IRE) but record it
        horse_name = re.sub(r"\s*\([A-Z]{2,3}\)\s*$", "", horse_raw).strip()
        country_m = re.search(r"\(([A-Z]{2,3})\)\s*$", horse_raw)
        country = country_m.group(1) if country_m else "GB"

        jockey_m = re.search(
            r'rp-td-horse-jockey[^>]*>.*?href="[^"]+"[^>]*>([^<]+)</a>', row, re.DOTALL)
        jockey = clean(jockey_m.group(1)) if jockey_m else ""

        trainer_m = re.search(
            r'rp-td-horse-trainer.*?href="[^"]+"[^>]*>([^<]+)</a>', row, re.DOTALL)
        trainer = clean(trainer_m.group(1)) if trainer_m else ""

        form_m  = re.search(r'rp-td-horse-form"[^>]*>([^<]+)<', row)
        age_m   = re.search(r'rp-td-horse-age[^"]*"[^>]*>([^<]+)<', row)
        wgt_m   = re.search(r'rp-td-horse-weight"[^>]*>([^<]+)<', row)
        draw_m  = re.search(r'rp-td-horse-draw[^"]*"[^>]*>([^<]+)<', row)

        form    = clean(form_m.group(1))  if form_m  else ""
        age     = clean(age_m.group(1))   if age_m   else ""
        weight  = clean(wgt_m.group(1))   if wgt_m   else ""
        draw    = clean(draw_m.group(1))  if draw_m  else ""

        # Real silk image from Timeform CDN
        silk_m  = re.search(r'rp-silks"[^>]+src="([^"]+)"', row)
        silk_url = silk_m.group(1) if silk_m else ""

        # Odds: prefer data-price decimal, fall back to fractional text
        frac_m  = re.search(r'price-fractional">([^<]+)</span>', row)
        odds_str = clean(frac_m.group(1)) if frac_m else "SP"

        dec_m = re.search(r'data-price="([^"]+)"', row)
        if dec_m:
            try:
                odds_dec = round(float(dec_m.group(1)), 2)
            except ValueError:
                odds_dec = parse_odds(odds_str)
        else:
            odds_dec = parse_odds(odds_str)

        runners.append({
            "horse":   horse_name,
            "country": country,
            "jockey":  jockey,
            "trainer": trainer,
            "form":    form,
            "age":     age,
            "weight":  weight,
            "draw":    draw,
            "odds_str": odds_str,
            "odds_dec": odds_dec,
            "silk_url": silk_url,
        })

    if not runners:
        log(f"  No runners parsed — skipping {meta['venue']} {meta['time']}")
        return None

    # Sort by price (favourites first)
    runners.sort(key=lambda r: r["odds_dec"] if r["odds_dec"] else 9999)

    n = len(runners)
    for i, r in enumerate(runners):
        r["recommendation"] = recommend(r["odds_dec"], i + 1, n)

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
        "date":        meta["date"],
        "runners":     runners,
    }


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
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "races":        races,
    }

    out_path = os.path.join(out_dir, "races.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"Written: {out_path}", file=sys.stderr)
    print(f"Result:  {len(races)} races, {sum(r['num_runners'] for r in races)} runners", file=sys.stderr)


if __name__ == "__main__":
    main()
