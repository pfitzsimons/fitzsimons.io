#!/usr/bin/env python3
"""Diagnostic-only: check which free UK/IRE racecard sources are reachable
without hitting bot protection. Not part of the scraper — delete after use."""
import urllib.request
import urllib.error

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}

CANDIDATES = [
    "https://www.sportinglife.com/racing/racecards",
    "https://www.attheraces.com/racecards",
    "https://www.skysports.com/racing/racecards",
    "https://www.bbc.co.uk/sport/horse-racing/racecards",
    "https://www.itv.com/racing/racecards",
    "https://www.racingtv.com/racecards",
    "https://www.racingpost.com/racecards/",
    "https://www.oddschecker.com/horse-racing",
    "https://www.timeform.com/horse-racing/racecards",
]

for url in CANDIDATES:
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read(2000).decode("utf-8", errors="replace")
            marker = "WAF" if "waf" in body.lower() or "challenge" in body.lower() else "OK"
            print(f"{resp.status} {marker:5s} {url}")
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read(300).decode("utf-8", errors="replace")
        except Exception:
            pass
        print(f"{e.code} FAIL  {url}  {body[:150]!r}")
    except Exception as e:
        print(f"ERR  FAIL  {url}  {type(e).__name__}: {e}")
