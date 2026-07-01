#!/usr/bin/env python3
"""Diagnostic-only: inspect candidate free racecard sources for real
server-rendered data vs JS-only shells. Not part of the scraper — delete after use."""
import re
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
    "https://www.skysports.com/racing/racecards",
    "https://www.racingtv.com/racecards",
]

for url in CANDIDATES:
    print(f"=== {url} ===")
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  ERROR: {type(e).__name__}: {e}")
        continue

    print(f"  length: {len(body)}")
    times = re.findall(r'\b([01]\d|2[0-3]):[0-5]\d\b', body)
    print(f"  time-like tokens found: {len(times)} sample={times[:8]}")

    next_data = re.search(r'__NEXT_DATA__[^>]*>(.*?)</script>', body, re.DOTALL)
    nuxt_data = re.search(r'__NUXT__', body)
    apollo = re.search(r'__APOLLO_STATE__', body)
    print(f"  __NEXT_DATA__ present: {bool(next_data)} (len={len(next_data.group(1)) if next_data else 0})")
    print(f"  __NUXT__ present: {bool(nuxt_data)}")
    print(f"  __APOLLO_STATE__ present: {bool(apollo)}")

    for kw in ("jockey", "trainer", "racecard", "meeting", "going"):
        print(f"  count '{kw}': {body.lower().count(kw)}")

    # dump a small snippet around first time-like token as sanity check
    if times:
        idx = body.find(times[0] + ":")
        # find full match position instead
        m = re.search(r'([01]\d|2[0-3]):[0-5]\d', body)
        if m:
            s = max(0, m.start() - 100)
            print(f"  snippet: {body[s:m.start()+150]!r}")
    print()
