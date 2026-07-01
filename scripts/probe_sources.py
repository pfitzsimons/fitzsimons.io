#!/usr/bin/env python3
"""Diagnostic-only: dump full structure of one race + one runner from
Sporting Life's NEXT_DATA to design the real scraper. Delete after use."""
import json
import re
import urllib.request

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}

url = "https://www.sportinglife.com/racing/racecards"
req = urllib.request.Request(url, headers=HEADERS)
with urllib.request.urlopen(req, timeout=20) as resp:
    body = resp.read().decode("utf-8", errors="replace")

m = re.search(
    r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
    body, re.DOTALL
)
data = json.loads(m.group(1))

props = data["props"]["pageProps"]
meetings = props.get("meetings", [])
print(f"num meetings: {len(meetings)}")

meeting0 = meetings[0]
print("meeting_summary:", json.dumps(meeting0["meeting_summary"], indent=None)[:800])
print("meeting0 races count:", len(meeting0.get("races", [])))
if meeting0.get("races"):
    race0 = meeting0["races"][0]
    print("race0 keys:", list(race0.keys()))
    print("race0 (trunc):", json.dumps(race0, indent=None)[:2000])

print()
print("=== nextTenRaces[0] full dump (trunc 3000) ===")
if props.get("nextTenRaces"):
    r = props["nextTenRaces"][0]
    print(json.dumps(r, indent=None)[:3000])
    rides = r.get("rides", [])
    print(f"\nrides count: {len(rides)}")
    if rides:
        print("ride0 keys:", list(rides[0].keys()))
        print("ride0 full:", json.dumps(rides[0], indent=2)[:2500])
