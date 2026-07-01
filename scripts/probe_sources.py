#!/usr/bin/env python3
"""Diagnostic-only: verify race page URLs are slug-agnostic (only the numeric
id matters), and check course-slug derivation from meeting data. Delete after use."""
import json
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


def fetch(url):
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")


# Known-good race from earlier probe: id 925342, course thirsk, date 2026-07-01
wrong_slug_url = "https://www.sportinglife.com/racing/racecards/2026-07-01/thirsk/racecard/925342/completely-wrong-slug-xyz"
status, body = fetch(wrong_slug_url)
print(f"wrong-slug URL status: {status}, len={len(body)}")
m = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', body, re.DOTALL)
print("has NEXT_DATA:", bool(m))
if m:
    data = json.loads(m.group(1))
    race = data["props"]["pageProps"].get("race", {})
    print("race name via wrong slug:", race.get("name") if isinstance(race, dict) else "N/A")
    print("rides count:", len(race.get("rides", [])) if isinstance(race, dict) else "N/A")

# Now check: can we omit course slug too, using a dummy course name?
wrong_course_url = "https://www.sportinglife.com/racing/racecards/2026-07-01/not-a-real-course/racecard/925342/whatever"
status2, body2 = fetch(wrong_course_url)
print(f"\nwrong-course URL status: {status2}, len={len(body2)}")

# Check meeting_summary course name -> what slug format is expected
main_url = "https://www.sportinglife.com/racing/racecards"
_, main_body = fetch(main_url)
m2 = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', main_body, re.DOTALL)
data2 = json.loads(m2.group(1))
meetings = data2["props"]["pageProps"]["meetings"]
print(f"\ntotal meetings: {len(meetings)}")
for mt in meetings[:5]:
    course = mt["meeting_summary"]["course"]["name"]
    races = mt.get("races", [])
    print(f"  course={course!r} races={len(races)}")
    if races:
        r0 = races[0]
        print(f"    race0: id={r0['race_summary_reference']['id']} name={r0['name']!r} time={r0['time']}")
