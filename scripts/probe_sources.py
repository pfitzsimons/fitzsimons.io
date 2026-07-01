#!/usr/bin/env python3
"""Diagnostic-only: find per-race URL pattern on Sporting Life and confirm
individual race pages expose full runner (rides) data. Delete after use."""
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

# Find hrefs that look like race links
hrefs = re.findall(r'href="(/racing/racecards/[^"]+)"', body)
uniq = list(dict.fromkeys(hrefs))
print(f"found {len(uniq)} unique racecards hrefs, sample:")
for h in uniq[:15]:
    print(" ", h)

if not uniq:
    print("no hrefs found, dumping a chunk of body around 'Thirsk'")
    idx = body.find("Thirsk")
    print(body[max(0, idx-500):idx+500])
    raise SystemExit

# pick a race link that looks like a specific race (not a meeting index)
race_links = [h for h in uniq if re.search(r'/racecards/\d{4}-\d{2}-\d{2}/', h)]
print(f"\nrace-like links: {len(race_links)}")
target = race_links[0] if race_links else uniq[0]
full_url = "https://www.sportinglife.com" + target
print(f"\nFetching individual race page: {full_url}")

req2 = urllib.request.Request(full_url, headers=HEADERS)
with urllib.request.urlopen(req2, timeout=20) as resp2:
    body2 = resp2.read().decode("utf-8", errors="replace")

m = re.search(
    r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
    body2, re.DOTALL
)
if not m:
    print("no NEXT_DATA on race page")
    raise SystemExit

data2 = json.loads(m.group(1))
pp = data2["props"]["pageProps"]
print("pageProps keys:", list(pp.keys()))
blob = json.dumps(pp)
print("len:", len(blob))
print("has 'rides':", "\"rides\"" in blob)
print("has 'betting_forecast':", "betting_forecast" in blob)

# try to locate the race object with rides
def find_rides(obj, path=""):
    if isinstance(obj, dict):
        if "rides" in obj:
            print(f"FOUND rides at {path}, count={len(obj['rides'])}")
            if obj["rides"]:
                print("first ride keys:", list(obj["rides"][0].keys()))
        for k, v in obj.items():
            find_rides(v, f"{path}.{k}")
    elif isinstance(obj, list):
        for i, v in enumerate(obj[:3]):
            find_rides(v, f"{path}[{i}]")

find_rides(pp, "pageProps")
