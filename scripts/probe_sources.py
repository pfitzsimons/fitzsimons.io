#!/usr/bin/env python3
"""Diagnostic-only: inspect Sporting Life's __NEXT_DATA__ structure for
odds/form/weight fields. Not part of the scraper — delete after use."""
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
if not m:
    print("NEXT_DATA script tag not found with that exact id, searching loosely...")
    m = re.search(r'__NEXT_DATA__[^>]*>(.*?)</script>', body, re.DOTALL)

data = json.loads(m.group(1))

def walk_keys(obj, path="", depth=0, max_depth=5):
    if depth > max_depth:
        return
    if isinstance(obj, dict):
        print("  " * depth + f"{path} keys: {list(obj.keys())[:20]}")
        for k, v in list(obj.items())[:6]:
            walk_keys(v, f"{path}.{k}", depth + 1, max_depth)
    elif isinstance(obj, list) and obj:
        print("  " * depth + f"{path} [list len={len(obj)}]")
        walk_keys(obj[0], f"{path}[0]", depth + 1, max_depth)

walk_keys(data, "root", 0, 4)

# Try to find anything odds-related anywhere in the JSON
blob = json.dumps(data)
for kw in ("odds", "price", "fraction", "decimal", "betting", "\"form\"", "weight", "\"going\""):
    print(f"count {kw!r}: {blob.lower().count(kw.lower())}")
