#!/usr/bin/env python3
"""Build a self-contained, offline copy of the site.

Mobile browsers block fetch() of local files when a page is opened from the
filesystem (file://), so the live horses page — which fetches races.json and
accuracy.json — breaks offline. This script inlines that data and installs a
tiny fetch shim so the page works with zero network access. Save the output
folder to your phone and open home.html with no connection.

Usage:
    python3 scripts/build_offline.py --out offline
"""
import argparse
import json
import os
import shutil


def read(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


def build(root, out):
    horses_html = read(os.path.join(root, "horses", "index.html"))
    home_html = read(os.path.join(root, "index.html"))

    # Load the two data files the horses page fetches at runtime.
    data = {}
    for name in ("races.json", "accuracy.json"):
        p = os.path.join(root, "horses", name)
        if os.path.exists(p):
            data[name] = json.loads(read(p))

    # A fetch shim: intercept the local .json requests and serve the inlined
    # data. Everything else (there is nothing else local) falls through.
    shim = (
        "<script id=\"offline-data\">\n"
        "window.__OFFLINE__ = " + json.dumps(data, separators=(",", ":")) + ";\n"
        "(function(){\n"
        "  var orig = window.fetch ? window.fetch.bind(window) : null;\n"
        "  window.fetch = function(url){\n"
        "    var key = String(url).split('?')[0].replace(/^.*\\//,'');\n"
        "    if (Object.prototype.hasOwnProperty.call(window.__OFFLINE__, key)) {\n"
        "      var body = JSON.stringify(window.__OFFLINE__[key]);\n"
        "      return Promise.resolve(new Response(body, {status:200,\n"
        "        headers:{'Content-Type':'application/json'}}));\n"
        "    }\n"
        "    return orig ? orig.apply(this, arguments)\n"
        "                : Promise.reject(new Error('offline: ' + key));\n"
        "  };\n"
        "})();\n"
        "</script>\n"
    )

    # Inject the shim right after <body> so it runs before the page's own
    # scripts call fetch().
    horses_offline = horses_html.replace("<body>", "<body>\n" + shim, 1)

    # Two flat, standalone files — the pages don't cross-link and each carries
    # its own inline CSS, so there's nothing to unzip or wire up on a phone.
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "home.html"), "w", encoding="utf-8") as f:
        f.write(home_html)
    with open(os.path.join(out, "race-day.html"), "w", encoding="utf-8") as f:
        f.write(horses_offline)

    inlined = ", ".join(sorted(data)) or "no data files found"
    print("Built offline site in %s/ (inlined: %s)" % (out, inlined))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="offline")
    ap.add_argument("--root", default=os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    args = ap.parse_args()
    build(args.root, args.out)
