#!/usr/bin/env python3
"""
Two hosts (GitHub Actions and the Mac) run scripts/run_pipeline.sh against the
same origin. This checks, offline, that they can't corrupt each other:

  1. A push that loses the race is redone on top of origin, so BOTH scrapes'
     data ends up in races.json (no lost update, no text-merged JSON).
  2. The start-of-day archive the first host pushed is never overwritten by
     the second host's own (later) archive.
  3. A clone left with an unpushed commit (failed push) whose races.json
     conflicts with origin recovers by itself: it resets to origin and carries
     over only a history/ file origin lacks.
  4. The results job counts as done when yesterday's results_full file is
     already on origin, even from a fresh clone with no marker.

It uses a temporary bare repo and stub scrape/results scripts; the real
run_pipeline.sh is run with PIPELINE_REPO_DIR pointing at each clone.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
from datetime import date, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
PIPELINE = os.path.join(HERE, "run_pipeline.sh")

# Stub scraper: merges this host's name into races.json (like the real
# intraday merge) and freezes the archive only if absent. If $HOOK is set it
# runs that command mid-scrape: the "other host pushed meanwhile" race.
STUB_SCRAPE = textwrap.dedent('''\
    import json, os, subprocess, sys
    out = sys.argv[sys.argv.index("--out") + 1]
    today = "2026-01-01"
    host = os.environ["HOST"]
    live = os.path.join(out, "races.json")
    races = json.load(open(live))["races"] if os.path.exists(live) else []
    if host not in races:
        races.append(host)
    arch = os.path.join(out, "history", f"races_{today}.json")
    if not os.path.exists(arch):
        os.makedirs(os.path.dirname(arch), exist_ok=True)
        json.dump({"date": today, "races": [f"archive-by-{host}"]}, open(arch, "w"))
    hook = os.environ.pop("HOOK", "")
    if hook:
        subprocess.run(hook, shell=True, check=True)
    json.dump({"date": today, "races": races}, open(live, "w"), indent=2)
''')

STUB_RESULTS = textwrap.dedent('''\
    import sys
    sys.exit("fetch_results stub should not have run")
''')


def git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, check=True,
                          capture_output=True, text=True).stdout.strip()


def env_for(host, clone, state, hook=""):
    e = dict(os.environ)
    e.update({
        "HOST": host, "HOOK": hook,
        "PIPELINE_REPO_DIR": clone,
        "PIPELINE_STATE_DIR": state,
        "PIPELINE_PYTHON": sys.executable,
        "PIPELINE_LOCK_WAIT": "0",
        "GITHUB_ACTIONS": "true",           # log to stdout
        "GIT_AUTHOR_NAME": host, "GIT_AUTHOR_EMAIL": f"{host}@test",
        "GIT_COMMITTER_NAME": host, "GIT_COMMITTER_EMAIL": f"{host}@test",
    })
    return e


def run(host, clone, state, *args, hook=""):
    p = subprocess.run(["bash", PIPELINE, *args], env=env_for(host, clone, state, hook),
                       capture_output=True, text=True)
    return p.returncode, p.stdout + p.stderr


def origin_json(bare, path):
    return json.loads(git(bare, "show", f"main:{path}"))


def setup(tmp):
    bare = os.path.join(tmp, "origin.git")
    git(tmp, "init", "-q", "--bare", "-b", "main", bare)
    seed = os.path.join(tmp, "seed")
    git(tmp, "clone", "-q", bare, seed)
    os.makedirs(os.path.join(seed, "scripts"))
    os.makedirs(os.path.join(seed, "horses", "history"))
    open(os.path.join(seed, "scripts", "scrape_races.py"), "w").write(STUB_SCRAPE)
    open(os.path.join(seed, "scripts", "fetch_results.py"), "w").write(STUB_RESULTS)
    json.dump({"date": "2026-01-01", "races": []}, open(os.path.join(seed, "horses", "races.json"), "w"))
    open(os.path.join(seed, "horses", "history", ".keep"), "w").close()
    git(seed, "add", "-A")
    git(seed, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "seed")
    git(seed, "push", "-q", "origin", "main")
    clones = {}
    for host in ("mac", "actions"):
        c = os.path.join(tmp, host)
        git(tmp, "clone", "-q", bare, c)
        clones[host] = (c, os.path.join(tmp, f"state-{host}"))
    return bare, clones


def check(cond, msg, log=""):
    if not cond:
        print(f"FAIL: {msg}\n{log}")
        sys.exit(1)
    print(f"ok  {msg}")


def test_race_redo(tmp):
    bare, clones = setup(tmp)
    mac, actions = clones["mac"], clones["actions"]
    # Actions scrapes; mid-scrape the Mac runs a full scrape and pushes first.
    hook = (f"HOST=mac HOOK= PIPELINE_REPO_DIR={mac[0]} PIPELINE_STATE_DIR={mac[1]} "
            f"GIT_AUTHOR_NAME=mac GIT_COMMITTER_NAME=mac bash {PIPELINE} scrape")
    rc, log = run("actions", *actions, "scrape", hook=hook)
    check(rc == 0, "losing push is redone, run succeeds", log)
    check("redoing on top" in log, "redo path was exercised", log)
    races = origin_json(bare, "horses/races.json")["races"]
    check(sorted(races) == ["actions", "mac"], f"both hosts' data kept in races.json ({races})", log)
    arch = origin_json(bare, "horses/history/races_2026-01-01.json")["races"]
    check(arch == ["archive-by-mac"], "first-pushed start-of-day archive left frozen", log)


def test_stale_local_commit(tmp):
    bare, clones = setup(tmp)
    mac, actions = clones["mac"], clones["actions"]
    # Mac makes a commit that never gets pushed: a new archive for another day
    # plus a races.json edit that will conflict.
    c = mac[0]
    json.dump({"date": "2025-12-31", "races": ["stale"]}, open(f"{c}/horses/races.json", "w"))
    json.dump({"date": "2025-12-31", "races": ["kept"]}, open(f"{c}/horses/history/races_2025-12-31.json", "w"))
    git(c, "add", "-A")
    git(c, "-c", "user.name=mac", "-c", "user.email=m@t", "commit", "-q", "-m", "unpushed")
    # Meanwhile Actions pushes.
    rc, log = run("actions", *actions, "scrape")
    check(rc == 0, "actions scrape pushes", log)
    rc, log = run("mac", *mac, "scrape")
    check(rc == 0, "mac recovers from a conflicting unpushed commit without manual fixes", log)
    races = origin_json(bare, "horses/races.json")["races"]
    check(sorted(races) == ["actions", "mac"], f"origin's races.json won, mac merged on top ({races})", log)
    check(origin_json(bare, "horses/history/races_2025-12-31.json")["races"] == ["kept"],
          "history file origin lacked was carried over and pushed", log)


def test_results_done_from_repo(tmp):
    bare, clones = setup(tmp)
    c, state = clones["actions"]
    yday = (date.today() - timedelta(days=1)).isoformat()
    open(f"{c}/horses/history/results_full_{yday}.json", "w").write("{}")
    git(c, "add", "-A")
    git(c, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "results")
    git(c, "push", "-q", "origin", "main")
    fresh = os.path.join(tmp, "fresh")
    git(tmp, "clone", "-q", bare, fresh)
    rc, log = run_results_check(fresh, os.path.join(tmp, "state-fresh"))
    check(rc == 0 and "already graded" in log, "results skipped when origin already has yesterday", log)


def run_results_check(clone, state):
    # Run main() at a fake 10:00 UTC (results due) by exporting a bash `date`
    # function, which wins over PATH lookups inside run_pipeline.sh.
    wrapper = ('date() { if [ "$*" = "-u +%H" ]; then echo 10; else command date "$@"; fi; }; '
               f'export -f date; exec bash {PIPELINE}')
    p = subprocess.run(["bash", "-c", wrapper], env=env_for("actions", clone, state),
                       capture_output=True, text=True)
    return p.returncode, p.stdout + p.stderr


if __name__ == "__main__":
    for t in (test_race_redo, test_stale_local_commit, test_results_done_from_repo):
        tmp = tempfile.mkdtemp(prefix="pipeline-test-")
        try:
            print(f"-- {t.__name__}")
            t(tmp)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
    print("all pipeline overlap tests passed")
