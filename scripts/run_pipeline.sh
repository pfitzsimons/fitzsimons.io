#!/bin/bash
# run_pipeline.sh — scheduled /horses data pipeline, run off GitHub Actions.
#
# GitHub's cron queue drops and delays runs (community discussion 156282), so
# the schedule lives on a host we control (a macOS launchd agent today — see
# scripts/launchd/ and the README). Any cron/systemd host works the same way.
#
# Run it from a DEDICATED clone, never a working checkout: each run pulls,
# writes horses/, commits and pushes there. It takes no arguments in normal
# use; call it every ~20 minutes and it decides what is due (UTC throughout,
# so BST/GMT never matters):
#
#   scrape   — every run 07:00–20:59 UTC: scripts/scrape_races.py merges into
#              the live races.json; the day's first run freezes the start-of-day
#              archive (enforced in persist_scrape, tested by
#              test_intraday_merge.py).
#   results  — once a day at/after 09:00 UTC: scripts/fetch_results.py grades
#              yesterday. "Done" is read from the repo itself — yesterday's
#              horses/history/results_full_<date>.json is on origin, and it is
#              only ever pushed by a successful run — so it works from a fresh
#              clone (Actions) and across hosts. The Mac also keeps a per-day
#              marker in $STATE_DIR as a cheap early exit. A failed run retries
#              on the next tick.
#
# Force a job (manual fallback, ignores the schedule):
#   run_pipeline.sh scrape [YYYY-MM-DD]
#   run_pipeline.sh results [YYYY-MM-DD]
#
# Overlap: on one host, a lock (a mkdir lock dir, holder's pid inside) covers
# BOTH jobs, so two runs never interleave; a run that can't get it within
# LOCK_WAIT seconds exits and the next tick picks the work up. On Actions the
# workflow's concurrency group does the same. ACROSS hosts, origin always
# wins: if a push is rejected because the other host pushed first, this run
# never text-merges JSON (a git rebase could silently splice two races.json
# versions together). It resets to origin and redoes the job on top, so the
# scrape merges into the newer races.json and accuracy.json is regraded. The
# only local work carried over is a horses/history/ file origin doesn't have
# yet (create-if-absent, like the start-of-day archive itself), so a frozen
# archive origin already has is never overwritten.
#
# Failures: the job exits non-zero. On Actions the output goes to the job log
# (and a failed run emails the repo owner). Elsewhere everything goes to
# $STATE_DIR/pipeline.log, a failed job raises a macOS notification (when
# osascript exists) and runs $PIPELINE_NOTIFY_CMD with the message as $1.
#
# Environment overrides: PIPELINE_REPO_DIR (the clone to work in; default:
# the clone this script lives in), PIPELINE_STATE_DIR, PIPELINE_PYTHON,
# PIPELINE_BRANCH, PIPELINE_LOCK_WAIT.
#
# The whole body is inside main() and the last line exits: `git pull` can
# rewrite this file mid-run, and bash must have parsed all of it beforehand.

main() {
  set -u
  # launchd starts with a bare PATH; keep whatever the caller had after it
  # (Actions puts setup-python's interpreter there).
  export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin${PATH:+:$PATH}"

  ON_ACTIONS=0
  [ "${GITHUB_ACTIONS:-}" = true ] && ON_ACTIONS=1
  REPO_DIR="${PIPELINE_REPO_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
  STATE_DIR="${PIPELINE_STATE_DIR:-$HOME/.local/state/fitzsimons-pipeline}"
  PYTHON="${PIPELINE_PYTHON:-python3}"
  BRANCH="${PIPELINE_BRANCH:-main}"
  LOCK_WAIT="${PIPELINE_LOCK_WAIT:-600}"
  SCRAPE_START=7      # UTC hour, inclusive
  SCRAPE_END=20       # UTC hour, inclusive
  RESULTS_AFTER=9     # UTC hour

  mkdir -p "$STATE_DIR"
  LOG="$STATE_DIR/pipeline.log"
  LOCK="$STATE_DIR/pipeline.lock"
  [ "$ON_ACTIONS" -eq 1 ] || exec >>"$LOG" 2>&1

  local force="${1:-}" force_date="${2:-}"
  local hour today
  hour=$((10#$(date -u +%H)))   # 10# so "08"/"09" aren't read as octal
  today=$(date -u +%Y-%m-%d)

  local do_scrape=0 do_results=0
  case "$force" in
    scrape)  do_scrape=1 ;;
    results) do_results=1 ;;
    "")
      [ "$hour" -ge "$SCRAPE_START" ] && [ "$hour" -le "$SCRAPE_END" ] && do_scrape=1
      [ "$hour" -ge "$RESULTS_AFTER" ] && [ ! -f "$STATE_DIR/results-$today.done" ] && do_results=1
      ;;
    *) log "unknown job '$force' (expected scrape|results)"; return 2 ;;
  esac
  [ "$do_scrape" -eq 0 ] && [ "$do_results" -eq 0 ] && return 0

  acquire_lock || return 0
  trap 'rm -rf "$LOCK"' EXIT

  cd "$REPO_DIR" || { fail "cannot cd to $REPO_DIR"; return 1; }
  sync_repo || return 1

  # Results already graded today (by this host or the other one)? Decided
  # from origin's content, after the sync, so a fresh clone gets it right.
  if [ "$do_results" -eq 1 ] && [ -z "$force" ]; then
    local yday
    yday=$("$PYTHON" -c 'import datetime; print(datetime.date.today() - datetime.timedelta(days=1))') \
      || { fail "could not work out yesterday's date"; return 1; }
    if [ -f "horses/history/results_full_$yday.json" ]; then
      log "results: $yday already graded on origin — skipping"
      do_results=0
      touch "$STATE_DIR/results-$today.done"
    fi
  fi

  local rc=0
  if [ "$do_scrape" -eq 1 ]; then
    with_redo run_scrape "$force_date" || rc=1
  fi
  if [ "$do_results" -eq 1 ]; then
    if with_redo run_results "$force_date"; then
      [ -z "$force" ] && touch "$STATE_DIR/results-$today.done"
    else
      rc=1
    fi
  fi
  return "$rc"
}

# Exit status a job returns when origin moved under it and it must be redone.
REDO=75

# Run a job; if its push lost a race with the other host, re-sync onto origin
# and run it again on top (up to 3 attempts).
with_redo() {
  local attempt rc
  for attempt in 1 2 3; do
    "$@"
    rc=$?
    [ "$rc" -ne "$REDO" ] && return "$rc"
    log "origin moved during $1 (attempt $attempt) — redoing on top of it"
    sync_repo || return 1
  done
  fail "$1: origin kept moving — gave up after 3 attempts"
  return 1
}

log()  { echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') $*"; }

fail() {
  log "FAILED: $*"
  if [ "${ON_ACTIONS:-0}" -eq 1 ]; then
    echo "::error title=Horses pipeline failed::$*"
    return 0
  fi
  local msg="fitzsimons.io pipeline: $*"
  if command -v osascript >/dev/null 2>&1; then
    osascript -e "display notification \"${msg//\"/}\" with title \"Horses pipeline failed\" sound name \"Basso\"" || true
  fi
  if [ -n "${PIPELINE_NOTIFY_CMD:-}" ]; then
    $PIPELINE_NOTIFY_CMD "$msg" || true
  fi
}

acquire_lock() {
  local waited=0
  until mkdir "$LOCK" 2>/dev/null; do
    local holder
    holder=$(cat "$LOCK/pid" 2>/dev/null || true)
    if [ -n "$holder" ] && ! kill -0 "$holder" 2>/dev/null; then
      log "removing stale lock from dead pid $holder"
      rm -rf "$LOCK"
      continue
    fi
    if [ "$waited" -ge "$LOCK_WAIT" ]; then
      log "lock held by pid ${holder:-?} for ${LOCK_WAIT}s — skipping this tick"
      return 1
    fi
    sleep 10
    waited=$((waited + 10))
  done
  echo $$ > "$LOCK/pid"
}

# Bring the clone up to date before writing anything: the scraper merges into
# the CURRENT races.json, and "first run of the day" means "no archive on
# origin yet". Uncommitted leftovers from a crashed run are discarded (they get
# regenerated). Local commits a failed push left behind are kept if origin
# hasn't moved (the next push sends them). If origin HAS moved, origin wins:
# reset to it, carrying over only horses/history/ files origin lacks (e.g. a
# start-of-day archive whose push failed). Never a text merge of JSON.
sync_repo() {
  if [ -n "$(git status --porcelain)" ]; then
    log "discarding uncommitted leftovers: $(git status --porcelain | tr '\n' ' ')"
    git reset --hard -q HEAD && git clean -fdq horses/
  fi
  git fetch -q origin "$BRANCH" || { fail "git fetch failed"; return 1; }
  local remote="origin/$BRANCH"
  if git merge-base --is-ancestor "$remote" HEAD; then
    return 0                                  # up to date, or only ahead
  fi
  if git merge-base --is-ancestor HEAD "$remote"; then
    git reset --hard -q "$remote"             # plain fast-forward
    return 0
  fi

  local keep base f
  keep=$(mktemp -d)
  if base=$(git merge-base HEAD "$remote"); then
    for f in $(git diff --name-only --diff-filter=A "$base" HEAD -- horses/history/); do
      git cat-file -e "$remote:$f" 2>/dev/null && continue   # origin has its own
      mkdir -p "$keep/$(dirname "$f")" && cp "$f" "$keep/$f"
    done
  fi
  log "origin moved; dropping local commits $(git rev-list --count "$remote..HEAD") and resetting to $remote"
  git reset --hard -q "$remote"
  if [ -d "$keep/horses" ]; then
    log "carrying over files origin lacks: $(cd "$keep" && find horses -type f | tr '\n' ' ')"
    cp -R "$keep/horses" .
  fi
  rm -rf "$keep"
}

# Commit the given paths and push. Returns $REDO if the other host pushed
# first, so with_redo re-syncs and runs the job again on the newer origin.
commit_and_push() {
  local msg="$1"; shift
  git add "$@" 2>/dev/null || true
  if git diff --staged --quiet; then
    log "no content changes — nothing to commit"
    return 0
  fi
  git commit -q -m "$msg" || { fail "git commit failed"; return 1; }
  if git push -q origin "HEAD:$BRANCH"; then
    log "pushed: $msg"
    return 0
  fi
  if git fetch -q origin "$BRANCH" && ! git merge-base --is-ancestor "origin/$BRANCH" HEAD; then
    log "push rejected — origin moved: $msg"
    return "$REDO"
  fi
  fail "push failed: $msg (commit kept locally; next run retries)"
  return 1
}

run_scrape() {
  local date_arg=()
  [ -n "${1:-}" ] && date_arg=(--date "$1")
  log "scrape: start ${1:-}"
  if ! "$PYTHON" scripts/scrape_races.py ${date_arg[@]+"${date_arg[@]}"} --out "$REPO_DIR/horses"; then
    fail "scrape_races.py exited non-zero"
    return 1
  fi
  local count
  count=$("$PYTHON" -c "import json; print(len(json.load(open('horses/races.json')).get('races', [])))") \
    || { fail "horses/races.json missing or unreadable after scrape"; return 1; }
  log "scrape: $count races live"
  # Live file plus the start-of-day archive the first run writes.
  commit_and_push "🏇 Races $(date -u +%Y-%m-%dT%H:%M)Z [skip ci]" horses/races.json horses/history/
}

run_results() {
  log "results: start ${1:-}"
  # Safety net only: if the scraper never froze today's start-of-day archive
  # (e.g. every morning run failed), freeze the current races.json. It must
  # NEVER overwrite an existing archive, or intraday data would leak into the
  # graded record.
  "$PYTHON" - <<'PY' || { fail "fallback archive check failed"; return 1; }
import json, os
if os.path.exists("horses/races.json"):
    d = json.load(open("horses/races.json"))
    date, races = d.get("date", ""), d.get("races", [])
    archive = f"horses/history/races_{date}.json"
    if not races:
        print(f"Skipping archive for {date} — races.json has 0 races")
    elif not os.path.exists(archive):
        os.makedirs("horses/history", exist_ok=True)
        with open("horses/races.json") as src, open(archive, "w") as dst:
            dst.write(src.read())
        print(f"Fallback-archived {len(races)} predictions for {date}")
    else:
        print(f"Start-of-day archive for {date} already frozen — leaving untouched")
PY
  local date_arg=()
  [ -n "${1:-}" ] && date_arg=(--results-date "$1")
  if ! "$PYTHON" scripts/fetch_results.py ${date_arg[@]+"${date_arg[@]}"} --out "$REPO_DIR/horses"; then
    fail "fetch_results.py exited non-zero"
    return 1
  fi
  commit_and_push "📊 Accuracy update $(date -u +%Y-%m-%d) [skip ci]" horses/accuracy.json horses/history/
}

main "$@"
exit $?
