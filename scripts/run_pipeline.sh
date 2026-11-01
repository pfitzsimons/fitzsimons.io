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
#              yesterday. A per-day marker is written only on success, so a
#              failed run retries on the next tick.
#
# Force a job (manual fallback, ignores the schedule):
#   run_pipeline.sh scrape [YYYY-MM-DD]
#   run_pipeline.sh results [YYYY-MM-DD]
#
# Overlap: one lock (a mkdir lock dir, holder's pid inside) covers BOTH jobs,
# so two runs can never interleave writes or pushes. A run that can't get the
# lock within LOCK_WAIT seconds exits and the next tick picks the work up. A
# manual workflow_dispatch on Actions pushing at the same time is handled by
# rebasing on origin before every push.
#
# Failures: everything goes to $STATE_DIR/pipeline.log; a failed job also
# raises a macOS notification (when osascript exists) and runs
# $PIPELINE_NOTIFY_CMD with the message as $1, if set.
#
# The whole body is inside main() and the last line exits: `git pull` can
# rewrite this file mid-run, and bash must have parsed all of it beforehand.

main() {
  set -u
  export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

  REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
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
  exec >>"$LOG" 2>&1

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

  local rc=0
  if [ "$do_scrape" -eq 1 ]; then
    run_scrape "$force_date" || rc=1
  fi
  if [ "$do_results" -eq 1 ]; then
    if run_results "$force_date"; then
      [ -z "$force" ] && touch "$STATE_DIR/results-$today.done"
    else
      rc=1
    fi
  fi
  return "$rc"
}

log()  { echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') $*"; }

fail() {
  log "FAILED: $*"
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
# regenerated); committed-but-unpushed work from a failed push is kept and
# rebased, so a start-of-day archive that missed its push still lands.
sync_repo() {
  if [ -n "$(git status --porcelain)" ]; then
    log "discarding uncommitted leftovers: $(git status --porcelain | tr '\n' ' ')"
    git reset --hard -q HEAD && git clean -fdq horses/
  fi
  git fetch -q origin "$BRANCH" || { fail "git fetch failed"; return 1; }
  if ! git rebase -q "origin/$BRANCH"; then
    git rebase --abort 2>/dev/null
    fail "could not rebase local commits onto origin/$BRANCH — fix the clone by hand ($REPO_DIR)"
    return 1
  fi
}

# Commit the given paths and push, rebasing on anything pushed meanwhile.
commit_and_push() {
  local msg="$1"; shift
  git add "$@" 2>/dev/null || true
  if git diff --staged --quiet; then
    log "no content changes — nothing to commit"
    return 0
  fi
  git commit -q -m "$msg" || { fail "git commit failed"; return 1; }
  local attempt
  for attempt in 1 2 3; do
    if git push -q origin "HEAD:$BRANCH"; then
      log "pushed: $msg"
      return 0
    fi
    log "push attempt $attempt rejected — rebasing on origin/$BRANCH"
    git fetch -q origin "$BRANCH" || true
    if ! git rebase -q "origin/$BRANCH"; then
      git rebase --abort 2>/dev/null
      break
    fi
  done
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
