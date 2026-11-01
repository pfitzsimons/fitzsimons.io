#!/bin/bash
# Install (or refresh) the launchd agent that runs the /horses pipeline on this
# Mac. Idempotent. Creates a dedicated clone the pipeline owns — never point it
# at a checkout you work in — and retires the old workflow_dispatch trigger
# agent, so GitHub Actions and this Mac don't both scrape.
#
#   scripts/launchd/install.sh            # install / reload
#   scripts/launchd/install.sh uninstall  # stop and remove the agent
set -eu

LABEL=io.fitzsimons.pipeline
OLD_LABEL=io.fitzsimons.scrape-trigger
CLONE="${PIPELINE_CLONE:-$HOME/.local/share/fitzsimons-pipeline/fitzsimons.io}"
STATE="$HOME/.local/state/fitzsimons-pipeline"
AGENTS="$HOME/Library/LaunchAgents"
PLIST="$AGENTS/$LABEL.plist"
DOMAIN="gui/$(id -u)"
HERE="$(cd "$(dirname "$0")" && pwd)"

if [ "${1:-}" = uninstall ]; then
  launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null || true
  rm -f "$PLIST"
  echo "Removed $LABEL (clone left at $CLONE)"
  exit 0
fi

mkdir -p "$STATE" "$AGENTS" "$(dirname "$CLONE")"
if [ ! -d "$CLONE/.git" ]; then
  git clone https://github.com/pfitzsimons/fitzsimons.io.git "$CLONE"
fi
git -C "$CLONE" checkout -q main
git -C "$CLONE" pull -q --rebase origin main
[ -x "$CLONE/scripts/run_pipeline.sh" ] || {
  echo "ERROR: $CLONE has no scripts/run_pipeline.sh — merge the pipeline change to main first" >&2
  exit 1
}

# Retire the old trigger (it only fired workflow_dispatch on GitHub Actions).
if [ -f "$AGENTS/$OLD_LABEL.plist" ]; then
  launchctl bootout "$DOMAIN/$OLD_LABEL" 2>/dev/null || true
  mv "$AGENTS/$OLD_LABEL.plist" "$AGENTS/$OLD_LABEL.plist.disabled"
  echo "Retired $OLD_LABEL (plist kept as $OLD_LABEL.plist.disabled)"
fi

sed -e "s#__HOME__#$HOME#g" -e "s#__CLONE__#$CLONE#g" "$HERE/$LABEL.plist" > "$PLIST"
launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null || true
launchctl bootstrap "$DOMAIN" "$PLIST"
echo "Installed $LABEL → $CLONE/scripts/run_pipeline.sh (log: $STATE/pipeline.log)"
echo "Tip: wake the Mac before the first scrape so the start-of-day archive is"
echo "frozen early:  sudo pmset repeat wakeorpoweron MTWRFSU 06:55:00"
