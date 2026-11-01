# fitzsimons.io

Patrick Fitzsimons' personal site, deployed at [www.fitzsimons.io](https://www.fitzsimons.io). Also home to a self-updating horse racing prediction system for UK & Ireland racing.

## What's here

- `index.html` — personal homepage (Engineering Manager, Belfast)
- `horses/` — Race Day: UK & Ireland horse racing predictions, a static page rendering `horses/races.json`
- `scripts/` — Python pipeline that scrapes races, scores runners, fetches results, and backtests/calibrates the scoring model
- `scripts/run_pipeline.sh` + `scripts/launchd/` — the scheduler that keeps the pipeline running (off GitHub Actions)
- `.github/workflows/` — tests on PRs, plus manual-fallback pipeline runs
- `css/` — shared styles for the homepage

## How the horse racing pipeline works

1. **`scrape_races.py`** runs every 20 minutes during UK/IRE racing hours (07:00–20:59 UTC, see Automation). It scrapes Sporting Life racecards and scores each runner on a dozen factors — recent form, consistency, odds value, weight-for-age, going suitability, jockey quality, recency/DNF penalties, distance suitability, freshness, class, and experience shrinkage — into a 0–100 score with a label, confidence, and Win/Skip recommendation. Separately, each runner gets a market-anchored win probability (`value.model_prob`: the morning best bookmaker price, sharpened; overnight forecast plus jockey/trainer/form when book prices are missing), which picks the site's Top Pick in each race. It writes `horses/races.json`, and the first run of the day freezes a leak-free start-of-day archive in `horses/history/`.
2. **`fetch_results.py`** runs once daily at 09:00 UTC, pulls the previous day's results, and updates `horses/accuracy.json`. Bets are settled at the best bookmaker price captured in the start-of-day scrape (SP where none was captured) — not at the displayed odds, which are Sporting Life's overnight forecast and run long on winners. Each day also records a favourite-to-win baseline in the same races. `accuracy.json` is fully derived: every run regrades the whole archive from `horses/history/` (full history; per-race detail only for the last 30 days), so it can't drift and a merge conflict on it fixes itself on the next run. `--rebuild` does the same offline.
3. **`calibrate.py`**, **`drift.py`**, and the **`backtest_*.py`** scripts are offline tools for checking whether the score is honest (calibration curve), whether it's profitable (flat-stake ROI, settled at the same bettable price as the site), and whether a signal is decaying over time.

## Quick start

Requires Python 3.11 (standard library only — no external dependencies).

```bash
# Scrape today's races and score runners
python3 scripts/scrape_races.py --out horses

# Fetch yesterday's results and update accuracy
python3 scripts/fetch_results.py --out horses

# Check calibration and ROI against the archive
python3 scripts/calibrate.py --out horses

# Check for decaying signals in a rolling window
python3 scripts/drift.py --out horses --window 10

# Run the offline tests (also run in CI on every PR touching scripts/)
python3 scripts/test_intraday_merge.py
python3 scripts/test_settlement.py

# Serve the site locally
python3 -m http.server
# then open index.html, horses/index.html
```

## Configuration

- `CNAME` sets the custom domain (`www.fitzsimons.io`).
- `ANCHOR_COEFS` / `ANCHOR_COMPONENTS` in `scrape_races.py` set the win-probability model behind the Top Pick. Refit with `python3 scripts/backtest_top_pick.py --fit`; run it without `--fit` for the walk-forward comparison.
- Scoring weights (`DIST_WEIGHT`, `FRESH_WEIGHT`, `OR_WEIGHT`, `TF_WEIGHT`, `EXPERIENCE_K`, etc.) live at the top of `scrape_races.py`. Several factors ship off by default — tune with the matching `backtest_*.py` script before enabling.

## Automation

The pipeline runs on a Mac launchd agent, not GitHub Actions: GitHub's cron queue dropped and delayed runs (community discussion 156282), and the start-of-day archive must be frozen before the first race. `scripts/run_pipeline.sh` runs from a dedicated clone every 20 minutes and works out what's due (UTC throughout):

- **Scrape** — every run 07:00–20:59 UTC: `scrape_races.py` merges into `horses/races.json`, and the day's first run freezes `horses/history/races_<date>.json`
- **Results & accuracy** — once a day from 09:00 UTC: `fetch_results.py` grades yesterday. It runs once per day and retries on the next tick if it fails

Every run pulls before it writes and rebases before it pushes, committing straight to `main` (`[skip ci]`). One lock covers both jobs, so runs can't overlap. If a push fails, the commit is kept and the next run pushes it.

Setup on the Mac (idempotent; clones to `~/.local/share/fitzsimons-pipeline/` and retires the old `workflow_dispatch` trigger agent):

```bash
scripts/launchd/install.sh
sudo pmset repeat wakeorpoweron MTWRFSU 06:55:00   # wake before the first scrape
```

- Log: `~/.local/state/fitzsimons-pipeline/pipeline.log`. A failed job also raises a macOS notification, and runs `$PIPELINE_NOTIFY_CMD "<message>"` if that's set (e.g. a `curl` to ntfy).
- Run a job by hand: `~/.local/share/fitzsimons-pipeline/fitzsimons.io/scripts/run_pipeline.sh scrape|results [YYYY-MM-DD]`
- The Mac must be awake. While it's off nothing runs, and the results job's fallback freezes the archive late. The script is plain bash, so any cron/systemd host can run it the same way.
- Manual fallback: both workflows (**Scrape Races**, **Fetch Results & Accuracy**) still accept `workflow_dispatch` with a date override, but they no longer have a schedule.

## Contributing

Changes to the scoring model should be validated against `horses/history/` with the relevant backtest script (`backtest_distance.py`, `backtest_experience.py`, `backtest_freshness.py`, `backtest_top_pick.py`, `backtest_value.py`, `backtest_win_threshold.py`) and checked with `drift.py` to confirm the change doesn't regress recent performance before merging. `calibrate.py --rescore` re-scores the whole archive walk-forward with the current model instead of trusting each archived day's (possibly stale) stored score/recommendation — use it to reconcile calibration numbers exactly with `backtest_value.py`.
