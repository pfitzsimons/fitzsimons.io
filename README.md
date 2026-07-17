# fitzsimons.io

Patrick Fitzsimons' personal site, deployed at [www.fitzsimons.io](https://www.fitzsimons.io). Also home to a self-updating horse racing prediction system for UK & Ireland racing.

## What's here

- `index.html` — personal homepage (Engineering Manager, Belfast)
- `horses/` — Race Day: UK & Ireland horse racing predictions, a static page rendering `horses/races.json`
- `scripts/` — Python pipeline that scrapes races, scores runners, fetches results, and backtests/calibrates the scoring model
- `.github/workflows/` — scheduled GitHub Actions that keep the pipeline running
- `css/` — shared styles for the homepage

## How the horse racing pipeline works

1. **`scrape_races.py`** runs every 20 minutes during UK/IRE racing hours (07:00–20:40 UTC). It scrapes Sporting Life racecards and scores each runner on a dozen factors — recent form, consistency, odds value, weight-for-age, going suitability, jockey quality, recency/DNF penalties, distance suitability, freshness, class, and experience shrinkage — into a 0–100 score with a label, confidence, and Win/Skip recommendation. It writes `horses/races.json`, and the first run of the day freezes a leak-free start-of-day archive in `horses/history/`.
2. **`fetch_results.py`** runs once daily at 09:00 UTC, pulls the previous day's results, and updates `horses/accuracy.json`.
3. **`calibrate.py`**, **`drift.py`**, and the **`backtest_*.py`** scripts are offline tools for checking whether the score is honest (calibration curve), whether it's profitable (flat-stake ROI), and whether a signal is decaying over time.

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

# Serve the site locally
python3 -m http.server
# then open index.html, horses/index.html
```

## Configuration

- `CNAME` sets the custom domain (`www.fitzsimons.io`).
- Scoring weights (`DIST_WEIGHT`, `FRESH_WEIGHT`, `OR_WEIGHT`, `TF_WEIGHT`, `EXPERIENCE_K`, etc.) live at the top of `scrape_races.py`. Several factors ship off by default — tune with the matching `backtest_*.py` script before enabling.

## Automation

Two scheduled workflows keep the data live and commit straight back to `main` (`[skip ci]`):

- **Scrape Races** — every 20 minutes, 07:00–20:40 UTC
- **Fetch Results & Accuracy** — daily at 09:00 UTC

Both can also be triggered manually via `workflow_dispatch` with a date override.

## Contributing

Changes to the scoring model should be validated against `horses/history/` with the relevant backtest script (`backtest_distance.py`, `backtest_experience.py`, `backtest_freshness.py`, `backtest_value.py`, `backtest_win_threshold.py`) and checked with `drift.py` to confirm the change doesn't regress recent performance before merging. `calibrate.py --rescore` re-scores the whole archive walk-forward with the current model instead of trusting each archived day's (possibly stale) stored score/recommendation — use it to reconcile calibration numbers exactly with `backtest_value.py`.
