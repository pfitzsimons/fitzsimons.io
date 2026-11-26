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

`scripts/run_pipeline.sh` is the whole pipeline. Each run works out what's due (UTC throughout):

- **Scrape**: every run 07:00–20:59 UTC. `scrape_races.py` merges into `horses/races.json`, and the day's first run freezes `horses/history/races_<date>.json`
- **Results & accuracy**: once a day from 09:00 UTC. `fetch_results.py` grades yesterday. It counts as done once yesterday's `results_full_<date>.json` is on `main`, and retries on the next tick if it fails

It runs on **GitHub Actions, started from AWS**. GitHub's own cron queue drops and delays runs (community discussion 156282), but a manual `workflow_dispatch` starts within seconds, so the schedule lives in AWS:

```
EventBridge Scheduler ──► Lambda ──► GitHub workflow_dispatch ──► Actions runs run_pipeline.sh ──► push to main
(every 20 min, 07:00–20:59 UTC)  fitzsimons-pipeline-dispatch   .github/workflows/scrape-races.yml ("Horses Pipeline")
```

- **Workflow**: `.github/workflows/scrape-races.yml` ("Horses Pipeline"). It has no `schedule:`. With no inputs it runs whatever is due; run it by hand from the Actions tab with `job` = `scrape` or `results` and an optional date to force one. One concurrency group, so runs never overlap.
- **AWS**: `infra/pipeline-scheduler.yaml` (plain CloudFormation, stack `fitzsimons-pipeline-scheduler`, eu-west-2). It contains the schedule `fitzsimons-pipeline-every-20-min`, the Lambda `fitzsimons-pipeline-dispatch`, roles `fitzsimons-pipeline-lambda` and `fitzsimons-pipeline-scheduler`, and an alarm on Lambda errors. If GitHub rejects a dispatch, the Lambda logs the status and body and fails.
- **Token**: a fine-grained GitHub token (this repo only, Actions: read and write) stored as a SecureString at `/fitzsimons/pipeline/github-token`. The Lambda reads it on every run, so rotating it is just overwriting the parameter:

  ```bash
  aws ssm put-parameter --profile fitzsimons --region eu-west-2 --name /fitzsimons/pipeline/github-token --type SecureString --overwrite --value "$(pbpaste)"
  ```

- **Deploy or update** the stack:

  ```bash
  aws cloudformation deploy --profile fitzsimons --region eu-west-2 --stack-name fitzsimons-pipeline-scheduler --template-file infra/pipeline-scheduler.yaml --capabilities CAPABILITY_NAMED_IAM
  ```

  Add `--parameter-overrides ScheduleState=DISABLED` to pause the schedule, or `AlertEmail=you@example.com` to get an email when a dispatch fails.
- **Trigger a run by hand**: `aws lambda invoke --profile fitzsimons --region eu-west-2 --function-name fitzsimons-pipeline-dispatch /dev/stdout`. Pass `--payload '{"ref":"my-branch"}' --cli-binary-format raw-in-base64-out` to run a branch's `run_pipeline.sh` against `main` before merging. It still pushes data to `main`.
- **Where to look**: dispatches are logged in CloudWatch at `/aws/lambda/fitzsimons-pipeline-dispatch`, and runs show in the Actions tab. A failed run emails the repo owner.

**Two hosts at once is safe.** The Mac launchd agent below runs the same script and can run alongside Actions. Every run syncs to `main` before it writes. If its push loses a race, it never text-merges JSON: it resets to `main` and redoes the job on top, so `races.json` keeps both scrapes and `accuracy.json` is regraded. The only local work it carries over is a `horses/history/` file `main` doesn't have yet, so a frozen start-of-day archive is never overwritten. `scripts/test_pipeline_overlap.py` tests this in CI.

### Mac fallback (launchd)

The original host. It runs the same script from a dedicated clone every 20 minutes:

```bash
scripts/launchd/install.sh             # install / reload
scripts/launchd/install.sh uninstall   # stop and remove
```

- Log: `~/.local/state/fitzsimons-pipeline/pipeline.log`. A failed job also raises a macOS notification, and runs `$PIPELINE_NOTIFY_CMD "<message>"` if that's set.
- Run a job by hand: `~/.local/share/fitzsimons-pipeline/fitzsimons.io/scripts/run_pipeline.sh scrape|results [YYYY-MM-DD]`
- It only runs while the Mac is awake. The script is plain bash, so any cron or systemd host can run it the same way.

## Contributing

Changes to the scoring model should be validated against `horses/history/` with the relevant backtest script (`backtest_distance.py`, `backtest_experience.py`, `backtest_freshness.py`, `backtest_top_pick.py`, `backtest_value.py`, `backtest_win_threshold.py`) and checked with `drift.py` to confirm the change doesn't regress recent performance before merging. `calibrate.py --rescore` re-scores the whole archive walk-forward with the current model instead of trusting each archived day's (possibly stale) stored score/recommendation — use it to reconcile calibration numbers exactly with `backtest_value.py`.
