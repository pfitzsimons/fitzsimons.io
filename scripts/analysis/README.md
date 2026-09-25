# Accuracy review analysis (2026-09-25)

Offline scripts behind the September 2026 accuracy review. They are not part of
the pipeline. Run them from anywhere. `build_rows.py`, `head_to_head.py` and
`prices.py` are stdlib-only; the others need `pip install numpy scipy`.

| Script | Question |
|---|---|
| `build_rows.py` | Rebuilds the walk-forward runner dataset (`rows.json`, gitignored). **Run this first.** |
| `head_to_head.py` | Does the model's top pick beat backing the favourite? |
| `clogit.py` | Does the model add information on top of the market price? |
| `prices.py` | Is `current_odds` a bettable price? (forecast vs best bookmaker vs SP) |
| `robust.py` | Rolling monthly value-bet test, settled at forecast / SP / bookmaker prices |
| `book_value.py` | The open lead: value betting against the best bookmaker price |

## Findings

Walk-forward rescore of the current model, burn-in 15 days: 129 days, 4,985
races, 45,740 runners.

1. **The accuracy panel only shows a short window.** `fetch_results.py` keeps
   the last 30 days in `accuracy.json`, and the panel hides days with no pick.
   Most of `horses/history/` never appears.

2. **The model doesn't beat backing the favourite.**

   | | win % | ROI @forecast | ROI @SP |
   |---|---|---|---|
   | Model top pick | 27.1 | −13.3% | −15.6% |
   | Favourite | 30.3 | −12.6% | −11.2% |
   | Strong Win Bet (n=57) | 40.4 | −11.8% | −18.2% |

   When the two disagree (35% of races), the favourite wins 27.0% and the
   model's pick 17.9%. Log-loss: market 1.864, best model softmax 1.992,
   uniform 2.134.

3. **The overall score adds nothing once the price is known**
   (`clogit.py`, split 2026-07-22). jockey, trainer and consistency add a
   small, significant amount: log-loss gain 0.0076, 95% day-bootstrap CI
   [0.0055, 0.0095]. distance, freshness, class and timeform add nothing.

4. **`odds_dec` (Sporting Life `betting.current_odds`) is the overnight
   forecast, not a bettable price.** It never moves intraday and equals SP
   only 8.9% of the time. For winners it's 25% above the morning best bookmaker
   price and 29% above SP; for losers only 3–4%. Anything settled at it (the
   live accuracy panel, `calibrate.py`, `backtest_*.py`) looks better than it
   really is. For example, the market+components value strategy shows +11% to
   +31% at the forecast price, and that disappears at SP (`robust.py`).

5. **Lead, not a finding** (`book_value.py`, data since 2026-07-17, races
   where every runner has `best_odds_dec`). A logit on the log best-book
   implied probability (coefficient ≈1.12) that backs EV > 5% at odds ≤ 10,
   settled at the best-book price:
   forward split +30.1% on 305 bets, CI [+13, +46]; reverse split +9.8% on
   188 bets, CI [−12, +33]. Caveats: about 5 weeks of data; effectively 2
   bookmakers (Betfair Sportsbook and Paddy Power are the same company, and
   Sky Bet rarely has the best price); the best-book overround (1.175) is no
   tighter than SP's, so some quotes may be stale; the reverse split isn't
   walk-forward.

## Possible next steps

1. Settle accuracy/ROI at `best_odds_dec` (or SP) instead of the forecast, and
   relabel that price on the site.
2. Show the full history in the accuracy panel, or label it "last 30 days",
   and add a "vs backing the favourite" baseline.
3. Paper-trade a market-anchored model (log market probability + jockey /
   trainer / consistency) forward for 4–6 weeks before it drives any
   recommendation.
