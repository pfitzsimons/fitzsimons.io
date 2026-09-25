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
| `book_value.py` | The "lead": value betting against the best bookmaker price (**refuted**, see 5) |

> **Caveat on the whole dataset.** `build_rows.py` keeps only horses that
> actually ran, so each race's field silently drops late non-runners — known
> only after the fact. Anything that normalises prices across the field
> (`clogit.py`, `robust.py`, `book_value.py`) uses that hindsight. It's what
> produced the false lead in finding 5. Rebuild the field from the
> start-of-day archive (void bets on non-runners) before trusting a new result.

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

5. **Refuted: the best-bookmaker value lead was hindsight.** `book_value.py`
   (a logit on the log best-book implied probability, coefficient ≈1.12,
   backing EV > 5% at odds ≤ 10) showed +30.1% on 305 bets forward and +9.8%
   on 188 reversed. But those bets came from races where a horse (often the
   morning favourite) was a **late non-runner**: dropping it after the fact
   pushes the rest of the book below 100% (192 races summed < 1.0), so
   nearly every remaining runner looks like value, and Rule 4 deductions
   would take that back in real betting. Using the morning field, which is
   what was actually knowable, the same rule fires **twice since 2026-07-17**.
   With a ~17.5% overround across effectively two bookmakers, sharpening the
   market price can't find positive EV. Nothing to paper-trade.

## Possible next steps

1. Settle accuracy/ROI at `best_odds_dec` (or SP) instead of the forecast, and
   relabel that price on the site.
2. Show the full history in the accuracy panel, or label it "last 30 days",
   and add a "vs backing the favourite" baseline.
3. ~~Paper-trade the market-anchored model~~ — dropped, see finding 5.

Steps 1 and 2 shipped: `fetch_results.py`, `calibrate.py` and every
`backtest_*.py` now settle at the best bookmaker price (SP before
2026-07-17); the accuracy log keeps the full history and a favourite
baseline. Walk-forward Strong Win Bet moves from −11.8% to −17.9% (n=57). Regrading the full archive (`--rebuild`): 3,356 bets
over 99 graded days, ROI −12.6%, against −10.0% for backing the morning
favourite in the same races (−10.5% at the old forecast price).
