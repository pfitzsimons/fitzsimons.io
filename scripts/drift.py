#!/usr/bin/env python3
"""
Drift monitor — catch decaying signals with a rolling-window ROI/hit-rate view.

Jockey/trainer form, market efficiency and course conditions all change over
time, so a model that paid last month can quietly stop paying. This walks the
current model forward over the archive (same leak-free path backtest_value.py
uses) and reports ROI and hit-rate for the profitable Strong Win Bet tier and
overall in consecutive date windows, then flags the most recent window if it
has fallen well below the earlier average.

    python3 scripts/drift.py                 # 10-day windows
    python3 scripts/drift.py --window 7      # weekly

Run it periodically (or from the results workflow); a sustained drop in the
trailing window is the cue to re-backtest and retune.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import backtest_value as bv
import strike_rates as sr


def hit_rate(pairs):
    """Fraction of settled bets that returned a profit (won), and count."""
    n = len(pairs)
    w = sum(1 for st, ret in pairs if ret > st)
    return (w / n * 100 if n else 0.0), n


def windows(day_series, size):
    """Group a list of (date, [pairs]) into consecutive windows of `size` days."""
    out = []
    for i in range(0, len(day_series), size):
        chunk = day_series[i:i + size]
        lo, hi = chunk[0][0], chunk[-1][0]
        pairs = [p for _, ps in chunk for p in ps]
        out.append((lo, hi, pairs))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="horses", help="Directory holding history/")
    ap.add_argument("--window", type=int, default=10, help="Window size in days")
    ap.add_argument("--burn-in", type=int, default=15)
    args = ap.parse_args()

    hist = os.path.join(os.path.abspath(args.out), "history")
    days = sr.iter_history(hist)
    if not days:
        print("No joined history found.", file=sys.stderr)
        return

    per_day = bv.walk_forward(days, args.burn_in)
    swin = per_day["new"]["swin"]
    alld = [(d, ws + es) for (d, ws), (_, es)
            in zip(per_day["new"]["win"], per_day["new"]["ew"])]

    print(f"{len(days)} days, model rolled forward from day {args.burn_in}; "
          f"{args.window}-day windows\n")
    print(f'{"window":23} | {"StrongWin ROI":>14} {"hit%":>6} {"n":>4} '
          f'| {"overall ROI":>12} {"n":>4}')
    print("-" * 74)
    sw_rois = []
    for (lo, hi, sp), (_, _, ap_) in zip(windows(swin, args.window),
                                         windows(alld, args.window)):
        sr_roi, sr_st = bv.roi(sp)
        hr, _ = hit_rate(sp)
        a_roi, a_st = bv.roi(ap_)
        sw_rois.append((sr_roi, sr_st))
        print(f"{lo}..{hi} | {sr_roi:+7.1f}%  {hr:5.0f}% {int(sr_st):>4} "
              f"| {a_roi:+7.1f}% {int(a_st):>4}")

    # Drift flag: compare the trailing window to the mean of the earlier ones.
    if len(sw_rois) >= 3:
        trailing = sw_rois[-1][0]
        earlier = [r for r, _ in sw_rois[:-1]]
        base = sum(earlier) / len(earlier)
        print("\n" + "-" * 74)
        print(f"trailing Strong Win ROI {trailing:+.1f}% vs earlier avg {base:+.1f}%")
        if trailing < base - 15:
            print("  ⚠ DRIFT: trailing window is >15pp below the earlier average — "
                  "re-backtest and retune before trusting live picks.")
        else:
            print("  ✓ no material drift in the trailing window.")


if __name__ == "__main__":
    main()
