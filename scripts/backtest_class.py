#!/usr/bin/env python3
"""
Walk-forward A/B for the official-rating and Timeform-star factors
(scrape_races.OR_WEIGHT / TF_WEIGHT).

Both were added 2026-07-04 alongside distance/freshness but, per the
freshness-and-class-factors memory note, were shipped OFF at weight 0 and
marked "CANNOT be backtested now" — unlike distance/freshness they aren't
reconstructible from results, and the archive didn't store them before that
date. That blocker is gone: official_rating and timeform_stars are stored
directly per-runner in the archive (components.official_rating /
components.timeform_stars) from the day they were added, and ~6 weeks of real
capture (2026-07-05 onward) has now accumulated — this is the first time
either factor can be tested. No reconstruction needed and no conservative
caveat: this uses the exact same raw values production captures.

Two arms per weight, same shipped model otherwise (strike-rates, experience
shrink, class/weight normalisation — the full pipeline, matching the
experience_shrink fix in backtest_value.py from the 2026-08-17 review):

    off : OR_WEIGHT/TF_WEIGHT = 0   (current production model)
    on  : OR_WEIGHT/TF_WEIGHT = w   (form weight reduced by the same w)

    python3 scripts/backtest_class.py                  # sweep both factors
    python3 scripts/backtest_class.py --bootstrap       # + significance
    python3 scripts/backtest_class.py --or-weight 0.05  # single OR weight
    python3 scripts/backtest_class.py --tf-weight 0.05  # single TF weight
"""

import argparse
import copy
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scrape_races as s
import strike_rates as sr
import fetch_results as fr


# ─────────────────────────────────────────────────────────────
# Scoring one race through the real production code path
# ─────────────────────────────────────────────────────────────

def score_and_recommend(prace, table, or_weight, tf_weight):
    """Re-score a race with the shipped strike-rate model plus OR_WEIGHT /
    TF_WEIGHT, reading each runner's archived official_rating/timeform_stars
    directly (no reconstruction — these are stored raw, unlike distance/
    freshness). Returns runners (deep-copied), sorted best-first."""
    runners = [copy.deepcopy(r) for r in prace["runners"]]
    n = len(runners)

    s.STRIKE_TABLE = table
    sr.JOCKEY_WEIGHT, sr.TRAINER_WEIGHT = 0.075, 0.075
    s.OR_WEIGHT = or_weight
    s.TF_WEIGHT = tf_weight

    for run in runners:
        comp = run.get("components") or {}
        run["_official_rating"] = comp.get("official_rating")
        run["_timeform_stars"]  = comp.get("timeform_stars")
        res = s.score_runner(run, n, prace["going"], prace["distance"], prace["title"])
        run["_score"] = res["_score"]
        run["_components"] = res["_components"]
        run["_form_analysis"] = res["_form_analysis"]
    s.normalise_weight_scores(runners)
    s.normalise_class_scores(runners)

    cf = s.COURSE_COEFFICIENTS.get(prace["course"], 1.0)
    if cf != 1.0:
        for run in runners:
            run["_score"] = max(0.0, min(100.0, run["_score"] * cf))

    for run in runners:
        num_runs = (run.get("_form_analysis") or {}).get("num_runs", 0)
        run["_score"] = s.experience_shrink(run["_score"], num_runs)

    runners.sort(key=lambda r: r["_score"], reverse=True)
    for run in runners:
        form = s.parse_form(run.get("form", ""))
        run["recommendation"] = s.make_recommendation(run["_score"], run.get("odds_dec"), n, form)
    s._post_process_win_bets(runners, n)
    return runners


def primary_win(runners):
    for r in runners:
        if r["recommendation"]["type"] == "Win":
            return r
    return None


def pnl_for(run, strong_only=False):
    oc = run.get("_oc")
    if not oc or oc["status"] == "non_runner":
        return None
    od = run.get("odds_dec")
    if not od or od <= 1:
        return None
    if strong_only and run["recommendation"].get("label") != "Strong Win Bet":
        return None
    res = "correct" if (oc["status"] == "finished" and oc["pos"] == 1) else "incorrect"
    return fr.bet_pnl("Win", res, od)


# ─────────────────────────────────────────────────────────────
# Walk-forward driver
# ─────────────────────────────────────────────────────────────

def walk_forward(days, burn_in, or_weights, tf_weights):
    """Return per_day[arm][tier] -> [(date, [(stake,ret),...]), ...].
    arm is "off", "or:<w>" or "tf:<w>" — OR and TF are swept independently,
    each held at 0 while the other varies."""
    arms = {"off": (0.0, 0.0)}
    arms.update({f"or:{w:.3f}": (w, 0.0) for w in or_weights})
    arms.update({f"tf:{w:.3f}": (0.0, w) for w in tf_weights})
    table = sr.StrikeTable()
    per_day = {a: {t: [] for t in ("win", "swin")} for a in arms}

    for i, (date_str, praces) in enumerate(days):
        if i >= burn_in:
            day = {a: {t: [] for t in ("win", "swin")} for a in arms}
            for prace in praces:
                for arm, (orw, tfw) in arms.items():
                    runners = score_and_recommend(prace, table, orw, tfw)
                    run = primary_win(runners)
                    if not run:
                        continue
                    p = pnl_for(run)
                    if p:
                        day[arm]["win"].append(p)
                    ps = pnl_for(run, strong_only=True)
                    if ps:
                        day[arm]["swin"].append(ps)
            for a in arms:
                for t in ("win", "swin"):
                    per_day[a][t].append((date_str, day[a][t]))
        for prace in praces:
            table.add_race(prace)
    return per_day, arms


# ─────────────────────────────────────────────────────────────
# Reporting
# ─────────────────────────────────────────────────────────────

def roi(pairs):
    st = sum(p[0] for p in pairs)
    rt = sum(p[1] for p in pairs)
    return ((rt - st) / st * 100 if st else 0.0), st


def flat(day_pairs):
    return [p for _, ps in day_pairs for p in ps]


def bootstrap(per_day, arm, tier, nboot=2000):
    paired = list(zip(per_day[arm][tier], per_day["off"][tier]))
    deltas = []
    for _ in range(nboot):
        samp = [random.choice(paired) for _ in paired]
        ab = [p for (_, a_), _ in samp for p in a_]
        ob = [p for _, (_, o_) in samp for p in o_]
        deltas.append(roi(ab)[0] - roi(ob)[0])
    deltas.sort()
    m = sum(deltas) / len(deltas)
    return m, deltas[int(0.025 * len(deltas))], deltas[int(0.975 * len(deltas))], \
        sum(1 for d in deltas if d > 0) / len(deltas)


def report(per_day, arms, do_bootstrap):
    print("\n============  OFFICIAL RATING / TIMEFORM WALK-FORWARD ROI (out-of-sample)  ============")
    print(f'{"arm":14} | {"Strong Win Bet":>20} | {"All Win":>20}')
    print("-" * 62)
    for arm in arms:
        s_roi, s_st = roi(flat(per_day[arm]["swin"]))
        w_roi, w_st = roi(flat(per_day[arm]["win"]))
        print(f"{arm:14} | {s_roi:+6.1f}%  n{int(s_st):<5}      "
              f"| {w_roi:+6.1f}%  n{int(w_st):<5}")

    if do_bootstrap:
        print("\n----  significance: arm − off ROI delta (bootstrap over days)  ----")
        for arm in arms:
            if arm == "off":
                continue
            for tier, name in (("swin", "Strong Win Bet"), ("win", "All Win")):
                m, lo, hi, p = bootstrap(per_day, arm, tier)
                verdict = "significant" if (lo > 0 or hi < 0) else "not significant"
                print(f"{arm} {name:16} | delta {m:+5.1f}pp  95% CI [{lo:+.1f}, {hi:+.1f}]"
                      f"  P(on>off)={p:.2f}  ({verdict})")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="horses", help="Directory holding history/")
    ap.add_argument("--burn-in", type=int, default=15,
                    help="Warm-up days excluded from ROI (default 15)")
    ap.add_argument("--or-weight", type=float, default=None,
                    help="Single OR_WEIGHT to test (default: sweep)")
    ap.add_argument("--tf-weight", type=float, default=None,
                    help="Single TF_WEIGHT to test (default: sweep)")
    ap.add_argument("--bootstrap", action="store_true", default=None)
    ap.add_argument("--seed", type=int, default=1)
    args = ap.parse_args()
    random.seed(args.seed)

    single = args.or_weight is not None or args.tf_weight is not None
    or_weights = [args.or_weight] if args.or_weight is not None else \
        ([] if args.tf_weight is not None else [0.03, 0.05, 0.08, 0.12])
    tf_weights = [args.tf_weight] if args.tf_weight is not None else \
        ([] if args.or_weight is not None else [0.03, 0.05, 0.08, 0.12])
    do_boot = args.bootstrap if args.bootstrap is not None else single

    hist = os.path.join(os.path.abspath(args.out), "history")
    days = sr.iter_history(hist)
    if not days:
        print("No joined history found (need races_*.json + results_full_*.json).",
              file=sys.stderr)
        return
    print(f"{len(days)} days ({days[0][0]}..{days[-1][0]}), "
          f"burn-in {args.burn_in} -> {len(days) - args.burn_in} out-of-sample days")

    per_day, arms = walk_forward(days, args.burn_in, or_weights, tf_weights)
    report(per_day, arms, do_boot)


if __name__ == "__main__":
    main()
