"""
00_full_rebuild.py
==================
Full sequential pipeline rebuild. Run this once to ingest all data
and rebuild features from scratch. Takes ~4-6 hours total.

Run: python scripts/00_full_rebuild.py
     python scripts/00_full_rebuild.py --from step3   # resume from a step
     python scripts/00_full_rebuild.py --list         # show all steps

Steps:
  1.  03_pull_stats_and_lines.py      — CBBD games + lines (all seasons)
  2.  03_pull_all_sources.py          — Torvik season/daily/preds, Haslametrics
  3.  03d_pull_kenpom_archive.py      — KenPom daily snapshots + fanmatch
  4.  03d_fetch_espn_ids.py           — ESPN game IDs (~37% coverage)
  5.  03e_pull_referees.py 2025       — Referee assignments
  6.  03e_pull_referees.py 2024       — Referee assignments
  7.  03e_pull_referees.py 2023       — Referee assignments
  8.  03e_pull_referees.py 2022       — Referee assignments
  9.  03f_compute_travel.py           — Travel distances + timezones
  10. 03g_recency_features.py         — Rolling efficiency features
  11. 03h_compute_trends.py           — Trend slopes
  12. 10_scrape_historical_lines.py   — Historical DK open/close lines
  13. 04_build_features.py            — Build game_features_v2
  14. 05_backtest_all_combos.py       — Spread model combo backtest
  15. 05b_backtest_totals_combos.py   — Totals model combo backtest
"""

import subprocess, sys, os, time, argparse
from datetime import datetime

ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS = os.path.join(ROOT, 'scripts')
PYTHON  = sys.executable

STEPS = [
    (0,  "Games table (CBBD)",            ["02_pull_games.py"]),
    (1,  "CBBD games + lines",            ["03_pull_stats_and_lines.py"]),
    (2,  "Torvik/Haslametrics",           ["03_pull_all_sources.py"]),
    (3,  "KenPom archive + fanmatch",     ["03d_pull_kenpom_archive.py"]),
    (4,  "ESPN game IDs",                 ["03d_fetch_espn_ids.py"]),
    (5,  "Refs 2025",                     ["03e_pull_referees.py", "--season", "2025"]),
    (6,  "Refs 2024",                     ["03e_pull_referees.py", "--season", "2024"]),
    (7,  "Refs 2023",                     ["03e_pull_referees.py", "--season", "2023"]),
    (8,  "Refs 2022",                     ["03e_pull_referees.py", "--season", "2022"]),
    (9,  "Travel distances",              ["03f_compute_travel.py"]),
    (10, "Recency features",              ["03g_recency_features.py"]),
    (11, "Trend slopes",                  ["03h_compute_trends.py"]),
    (12, "Historical DK lines (2026)",    ["10_scrape_historical_lines.py", "--season", "2026"]),
    (13, "Build features (game_features_v2)", ["04_build_features.py"]),
    (14, "Spread combo backtest",         ["05_backtest_all_combos.py"]),
    (15, "Totals combo backtest",         ["05b_backtest_totals_combos.py"]),
]


def run_step(num, label, cmd_parts):
    script = os.path.join(SCRIPTS, cmd_parts[0])
    args   = cmd_parts[1:]
    print(f"\n{'='*60}")
    print(f"STEP {num}/15 — {label}")
    print(f"  {datetime.now().strftime('%H:%M:%S')} | {cmd_parts[0]} {' '.join(args)}")
    print(f"{'='*60}")

    start = time.time()
    result = subprocess.run([PYTHON, script] + args, cwd=ROOT)
    elapsed = time.time() - start

    if result.returncode != 0:
        print(f"\n  ✗ FAILED (exit {result.returncode}) after {elapsed:.0f}s")
        print(f"  To resume: python scripts/00_full_rebuild.py --from step{num+1}")
        return False

    print(f"\n  ✓ Done in {elapsed/60:.1f} min")
    return True


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--from',  dest='from_step', type=int, default=1,
                        help='Resume from step N (e.g. --from 5)')
    parser.add_argument('--only',  dest='only_step', type=int, default=None,
                        help='Run only step N')
    parser.add_argument('--list',  action='store_true',
                        help='List all steps and exit')
    parser.add_argument('--through', dest='through_step', type=int, default=None,
                        help='Run steps from --from through N')
    args = parser.parse_args()

    if args.list:
        print("Full rebuild steps:")
        for num, label, cmd in STEPS:
            print(f"  {num:>2}. {label:<40} {cmd[0]}")
        sys.exit(0)

    start_total = time.time()
    print(f"NCAAB Full Pipeline Rebuild")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    for num, label, cmd in STEPS:
        if args.only_step and num != args.only_step:
            continue
        if num < args.from_step:
            print(f"  Skipping step {num} ({label})")
            continue
        if args.through_step and num > args.through_step:
            break

        ok = run_step(num, label, cmd)
        if not ok:
            sys.exit(1)

    elapsed = (time.time() - start_total) / 3600
    print(f"\n{'='*60}")
    print(f"ALL STEPS COMPLETE in {elapsed:.1f} hrs")
    print(f"Next: review backtest results and retrain models")
    print(f"  python scripts/06_train_final_model.py --combo 'BEST_COMBO'")
    print(f"  python scripts/08_train_totals_model.py --combo 'BEST_COMBO'")
    print(f"{'='*60}")
