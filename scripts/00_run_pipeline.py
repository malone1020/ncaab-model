"""
00_run_pipeline.py
==================
Master orchestration script. Runs the full data pipeline in dependency order.

Usage:
  python scripts/00_run_pipeline.py             # full rebuild
  python scripts/00_run_pipeline.py --update    # refresh current-season data only
  python scripts/00_run_pipeline.py --audit     # audit only, no changes
  python scripts/00_run_pipeline.py --features  # rebuild features + backtest only

Steps executed (in order):
  0. Audit current DB state
  1. Pull / refresh Torvik daily snapshots
  2. Pull / refresh KenPom daily + fanmatch
  3. Pull referee assignments (03e)
  4. Compute travel distance (03f)
  5. Rebuild rolling efficiency
  6. Build unified feature matrix (04)
  7. Backtest all combos (05)
"""

import subprocess, sys, os, time

ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS = os.path.join(ROOT, 'scripts')

def run(script, label, extra_args=None):
    path = os.path.join(SCRIPTS, script)
    cmd  = [sys.executable, path] + (extra_args or [])
    print(f"\n{'='*60}")
    print(f"  STEP: {label}")
    print(f"{'='*60}")
    start = time.time()
    result = subprocess.run(cmd, cwd=ROOT)
    elapsed = time.time() - start
    if result.returncode != 0:
        print(f"\n❌ FAILED: {label} (exit {result.returncode})")
        ans = input("Continue anyway? [y/N]: ").strip().lower()
        if ans != 'y':
            sys.exit(1)
    else:
        print(f"\n✅ {label} complete ({elapsed:.0f}s)")
    return result.returncode == 0


def main():
    args = sys.argv[1:]
    audit_only   = '--audit'    in args
    update_only  = '--update'   in args
    features_only= '--features' in args

    print("\n" + "="*60)
    print("  NCAAB MODEL — MASTER PIPELINE")
    print("="*60)

    # Always audit first
    run('00_audit_data.py', 'Data audit')

    if audit_only:
        print("\nAudit complete. Exiting (--audit mode).")
        return

    if not features_only:
        # Data collection phase
        run('03b_backfill_torvik_daily.py',   'Torvik daily snapshots (refresh)')
        run('03d_pull_kenpom_archive.py',      'KenPom daily + fanmatch (refresh)')
        run('03e_pull_referees.py',            'Referee assignments')
        run('03f_compute_travel.py',           'Travel distance + timezone')
        run('build_rolling_features.py',       'Rolling box score efficiency')
        run('03g_recency_features.py',         'Recency-weighted efficiency + experience')

    # Feature + model phase
    run('04_build_features.py',   'Build unified feature matrix')
    run('05_backtest_all_combos.py', 'Backtest all feature combos')

    if not update_only and not features_only:
        combo = input("\nEnter winning combo for production model (e.g. CONTEXT+TVD+KPD+KP_FANMATCH): ").strip()
        if combo:
            run('06_train_final_model.py', 'Train production model', ['--combo', combo])

    print("\n" + "="*60)
    print("  PIPELINE COMPLETE")
    print("="*60)


if __name__ == '__main__':
    main()
