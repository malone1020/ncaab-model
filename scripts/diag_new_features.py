"""
diag_new_features.py
====================
Investigates why TRAVEL, REFS, RECENCY, EXPERIENCE didn't appear in top combos.
Checks: coverage, null rates, and whether features were actually written to game_features_v2.

Run: python scripts/diag_new_features.py
"""
import sqlite3, os
import pandas as pd
import numpy as np

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

def section(t): print(f"\n{'='*60}\n  {t}\n{'='*60}")

# ── 1. Check new tables exist and have data ───────────────────────────────
section("1. NEW TABLE ROW COUNTS")
for tbl in ['team_travel', 'referee_game', 'referee_profiles', 'recency_eff', 'team_experience']:
    try:
        n = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"  {tbl:<30} {n:>8,} rows")
    except Exception as e:
        print(f"  {tbl:<30} ❌ MISSING ({e})")

# ── 2. Check coverage in game_features_v2 ────────────────────────────────
section("2. FEATURE COVERAGE IN game_features_v2 (must have spread)")
cols = [r[1] for r in conn.execute("PRAGMA table_info(game_features_v2)").fetchall()]
print(f"  Total columns: {len(cols)}")

# Key new columns to check
check_cols = [
    # Travel
    'away_travel_miles', 'tz_crossings', 'away_road_game_n', 'away_long_trip',
    # Refs
    'ref_avg_fpg', 'ref_home_bias', 'has_ref_data',
    # Recency
    'h_rew_adj_em', 'a_rew_adj_em', 'rew_em_gap', 'trend_em_gap', 'has_rew',
    # Experience
    'h_experience', 'a_experience', 'exp_gap', 'has_experience',
]

total_with_spread = conn.execute(
    "SELECT COUNT(*) FROM game_features_v2 WHERE spread IS NOT NULL"
).fetchone()[0]
print(f"  Total games with spread: {total_with_spread:,}\n")

missing_cols = []
for col in check_cols:
    if col not in cols:
        print(f"  ❌ MISSING COLUMN: {col}")
        missing_cols.append(col)
        continue
    n = conn.execute(
        f"SELECT COUNT(*) FROM game_features_v2 WHERE {col} IS NOT NULL AND spread IS NOT NULL"
    ).fetchone()[0]
    pct = n / total_with_spread * 100 if total_with_spread else 0
    status = "✅" if pct > 50 else ("⚠️ " if pct > 10 else "❌")
    print(f"  {status} {col:<35} {n:>6,}/{total_with_spread:,} ({pct:.1f}%)")

# ── 3. Sample values for new features ────────────────────────────────────
section("3. SAMPLE VALUES (non-null)")
if 'away_travel_miles' in cols:
    r = conn.execute("""
        SELECT MIN(away_travel_miles), MAX(away_travel_miles), AVG(away_travel_miles)
        FROM game_features_v2 WHERE away_travel_miles IS NOT NULL
    """).fetchone()
    print(f"  away_travel_miles: min={r[0]:.0f}, max={r[1]:.0f}, avg={r[2]:.0f} miles")

if 'ref_avg_fpg' in cols:
    r = conn.execute("""
        SELECT MIN(ref_avg_fpg), MAX(ref_avg_fpg), AVG(ref_avg_fpg)
        FROM game_features_v2 WHERE ref_avg_fpg IS NOT NULL
    """).fetchone()
    if r[0]: print(f"  ref_avg_fpg:       min={r[0]:.1f}, max={r[1]:.1f}, avg={r[2]:.1f}")

if 'h_rew_adj_em' in cols:
    r = conn.execute("""
        SELECT MIN(h_rew_adj_em), MAX(h_rew_adj_em), AVG(h_rew_adj_em)
        FROM game_features_v2 WHERE h_rew_adj_em IS NOT NULL
    """).fetchone()
    if r[0]: print(f"  h_rew_adj_em:      min={r[0]:.2f}, max={r[1]:.2f}, avg={r[2]:.2f}")

if 'exp_gap' in cols:
    r = conn.execute("""
        SELECT MIN(exp_gap), MAX(exp_gap), AVG(exp_gap)
        FROM game_features_v2 WHERE exp_gap IS NOT NULL
    """).fetchone()
    if r[0]: print(f"  exp_gap:           min={r[0]:.2f}, max={r[1]:.2f}, avg={r[2]:.2f}")

# ── 4. Spot-check a single game for all new features ─────────────────────
section("4. SPOT-CHECK: RECENT GAME WITH SPREAD")
if not missing_cols:
    game = conn.execute("""
        SELECT game_date, home_team, away_team,
               spread, away_travel_miles, tz_crossings, away_road_game_n,
               ref_avg_fpg, ref_home_bias, has_ref_data,
               h_rew_adj_em, a_rew_adj_em, rew_em_gap,
               h_experience, a_experience, exp_gap
        FROM game_features_v2
        WHERE spread IS NOT NULL
          AND away_travel_miles IS NOT NULL
        ORDER BY game_date DESC LIMIT 5
    """).fetchall()
    for row in game:
        print(f"\n  {row[0]} | {row[2]} @ {row[1]} | spread={row[3]}")
        print(f"    travel={row[4]:.0f}mi  tz={row[5]}  road_n={row[6]}")
        print(f"    ref_fpg={row[7]}  ref_bias={row[8]}  has_ref={row[9]}")
        print(f"    h_rew_em={row[10]}  a_rew_em={row[11]}  rew_gap={row[12]}")
        print(f"    h_exp={row[13]}  a_exp={row[14]}  exp_gap={row[15]}")
else:
    print(f"  Skipped — {len(missing_cols)} columns missing from game_features_v2")
    print(f"  Missing: {missing_cols}")
    print(f"\n  ⚠️  The new scripts (03e-03g) need to be run BEFORE 04_build_features.py")
    print(f"  Run order:")
    print(f"    python scripts/03e_pull_referees.py")
    print(f"    python scripts/03f_compute_travel.py")
    print(f"    python scripts/03g_recency_features.py")
    print(f"    python scripts/04_build_features.py   ← must run AFTER")
    print(f"    python scripts/05_backtest_all_combos.py")

# ── 5. Win rate by feature presence ──────────────────────────────────────
section("5. RAW WIN RATE BY FEATURE PRESENCE (sanity check)")
print("  Checks: does each feature correlate with ATS outcome at all?\n")
gf = pd.read_sql("""
    SELECT ats_win, away_travel_miles, tz_crossings,
           ref_avg_fpg, ref_home_bias,
           rew_em_gap, exp_gap
    FROM game_features_v2
    WHERE spread BETWEEN 0.5 AND 9.0 AND ats_win IS NOT NULL
""", conn)

if 'away_travel_miles' in gf.columns and gf['away_travel_miles'].notna().sum() > 100:
    for col in ['away_travel_miles', 'tz_crossings', 'ref_avg_fpg', 'rew_em_gap', 'exp_gap']:
        if col not in gf.columns or gf[col].notna().sum() < 50:
            continue
        # Split into high/low and compare ATS win rates
        med = gf[col].median()
        hi  = gf[gf[col] > med]['ats_win'].mean()
        lo  = gf[gf[col] <= med]['ats_win'].mean()
        n_hi = (gf[col] > med).sum()
        n_lo = (gf[col] <= med).sum()
        print(f"  {col}:")
        print(f"    High (>{med:.1f}): {hi:.3f} WR (n={n_hi:,})")
        print(f"    Low  (<={med:.1f}): {lo:.3f} WR (n={n_lo:,})")
        print(f"    Difference: {abs(hi-lo):.3f} — {'signal present' if abs(hi-lo) > 0.005 else 'no signal'}")
else:
    print("  ⚠️  new features not in game_features_v2 yet — run collection scripts first")

conn.close()
print("\n\nDiagnostic complete.")
