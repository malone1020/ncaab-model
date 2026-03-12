"""
diag_buckets.py — Is the 0-3pt spread bucket edge real and consistent?
Also checks rolling feature coverage gap.
Run from scripts/: python diag_buckets.py
"""
import sqlite3, os, json
import pandas as pd
import numpy as np
from xgboost import XGBRegressor

ROOT  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB    = os.path.join(ROOT, 'data', 'basketball.db')
MODEL = os.path.join(ROOT, 'models', 'best_combo_model.json')

VIG   = -110
def ats_roi(bets): return (bets * (100/110) - (~bets) * 1).mean() if len(bets) else None

conn = sqlite3.connect(DB)
df   = pd.read_sql("SELECT * FROM game_features_v2", conn)
conn.close()

# Rebuild predictions using walk-forward (same as backtest)
FEATURE_COLS = [
    'conf_game','neutral_site','hca','hca_adj','home_b2b','away_b2b','rest_diff',
    'h_tvd_adj_em','a_tvd_adj_em','h_tvd_barthag','a_tvd_barthag',
    'h_tvd_efg_o','a_tvd_efg_o','h_tvd_efg_d','a_tvd_efg_d',
    'h_tvd_tov_o','a_tvd_tov_o','h_tvd_orb','a_tvd_orb',
    'tvd_em_gap','tvd_bar_gap','tvd_efg_gap','tvd_orb_gap','tvd_tov_gap',
]

df = df[df['spread'].notna() & df['ats_win'].notna()].copy()
seasons = sorted(df['season'].unique())

all_preds = []
for test_s in seasons[2:]:
    train = df[df['season'] < test_s].copy()
    test  = df[df['season'] == test_s].copy()

    avail = [c for c in FEATURE_COLS if c in train.columns and train[c].notna().mean() > 0.4]
    meds  = train[avail].median()
    X_tr  = train[avail].fillna(meds)
    y_tr  = train['actual_margin'].dropna()
    X_tr  = X_tr.loc[y_tr.index]
    X_te  = test[avail].fillna(meds)

    mdl = XGBRegressor(n_estimators=200, max_depth=4, learning_rate=0.05,
                       subsample=0.8, colsample_bytree=0.8, min_child_weight=10,
                       random_state=42, verbosity=0)
    mdl.fit(X_tr, y_tr)

    test = test.copy()
    test['pred'] = mdl.predict(X_te)
    test['edge'] = test['pred'] - (-test['spread'])
    all_preds.append(test)

results = pd.concat(all_preds)

# ── 0-3pt bucket by season ──────────────────────────────────────────────
print("=" * 60)
print("0-3pt SPREAD BUCKET — by season (edge≥3)")
print("=" * 60)
print(f"{'Season':<8} {'WR':>7} {'ROI':>7} {'Bets':>6}")
bucket = results[results['spread'].abs().between(0.5, 3.0)]
bucket_edge = bucket[bucket['edge'].abs() >= 3.0]
for s, g in bucket_edge.groupby('season'):
    bets = g['ats_win'].astype(bool)
    wr  = bets.mean()
    roi = ats_roi(bets)
    print(f"  {s:<6} {wr:.1%} {roi:+.1%} {len(bets):>6}")
tot = bucket_edge['ats_win'].astype(bool)
print(f"  {'TOTAL':<6} {tot.mean():.1%} {ats_roi(tot):+.1%} {len(tot):>6}")

# ── 3-6pt bucket by season ──────────────────────────────────────────────
print(f"\n{'='*60}")
print("3-6pt SPREAD BUCKET — by season (edge≥3)")
print("=" * 60)
print(f"{'Season':<8} {'WR':>7} {'ROI':>7} {'Bets':>6}")
bucket2 = results[results['spread'].abs().between(3.0, 6.0)]
bucket2_edge = bucket2[bucket2['edge'].abs() >= 3.0]
for s, g in bucket2_edge.groupby('season'):
    bets = g['ats_win'].astype(bool)
    wr  = bets.mean()
    roi = ats_roi(bets)
    print(f"  {s:<6} {wr:.1%} {roi:+.1%} {len(bets):>6}")
tot2 = bucket2_edge['ats_win'].astype(bool)
print(f"  {'TOTAL':<6} {tot2.mean():.1%} {ats_roi(tot2):+.1%} {len(tot2):>6}")

# ── Rolling feature coverage diagnosis ──────────────────────────────────
print(f"\n{'='*60}")
print("ROLLING FEATURE COVERAGE DIAGNOSIS")
print("=" * 60)
roll_cols = [c for c in df.columns if c.startswith('roll_') or 'rolling' in c.lower()]
if roll_cols:
    for col in roll_cols[:5]:
        cov = df[col].notna().mean()
        print(f"  {col}: {cov:.1%} coverage")
else:
    print("  No rolling columns found in game_features_v2")
    print(f"  All columns starting with 'roll': {[c for c in df.columns if 'roll' in c.lower()][:10]}")

# ── Edge distribution ────────────────────────────────────────────────────
print(f"\n{'='*60}")
print("EDGE DISTRIBUTION (0.5-9pt spread, all seasons)")
print("=" * 60)
bet_df = results[results['spread'].abs().between(0.5, 9.0)].copy()
for lo, hi in [(0,1),(1,2),(2,3),(3,4),(4,5),(5,7),(7,10),(10,20)]:
    sub = bet_df[bet_df['edge'].abs().between(lo, hi)]
    if len(sub) < 20: continue
    bets = sub['ats_win'].astype(bool)
    print(f"  edge {lo}-{hi}:  {bets.mean():.1%} WR  {ats_roi(bets):+.1%} ROI  ({len(sub)} bets)")

# ── Are we betting the right direction? ─────────────────────────────────
print(f"\n{'='*60}")
print("DIRECTION CHECK (edge≥3, 0.5-9pt spread)")
print("=" * 60)
e3 = bet_df[bet_df['edge'].abs() >= 3.0]
fav_home = e3[e3['edge'] > 0]   # betting home
fav_away = e3[e3['edge'] < 0]   # betting away
print(f"  Betting home (edge>0): {fav_home['ats_win'].mean():.1%} WR  ({len(fav_home)} bets)")
print(f"  Betting away (edge<0): {fav_away['ats_win'].mean():.1%} WR  ({len(fav_away)} bets)")
