"""
diag_edge_threshold.py
Shows WR and ROI broken out by edge threshold for the best combo.
Also shows results by season and by spread bucket.
Run from project root: python scripts/diag_edge_threshold.py
"""
import sqlite3, os
import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.impute import SimpleImputer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')

BEST_COMBO = ['TVD','HA_SHOT','HA_MATCHUP']  # + CONTEXT always

FEATURE_GROUPS = {
    'CONTEXT': ['neutral_site','conf_game','hca_adj','rest_diff'],
    'TVD': [
        'h_tvd_adj_em','h_tvd_adj_o','h_tvd_adj_d','h_tvd_adj_t','h_tvd_barthag','h_tvd_wab',
        'a_tvd_adj_em','a_tvd_adj_o','a_tvd_adj_d','a_tvd_adj_t','a_tvd_barthag','a_tvd_wab',
        'tvd_em_gap','has_tvd_home','has_tvd_away',
    ],
    'HA_SHOT': [
        'h_ha_o_3par','h_ha_d_3par','h_ha_o_prox','h_ha_d_prox',
        'h_ha_o_mrar','h_ha_d_mrar','h_ha_o_fg_pct','h_ha_d_fg_pct',
        'a_ha_o_3par','a_ha_d_3par','a_ha_o_prox','a_ha_d_prox',
        'a_ha_o_mrar','a_ha_d_mrar','a_ha_o_fg_pct','a_ha_d_fg_pct',
        'ha_gap_o_3par','ha_gap_d_3par','ha_gap_o_prox','ha_gap_d_prox',
        'ha_gap_o_mrar','ha_gap_d_mrar',
    ],
    'HA_MATCHUP': [
        'ha_3par_matchup_h','ha_3par_matchup_a',
        'ha_prox_matchup_h','ha_prox_matchup_a',
    ],
}

PAYOUT = 100/110

def roi(won, total):
    return (won * PAYOUT - (total - won)) / total if total else None

conn = sqlite3.connect(DB)
df = pd.read_sql("SELECT * FROM game_features_v2", conn)
conn.close()

feat_cols = []
for g in ['CONTEXT'] + BEST_COMBO:
    feat_cols += [c for c in FEATURE_GROUPS[g] if c in df.columns]

df = df[df['spread'].notna() & df['ats_win'].notna()].copy()
seasons = sorted(df['season'].unique())

all_preds = []
for test_s in seasons[2:]:
    train_ss = [s for s in seasons if s < test_s][-4:]
    train = df[df['season'].isin(train_ss)].copy()
    test  = df[df['season'] == test_s].copy()
    test  = test[test['spread'].abs().between(0.5, 9.0)]
    if len(test) < 50: continue

    X_tr = train[feat_cols].copy()
    y_tr = train['actual_margin'].copy()
    X_te = test[feat_cols].copy()

    valid = [c for c in feat_cols if X_tr[c].notna().sum() > 50]
    X_tr, X_te = X_tr[valid], X_te[valid]

    imp = SimpleImputer(strategy='median')
    X_tr_i = imp.fit_transform(X_tr)
    X_te_i  = imp.transform(X_te)

    mdl = XGBRegressor(n_estimators=200, max_depth=4, learning_rate=0.05,
                       subsample=0.8, colsample_bytree=0.8,
                       min_child_weight=10, reg_lambda=2.0,
                       random_state=42, n_jobs=-1, verbosity=0)
    mdl.fit(X_tr_i, y_tr)

    test = test.copy()
    test['pred'] = mdl.predict(X_te_i)
    test['edge'] = test['pred'] - (-test['spread'])
    test['bet_won'] = (((test['edge'] > 0) & (test['ats_win'] == 1)) |
                       ((test['edge'] < 0) & (test['ats_win'] == 0))).astype(int)
    all_preds.append(test)

results = pd.concat(all_preds)
print(f"Total out-of-sample games (0.5-9pt spread): {len(results):,}")

# ── Edge threshold table ──────────────────────────────────────────────────────
print("\n" + "="*60)
print("ROI BY MINIMUM EDGE THRESHOLD")
print("="*60)
print(f"{'Min Edge':>10}  {'Bets':>6}  {'WR':>7}  {'ROI':>8}")
print("-"*40)
for thresh in [1, 2, 3, 4, 5, 6, 7, 8, 10]:
    sub = results[results['edge'].abs() >= thresh]
    if len(sub) < 50: break
    w = sub['bet_won'].sum()
    print(f"  {thresh:>6}+   {len(sub):>6}  {w/len(sub):.3f}  {roi(w,len(sub)):>+.4f}")

# ── Results by season (edge >= 3) ─────────────────────────────────────────────
print("\n" + "="*60)
print("BY SEASON (edge >= 3)")
print("="*60)
print(f"{'Season':>8}  {'Bets':>6}  {'WR':>7}  {'ROI':>8}")
print("-"*40)
e3 = results[results['edge'].abs() >= 3]
for s, g in e3.groupby('season'):
    w = g['bet_won'].sum()
    print(f"  {s:>6}   {len(g):>6}  {w/len(g):.3f}  {roi(w,len(g)):>+.4f}")
w = e3['bet_won'].sum()
print(f"  {'TOTAL':>6}   {len(e3):>6}  {w/len(e3):.3f}  {roi(w,len(e3)):>+.4f}")

# ── Results by spread bucket (edge >= 3) ──────────────────────────────────────
print("\n" + "="*60)
print("BY SPREAD BUCKET (edge >= 3)")
print("="*60)
print(f"{'Bucket':>12}  {'Bets':>6}  {'WR':>7}  {'ROI':>8}")
print("-"*45)
for lo, hi in [(0.5,3),(3,6),(6,9)]:
    sub = e3[e3['spread'].abs().between(lo, hi)]
    if len(sub) < 20: continue
    w = sub['bet_won'].sum()
    print(f"  {lo}-{hi}pt      {len(sub):>6}  {w/len(sub):.3f}  {roi(w,len(sub)):>+.4f}")

# ── Betting direction (edge >= 3) ─────────────────────────────────────────────
print("\n" + "="*60)
print("BETTING DIRECTION (edge >= 3)")
print("="*60)
home_bets = e3[e3['edge'] > 0]
away_bets = e3[e3['edge'] < 0]
wh = home_bets['bet_won'].sum()
wa = away_bets['bet_won'].sum()
print(f"  Betting home:  {len(home_bets):>5} bets  {wh/len(home_bets):.3f} WR  {roi(wh,len(home_bets)):>+.4f} ROI")
print(f"  Betting away:  {len(away_bets):>5} bets  {wa/len(away_bets):.3f} WR  {roi(wa,len(away_bets)):>+.4f} ROI")
