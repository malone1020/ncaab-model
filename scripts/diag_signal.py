"""
diag_signal.py — check if haslametrics/torvik_preds add independent signal beyond torvik_daily
Run from scripts/: python diag_signal.py
"""
import sqlite3, pandas as pd, numpy as np, os
from numpy.linalg import lstsq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')

conn = sqlite3.connect(DB)
df = pd.read_sql("SELECT * FROM game_features_v2", conn)
conn.close()

df_bet = df[df['spread'].notna() & df['spread'].abs().between(0.5, 9.0)].copy()
print(f"Games with spread 0.5-9: {len(df_bet)}")
print(f"\n--- Coverage ---")
print(f"  tvd_em_gap:     {df_bet['tvd_em_gap'].notna().sum():,}")
print(f"  ha_eff_gap:     {df_bet['ha_eff_gap'].notna().sum():,}")
print(f"  torvik_pred:    {df_bet['torvik_pred'].notna().sum():,}")
print(f"  tvd + ha:       {(df_bet['tvd_em_gap'].notna() & df_bet['ha_eff_gap'].notna()).sum():,}")
print(f"  tvd + pred:     {(df_bet['tvd_em_gap'].notna() & df_bet['torvik_pred'].notna()).sum():,}")
print(f"  all three:      {(df_bet['tvd_em_gap'].notna() & df_bet['ha_eff_gap'].notna() & df_bet['torvik_pred'].notna()).sum():,}")

# ── Partial correlations controlling for tvd_em_gap ──
print(f"\n--- Partial correlations (controlling for tvd_em_gap) ---")

def partial_r(df_sub, predictor, target='actual_margin', control='tvd_em_gap'):
    sub = df_sub[[target, control, predictor]].dropna()
    if len(sub) < 100: return None, len(sub)
    X = sub[control].values.reshape(-1,1)
    resid_target    = sub[target].values    - X @ lstsq(X, sub[target].values,    rcond=None)[0]
    resid_predictor = sub[predictor].values - X @ lstsq(X, sub[predictor].values, rcond=None)[0]
    return np.corrcoef(resid_predictor, resid_target)[0,1], len(sub)

for col in ['ha_eff_gap','ha_d_eff_gap','h_ha_o_eff','a_ha_o_eff',
            'torvik_pred','torvik_vs_spread','tvd_efg_gap','tvd_orb_gap',
            'tvd_tov_gap','tvd_wab_gap','line_move']:
    if col not in df_bet.columns: continue
    r, n = partial_r(df_bet, col)
    if r is None: continue
    print(f"  {col:<30} partial_r={r:+.4f}  (n={n:,})")

# ── Raw correlations on full bet sample ──
print(f"\n--- Raw correlations on full 0.5-9pt sample ---")
for col in ['tvd_em_gap','tvd_bar_gap','ha_eff_gap','torvik_pred','torvik_vs_spread',
            'line_move','tvd_efg_gap','tvd_orb_gap']:
    if col not in df_bet.columns: continue
    sub = df_bet[['actual_margin', col]].dropna()
    if len(sub) < 100: continue
    r = np.corrcoef(sub[col], sub['actual_margin'])[0,1]
    print(f"  {col:<30} r={r:+.4f}  (n={len(sub):,})")

# ── Season coverage for torvik_daily ──
print(f"\n--- torvik_daily coverage by season ---")
for s, g in df_bet.groupby('season'):
    n_total = len(g)
    n_tvd   = g['tvd_em_gap'].notna().sum()
    print(f"  {s}: {n_tvd}/{n_total} ({100*n_tvd/n_total:.0f}%)")
