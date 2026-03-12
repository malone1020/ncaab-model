"""
diag_leakage.py - Check for data leakage in feature matrix
Run from scripts/: python diag_leakage.py
"""
import sqlite3, pandas as pd, numpy as np, os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')

conn = sqlite3.connect(DB)
df = pd.read_sql("SELECT * FROM game_features_v2", conn)
conn.close()

print(f"Total rows: {len(df)}")
print(f"Columns: {len(df.columns)}")

# Check correlation of every feature with actual_margin
target = df['actual_margin'].dropna()
print(f"\nRows with actual_margin: {len(target)}")

print("\n=== TOP 20 features correlated with actual_margin ===")
corrs = {}
for col in df.columns:
    if col in ('actual_margin','spread','ats_win','over_under','game_id','season',
               'game_date','home_team','away_team','home_score','away_score'): continue
    try:
        c = df[col].corr(df['actual_margin'])
        if not np.isnan(c):
            corrs[col] = abs(c)
    except: pass

for col, c in sorted(corrs.items(), key=lambda x: -x[1])[:20]:
    print(f"  {col:40s}  r={c:.4f}")

print("\n=== Checking if spread is in feature columns ===")
spread_cols = [c for c in df.columns if 'spread' in c.lower() or 'line' in c.lower()]
print(f"  Spread-related cols: {spread_cols}")

print("\n=== torvik_season sample (2019 Kansas) ===")
row = df[(df['season']==2019) & (df['home_team'].str.contains('Kansas', case=False, na=False))].head(1)
if len(row):
    tv_cols = [c for c in df.columns if c.startswith('h_tv_') or c.startswith('tv_em')]
    print(row[tv_cols + ['actual_margin','spread']].to_string())
