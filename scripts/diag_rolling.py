"""
diag_rolling.py - Check rolling feature coverage and quality
"""
import sqlite3, pandas as pd, numpy as np, os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

df = pd.read_sql("""
    SELECT h_r_ppp, a_r_ppp, h_r_efg, a_r_efg, roll_ppp_gap,
           h_r_pts, a_r_pts, season, game_date
    FROM game_features_v2
    WHERE spread IS NOT NULL
""", conn)
conn.close()

print(f"Games with spread: {len(df)}")
print(f"h_r_ppp coverage: {df['h_r_ppp'].notna().sum()} ({df['h_r_ppp'].notna().mean():.1%})")
print(f"roll_ppp_gap coverage: {df['roll_ppp_gap'].notna().sum()} ({df['roll_ppp_gap'].notna().mean():.1%})")
print()

# Check if rolling features correlate with actual margin at all
conn2 = sqlite3.connect(DB)
full = pd.read_sql("SELECT h_r_ppp, a_r_ppp, roll_ppp_gap, roll_efg_gap, actual_margin, spread FROM game_features_v2 WHERE spread IS NOT NULL", conn2)
conn2.close()

for col in ['h_r_ppp', 'a_r_ppp', 'roll_ppp_gap', 'roll_efg_gap']:
    valid = full[[col, 'actual_margin']].dropna()
    if len(valid) > 100:
        corr = valid[col].corr(valid['actual_margin'])
        print(f"  {col}: r={corr:.4f} ({len(valid)} games)")

print()
print("Sample rolling values:")
print(full[['h_r_ppp','a_r_ppp','roll_ppp_gap','actual_margin','spread']].dropna().head(10).to_string())
