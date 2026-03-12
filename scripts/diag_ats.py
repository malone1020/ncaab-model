"""
diag_ats.py - Check ats_win distribution and spread/margin relationship
"""
import sqlite3, pandas as pd, numpy as np, os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')

conn = sqlite3.connect(DB)
df = pd.read_sql("SELECT actual_margin, spread, ats_win FROM game_features_v2 WHERE spread IS NOT NULL AND ats_win IS NOT NULL", conn)
conn.close()

print(f"Games with spread+ats_win: {len(df)}")
print(f"ats_win distribution: {df['ats_win'].value_counts().to_dict()}")
print(f"Overall home ATS win rate: {df['ats_win'].mean():.3f}")
print()

# Check a few rows manually
print("Sample rows (actual_margin, spread, ats_win):")
print(df.head(10).to_string())
print()

# The spread sign convention
print("Spread stats:")
print(f"  Mean spread: {df['spread'].mean():.2f}")
print(f"  % negative spreads: {(df['spread']<0).mean():.1%}")
print(f"  % positive spreads: {(df['spread']>0).mean():.1%}")
print()

# Verify: when spread is negative (home favored), does home win more?
neg = df[df['spread'] < 0]
pos = df[df['spread'] > 0]
print(f"When spread<0 (home favored): home ATS win = {neg['ats_win'].mean():.3f} ({len(neg)} games)")
print(f"When spread>0 (away favored): home ATS win = {pos['ats_win'].mean():.3f} ({len(pos)} games)")
print()

# Check if actual_margin is correlated with ats_win suspiciously
# If ats_win = (actual_margin + spread > 0), correlation should be moderate
corr = df['actual_margin'].corr(df['ats_win'])
print(f"Correlation actual_margin vs ats_win: {corr:.4f}")
print(f"(Should be ~0.5-0.7 if spread is random noise around actual margin)")
