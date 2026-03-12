"""
diag_spread_convention.py
Investigate spread sign convention inconsistency.
"""
import sqlite3, pandas as pd, numpy as np, os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

# Pull game_lines joined with actual scores
df = pd.read_sql("""
    SELECT gl.game_id, gl.game_date, gl.home_team, gl.away_team,
           gl.spread, gl.spread_open,
           g.home_score, g.away_score,
           (g.home_score - g.away_score) AS actual_margin
    FROM game_lines gl
    JOIN games g ON gl.game_id = g.id
    WHERE gl.spread IS NOT NULL
      AND g.home_score IS NOT NULL
""", conn)
conn.close()

print(f"Total games: {len(df)}")
print(f"Spread distribution: mean={df['spread'].mean():.2f}, median={df['spread'].median():.2f}")
print(f"% spread < 0: {(df['spread']<0).mean():.1%}")
print(f"% spread > 0: {(df['spread']>0).mean():.1%}")
print()

# If spread is from home perspective (home-favored = negative):
# home covers when actual_margin + spread > 0
# If spread is from away perspective (away-favored = negative):
# home covers when actual_margin - spread > 0

df['covers_if_home_perspective'] = (df['actual_margin'] + df['spread'] > 0).astype(int)
df['covers_if_away_perspective'] = (df['actual_margin'] - df['spread'] > 0).astype(int)

neg = df[df['spread'] < 0]
pos = df[df['spread'] > 0]

print("=== When spread < 0 (should mean home favored) ===")
print(f"  Home win rate (actual): {(neg['actual_margin']>0).mean():.3f}")
print(f"  Cover rate (home perspective): {neg['covers_if_home_perspective'].mean():.3f}")
print(f"  Cover rate (away perspective): {neg['covers_if_away_perspective'].mean():.3f}")
print(f"  Sample (home wins by how much vs spread):")
print(neg[['home_team','away_team','spread','actual_margin']].head(5).to_string())

print()
print("=== When spread > 0 (should mean away favored) ===")
print(f"  Home win rate (actual): {(pos['actual_margin']>0).mean():.3f}")
print(f"  Cover rate (home perspective): {pos['covers_if_home_perspective'].mean():.3f}")
print(f"  Cover rate (away perspective): {pos['covers_if_away_perspective'].mean():.3f}")
print(f"  Sample:")
print(pos[['home_team','away_team','spread','actual_margin']].head(5).to_string())

print()
print("=== Checking if positive spreads are neutral site games ===")
pos_neutral = df[(df['spread']>0)]
# Check game table for neutral_site
conn2 = sqlite3.connect(DB)
neutral = pd.read_sql("SELECT id, neutral_site FROM games WHERE neutral_site=1 LIMIT 5", conn2)
neutral_ids = pd.read_sql("SELECT id FROM games WHERE neutral_site=1", conn2)['id'].tolist()
conn2.close()
pos_neutral_pct = df[df['spread']>0]['game_id'].isin(neutral_ids).mean()
print(f"  % of spread>0 games that are neutral site: {pos_neutral_pct:.1%}")
