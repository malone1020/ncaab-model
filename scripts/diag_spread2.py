"""
diag_spread2.py - Understand what the spread number means by looking at big favorites
"""
import sqlite3, pandas as pd, os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

df = pd.read_sql("""
    SELECT gl.game_id, gl.game_date, gl.home_team, gl.away_team,
           gl.spread, g.home_score, g.away_score,
           (g.home_score - g.away_score) AS actual_margin
    FROM game_lines gl
    JOIN games g ON gl.game_id = g.id
    WHERE gl.spread IS NOT NULL AND g.home_score IS NOT NULL
    GROUP BY gl.game_id
""", conn)
conn.close()

# Show games where |spread| > 20 so it's obvious who was favored
big = df[df['spread'].abs() > 20].copy()
print(f"Games with |spread| > 20: {len(big)}")
print()
print("Sample — large spread games (who actually won vs spread sign):")
print(big[['game_date','home_team','away_team','spread','actual_margin']].head(20).to_string())

print()
# When spread is very negative (e.g. -30), who wins?
very_neg = df[df['spread'] < -20]
print(f"\nWhen spread < -20: home wins {(very_neg['actual_margin']>0).mean():.1%} of time ({len(very_neg)} games)")
print(f"  Mean actual_margin: {very_neg['actual_margin'].mean():.1f}")

very_pos = df[df['spread'] > 20]
print(f"\nWhen spread > 20: home wins {(very_pos['actual_margin']>0).mean():.1%} of time ({len(very_pos)} games)")
print(f"  Mean actual_margin: {very_pos['actual_margin'].mean():.1f}")

# The key question: does spread predict actual_margin direction?
print(f"\nCorrelation spread vs actual_margin: {df['spread'].corr(df['actual_margin']):.4f}")
print("(Should be strongly negative if spread<0 = home favored)")
print("(Should be strongly positive if spread<0 = away favored)")
