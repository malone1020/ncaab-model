import sqlite3, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

print("Most recent dates in game_lines with spreads:")
rows = conn.execute("""
    SELECT game_date, COUNT(*) as n
    FROM game_lines WHERE spread IS NOT NULL
    GROUP BY game_date ORDER BY game_date DESC LIMIT 10
""").fetchall()
for r in rows: print(f"  {r[0]}: {r[1]} games")

print("\nSample from a recent date:")
rows = conn.execute("""
    SELECT game_date, home_team, away_team, spread, over_under
    FROM game_lines WHERE spread IS NOT NULL
    ORDER BY game_date DESC LIMIT 5
""").fetchall()
for r in rows: print(f"  {r}")
conn.close()
