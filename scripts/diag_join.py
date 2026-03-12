"""
diag_join.py - Check if game_lines.game_id correctly joins to games.id
"""
import sqlite3, pandas as pd, os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

# Check the join directly on a specific known game
print("=== game_lines sample (no join) ===")
gl = pd.read_sql("SELECT game_id, game_date, home_team, away_team, spread FROM game_lines WHERE game_date='2024-11-05' AND home_team LIKE '%Kansas%' LIMIT 5", conn)
print(gl.to_string())

print()
print("=== games sample (no join) ===")
g = pd.read_sql("SELECT id, game_date, home_team, away_team, home_score, away_score FROM games WHERE game_date='2024-11-05' AND home_team LIKE '%Kansas%' LIMIT 5", conn)
print(g.to_string())

print()
print("=== Do the IDs match? ===")
if len(gl) and len(g):
    print(f"  game_lines.game_id: {gl['game_id'].tolist()}")
    print(f"  games.id:           {g['id'].tolist()}")

print()
print("=== Direct join test ===")
joined = pd.read_sql("""
    SELECT gl.game_id, gl.game_date, gl.home_team as gl_home, gl.away_team as gl_away,
           gl.spread,
           g.home_team as g_home, g.away_team as g_away,
           g.home_score, g.away_score,
           (g.home_score - g.away_score) as actual_margin
    FROM game_lines gl
    JOIN games g ON gl.game_id = g.id
    WHERE gl.game_date = '2024-11-05'
    AND gl.home_team LIKE '%Kansas%'
    LIMIT 5
""", conn)
print(joined.to_string())

print()
print("=== Check if game_lines uses different ID format than games ===")
print("game_lines game_id samples:")
print(pd.read_sql("SELECT game_id FROM game_lines LIMIT 5", conn).to_string())
print()
print("games id samples:")
print(pd.read_sql("SELECT id FROM games LIMIT 5", conn).to_string())

conn.close()
