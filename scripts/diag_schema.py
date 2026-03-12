import sqlite3, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

print("=== game_lines columns ===")
for row in conn.execute("PRAGMA table_info(game_lines)"):
    print(f"  {row[1]} ({row[2]})")

print("\n=== game_lines sample ===")
import pandas as pd
gl = pd.read_sql("SELECT * FROM game_lines LIMIT 5", conn)
print(gl.to_string())

print("\n=== games sample ===")
g = pd.read_sql("SELECT id, game_date, home_team, away_team, home_score, away_score FROM games LIMIT 5", conn)
print(g.to_string())

print("\n=== game_lines ID range ===")
print(conn.execute("SELECT MIN(game_id), MAX(game_id), COUNT(*) FROM game_lines").fetchone())

print("\n=== games ID range ===")
print(conn.execute("SELECT MIN(id), MAX(id), COUNT(*) FROM games").fetchone())
