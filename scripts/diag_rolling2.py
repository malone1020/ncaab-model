import sqlite3, pandas as pd, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

print("Box score seasons:")
for r in conn.execute("SELECT season, COUNT(DISTINCT game_id) FROM game_team_stats s JOIN games g ON s.game_id=g.id GROUP BY g.season ORDER BY g.season"):
    print(f"  {r[0]}: {r[1]} games")

print("\nGames with spread by season:")
for r in conn.execute("SELECT season, COUNT(*) FROM game_features_v2 WHERE spread IS NOT NULL GROUP BY season ORDER BY season"):
    print(f"  {r[0]}: {r[1]}")

print("\nRolling coverage by season:")
for r in conn.execute("SELECT season, COUNT(*) total, SUM(CASE WHEN h_r_ppp IS NOT NULL THEN 1 ELSE 0 END) has_rolling FROM game_features_v2 WHERE spread IS NOT NULL GROUP BY season ORDER BY season"):
    print(f"  {r[0]}: {r[2]}/{r[1]} ({r[2]/r[1]:.0%})")
conn.close()
