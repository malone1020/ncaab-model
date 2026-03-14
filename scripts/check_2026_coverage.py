import sqlite3, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

print("=== 2025-26 SEASON DATA COVERAGE ===\n")

# games table
r = conn.execute("""
    SELECT COUNT(*), MIN(game_date), MAX(game_date)
    FROM games WHERE season=2026
""").fetchone()
print(f"games (2026):        {r[0]:,} rows | {r[1]} to {r[2]}")

# game_lines
r = conn.execute("""
    SELECT COUNT(*), MIN(game_date), MAX(game_date)
    FROM game_lines WHERE season=2026
""").fetchone()
print(f"game_lines (2026):   {r[0]:,} rows | {r[1]} to {r[2]}")

# torvik_daily
r = conn.execute("""
    SELECT COUNT(*), MIN(snapshot_date), MAX(snapshot_date)
    FROM torvik_daily WHERE season=2026
""").fetchone()
print(f"torvik_daily (2026): {r[0]:,} rows | {r[1]} to {r[2]}")

# kenpom_daily
r = conn.execute("""
    SELECT COUNT(*), MIN(snapshot_date), MAX(snapshot_date)
    FROM kenpom_daily WHERE season=2026
""").fetchone()
print(f"kenpom_daily (2026): {r[0]:,} rows | {r[1]} to {r[2]}")

# recency_eff
r = conn.execute("""
    SELECT COUNT(*), MIN(game_date), MAX(game_date)
    FROM recency_eff WHERE season=2026
""").fetchone()
print(f"recency_eff (2026):  {r[0]:,} rows | {r[1]} to {r[2]}")

# game_features_v2
r = conn.execute("""
    SELECT COUNT(*), MIN(game_date), MAX(game_date)
    FROM game_features_v2 WHERE season=2026
""").fetchone()
print(f"features_v2 (2026):  {r[0]:,} rows | {r[1]} to {r[2]}")

# line_movement (has current season from daily runs?)
r = conn.execute("""
    SELECT COUNT(*), MIN(game_date), MAX(game_date)
    FROM line_movement WHERE game_date >= '2025-11-01'
""").fetchone()
print(f"line_movement 2026:  {r[0]:,} rows | {r[1]} to {r[2]}")

conn.close()
