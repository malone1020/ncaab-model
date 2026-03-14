"""Audit 2026 season data completeness across all sources."""
import sqlite3, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

print("=" * 60)
print("2026 SEASON DATA AUDIT")
print("=" * 60)

# Games table
r = conn.execute("SELECT COUNT(*), MIN(game_date), MAX(game_date) FROM games WHERE season=2026").fetchone()
print(f"\ngames:              {r[0]:>6,} rows | {r[1]} to {r[2]}")

# game_lines
r = conn.execute("SELECT COUNT(*), MIN(game_date), MAX(game_date) FROM game_lines WHERE season=2026").fetchone()
r2 = conn.execute("SELECT COUNT(*) FROM game_lines WHERE season=2026 AND spread IS NOT NULL").fetchone()
r3 = conn.execute("SELECT COUNT(*) FROM game_lines WHERE season=2026 AND spread_open IS NOT NULL").fetchone()
print(f"game_lines:         {r[0]:>6,} rows | {r[1]} to {r[2]}")
print(f"  with spread:      {r2[0]:>6,}")
print(f"  with spread_open: {r3[0]:>6,}  (OddsAPI open/close)")

# torvik_daily
r = conn.execute("SELECT COUNT(*), MIN(snapshot_date), MAX(snapshot_date) FROM torvik_daily WHERE season=2026").fetchone()
# Convert snapshot_date format (YYYYMMDD) to readable
print(f"torvik_daily:       {r[0]:>6,} rows | {r[1]} to {r[2]}")

# torvik_game_preds
r = conn.execute("SELECT COUNT(*), MIN(game_date), MAX(game_date) FROM torvik_game_preds WHERE season=2026").fetchone()
print(f"torvik_game_preds:  {r[0]:>6,} rows | {r[1]} to {r[2]}")

# kenpom_daily
r = conn.execute("SELECT COUNT(*), MIN(snapshot_date), MAX(snapshot_date) FROM kenpom_daily WHERE season=2026").fetchone()
print(f"kenpom_daily:       {r[0]:>6,} rows | {r[1]} to {r[2]}")

# kenpom_fanmatch
r = conn.execute("SELECT COUNT(*), MIN(game_date), MAX(game_date) FROM kenpom_fanmatch WHERE season=2026").fetchone()
print(f"kenpom_fanmatch:    {r[0]:>6,} rows | {r[1]} to {r[2]}")

# recency_eff
r = conn.execute("SELECT COUNT(*), MIN(game_date), MAX(game_date) FROM recency_eff WHERE season=2026").fetchone()
print(f"recency_eff:        {r[0]:>6,} rows | {r[1]} to {r[2]}")

# game_features_v2
r = conn.execute("SELECT COUNT(*), MIN(game_date), MAX(game_date) FROM game_features_v2 WHERE season=2026").fetchone()
print(f"game_features_v2:   {r[0]:>6,} rows | {r[1]} to {r[2]}")

# referee_game
r = conn.execute("SELECT COUNT(*), MIN(game_date), MAX(game_date) FROM referee_game WHERE season=2026").fetchone()
print(f"referee_game:       {r[0]:>6,} rows | {r[1]} to {r[2]}")

# line_movement
r = conn.execute("SELECT COUNT(*), MIN(game_date), MAX(game_date) FROM line_movement WHERE game_date >= '2025-11-01'").fetchone()
print(f"line_movement 2026: {r[0]:>6,} rows | {r[1]} to {r[2]}")

# haslametrics - check if 2026 available
r = conn.execute("SELECT COUNT(*) FROM haslametrics WHERE season=2026").fetchone()
print(f"haslametrics:       {r[0]:>6,} rows (404 from source — not available yet)")

print("\n" + "=" * 60)
print("GAPS SUMMARY")
print("=" * 60)

# What's the latest game date we have?
latest_game = conn.execute("SELECT MAX(game_date) FROM games WHERE season=2026").fetchone()[0]
latest_tvd = conn.execute("SELECT MAX(snapshot_date) FROM torvik_daily WHERE season=2026").fetchone()[0]
latest_kpd = conn.execute("SELECT MAX(snapshot_date) FROM kenpom_daily WHERE season=2026").fetchone()[0]
latest_lines = conn.execute("SELECT MAX(game_date) FROM game_lines WHERE season=2026 AND spread IS NOT NULL").fetchone()[0]

print(f"  Latest game in games table: {latest_game}")
print(f"  Latest Torvik snapshot:     {latest_tvd}")
print(f"  Latest KenPom snapshot:     {latest_kpd}")
print(f"  Latest game_lines:          {latest_lines}")
print(f"  Today is 2026-03-14")
print()

if not latest_game:
    print("  ⚠ games table has NO 2026 data — run step 0 (02_pull_games.py)")
else:
    print(f"  ✓ games table populated through {latest_game}")

conn.close()
