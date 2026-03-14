"""Audit 2026 season data completeness across all sources."""
import sqlite3, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

print("=" * 60)
print("2026 SEASON DATA AUDIT")
print("=" * 60)

def q(sql, params=()):
    try:
        return conn.execute(sql, params).fetchone()
    except:
        return (0, None, None)

# Games table
r = q("SELECT COUNT(*), MIN(game_date), MAX(game_date) FROM games WHERE season=2026")
print(f"\ngames:              {r[0]:>6,} rows | {r[1]} to {r[2]}")

# game_lines
r  = q("SELECT COUNT(*), MIN(game_date), MAX(game_date) FROM game_lines WHERE season=2026")
r2 = q("SELECT COUNT(*) FROM game_lines WHERE season=2026 AND spread IS NOT NULL")
r3 = q("SELECT COUNT(*) FROM game_lines WHERE season=2026 AND spread_open IS NOT NULL")
print(f"game_lines:         {r[0]:>6,} rows | {r[1]} to {r[2]}")
print(f"  with spread:      {r2[0]:>6,}")
print(f"  with spread_open: {r3[0]:>6,}  (OddsAPI open/close)")

# torvik_daily
r = q("SELECT COUNT(*), MIN(snapshot_date), MAX(snapshot_date) FROM torvik_daily WHERE season=2026")
print(f"torvik_daily:       {r[0]:>6,} rows | {r[1]} to {r[2]}")

# torvik_game_preds
r = q("SELECT COUNT(*), MIN(game_date), MAX(game_date) FROM torvik_game_preds WHERE season=2026")
print(f"torvik_game_preds:  {r[0]:>6,} rows | {r[1]} to {r[2]}")

# kenpom_daily
r = q("SELECT COUNT(*), MIN(snapshot_date), MAX(snapshot_date) FROM kenpom_daily WHERE season=2026")
print(f"kenpom_daily:       {r[0]:>6,} rows | {r[1]} to {r[2]}")

# kenpom_fanmatch
r = q("SELECT COUNT(*), MIN(game_date), MAX(game_date) FROM kenpom_fanmatch WHERE season=2026")
print(f"kenpom_fanmatch:    {r[0]:>6,} rows | {r[1]} to {r[2]}")

# recency_eff — no season column, use date range
r = q("SELECT COUNT(*), MIN(game_date), MAX(game_date) FROM recency_eff WHERE game_date >= '2025-11-01'")
print(f"recency_eff:        {r[0]:>6,} rows | {r[1]} to {r[2]}")

# game_features_v2
r = q("SELECT COUNT(*), MIN(game_date), MAX(game_date) FROM game_features_v2 WHERE season=2026")
print(f"game_features_v2:   {r[0]:>6,} rows | {r[1]} to {r[2]}")

# referee_game
r = q("SELECT COUNT(*), MIN(game_date), MAX(game_date) FROM referee_game WHERE game_date >= '2025-11-01'")
print(f"referee_game:       {r[0]:>6,} rows | {r[1]} to {r[2]}")

# line_movement
r = q("SELECT COUNT(*), MIN(game_date), MAX(game_date) FROM line_movement WHERE game_date >= '2025-11-01'")
print(f"line_movement:      {r[0]:>6,} rows | {r[1]} to {r[2]}")

# haslametrics
r = q("SELECT COUNT(*) FROM haslametrics WHERE season=2026")
print(f"haslametrics:       {r[0]:>6,} rows (404 from source — not available mid-season)")

print("\n" + "=" * 60)
print("GAPS REQUIRING ACTION")
print("=" * 60)

latest_game_lines = q("SELECT MAX(game_date) FROM game_lines WHERE season=2026 AND spread IS NOT NULL")[0]
latest_tvd = q("SELECT MAX(snapshot_date) FROM torvik_daily WHERE season=2026")[0]
latest_kpd = q("SELECT COUNT(*) FROM kenpom_daily WHERE season=2026")[0]
games_count = q("SELECT COUNT(*) FROM games WHERE season=2026")[0]

if games_count == 0:
    print("  ⚠ CRITICAL: games table empty for 2026 — run: python scripts/00_full_rebuild.py --only 0")
else:
    print(f"  ✓ games: {games_count:,} rows")

if latest_kpd == 0:
    print("  ⚠ CRITICAL: kenpom_daily empty for 2026 — run: python scripts/00_full_rebuild.py --only 3")
else:
    print(f"  ✓ kenpom_daily: {latest_kpd:,} rows")

open_lines = q("SELECT COUNT(*) FROM game_lines WHERE season=2026 AND spread_open IS NOT NULL")[0]
total_lines = q("SELECT COUNT(*) FROM game_lines WHERE season=2026 AND spread IS NOT NULL")[0]
if open_lines < total_lines * 0.5:
    missing = total_lines - open_lines
    print(f"  ⚠ game_lines: {missing:,} games missing OddsAPI open lines — run: --only 12")
else:
    print(f"  ✓ game_lines: {open_lines:,} with open lines")

latest_lines_date = q("SELECT MAX(game_date) FROM game_lines WHERE season=2026")[0]
if latest_lines_date and latest_lines_date < '2026-03-13':
    print(f"  ⚠ game_lines only through {latest_lines_date} — missing recent games")
else:
    print(f"  ✓ game_lines through {latest_lines_date}")

conn.close()
