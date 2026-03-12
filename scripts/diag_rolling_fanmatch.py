"""
diag_rolling_fanmatch.py
========================
Diagnoses two coverage issues:
  1. Rolling efficiency = 0 (likely date format mismatch)
  2. KP fanmatch low coverage (likely team name mismatch)
"""
import sqlite3, os, re
from collections import Counter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

print("=" * 60)
print("1. ROLLING EFFICIENCY — date format check")
print("=" * 60)

r_dates = conn.execute("SELECT DISTINCT game_date FROM rolling_efficiency LIMIT 5").fetchall()
print(f"  rolling_efficiency.game_date samples: {[x[0] for x in r_dates]}")

g_dates = conn.execute("SELECT DISTINCT game_date FROM games LIMIT 5").fetchall()
print(f"  games.game_date samples:              {[x[0] for x in g_dates]}")

r_teams = conn.execute("SELECT DISTINCT team FROM rolling_efficiency LIMIT 5").fetchall()
print(f"  rolling_efficiency.team samples:      {[x[0] for x in r_teams]}")

g_teams = conn.execute("SELECT DISTINCT home_team FROM games LIMIT 5").fetchall()
print(f"  games.home_team samples:              {[x[0] for x in g_teams]}")

# Try an exact join
hits = conn.execute("""
    SELECT COUNT(*) FROM rolling_efficiency r
    JOIN games g ON r.game_date = g.game_date AND r.team = g.home_team
""").fetchone()[0]
print(f"\n  Exact join (rolling x games home_team): {hits:,}")

# Try normalized join
hits2 = conn.execute("""
    SELECT COUNT(*) FROM rolling_efficiency r
    JOIN games g ON LOWER(TRIM(r.game_date)) = LOWER(TRIM(g.game_date))
               AND LOWER(TRIM(r.team)) = LOWER(TRIM(g.home_team))
""").fetchone()[0]
print(f"  Normalized join (lower/trim):           {hits2:,}")

# Count rolling rows vs unique (date, team) combos
total_r = conn.execute("SELECT COUNT(*) FROM rolling_efficiency").fetchone()[0]
print(f"\n  Total rolling_efficiency rows: {total_r:,}")

print("\n" + "=" * 60)
print("2. KP FANMATCH — team name mismatch check")
print("=" * 60)

fm_total = conn.execute("SELECT COUNT(*) FROM kenpom_fanmatch").fetchone()[0]
print(f"  Total fanmatch rows: {fm_total:,}")

# Direct join
direct = conn.execute("""
    SELECT COUNT(*) FROM kenpom_fanmatch f
    JOIN games g ON f.game_date = g.game_date
      AND f.home_team = g.home_team AND f.away_team = g.away_team
""").fetchone()[0]
print(f"  Direct join hits (exact): {direct:,}")

# Sample mismatched fanmatch teams
miss = conn.execute("""
    SELECT DISTINCT f.home_team
    FROM kenpom_fanmatch f
    WHERE NOT EXISTS (
        SELECT 1 FROM games g WHERE LOWER(g.home_team) = LOWER(f.home_team)
    )
    ORDER BY f.home_team
    LIMIT 30
""").fetchall()
print(f"\n  Fanmatch home_team values NOT found in games.home_team ({len(miss)} shown):")
for x in miss:
    print(f"    '{x[0]}'")

# Sample games teams not in fanmatch
miss_g = conn.execute("""
    SELECT DISTINCT g.home_team
    FROM games g
    WHERE NOT EXISTS (
        SELECT 1 FROM kenpom_fanmatch f WHERE LOWER(f.home_team) = LOWER(g.home_team)
    )
    ORDER BY g.home_team
    LIMIT 20
""").fetchall()
print(f"\n  games.home_team values NOT found in fanmatch ({len(miss_g)} shown):")
for x in miss_g:
    print(f"    '{x[0]}'")

conn.close()
print("\nDone.")
