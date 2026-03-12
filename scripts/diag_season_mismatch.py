import sqlite3, os, pandas as pd
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

# What seasons does torvik_daily have?
print("torvik_daily seasons:")
for r in conn.execute("SELECT season, COUNT(DISTINCT snapshot_date) ndates, COUNT(DISTINCT team) nteams FROM torvik_daily GROUP BY season ORDER BY season"):
    print(f"  season={r[0]}  dates={r[1]}  teams={r[2]}")

print()
# What seasons does games have?
print("games seasons:")
for r in conn.execute("SELECT season, COUNT(*) n FROM games GROUP BY season ORDER BY season"):
    print(f"  season={r[0]}  games={r[1]}")

print()
# Critical: does snapshot 20151112 belong to season 2016?
# The backfill script used season_for_date() which returns year+1 for Nov/Dec
# Let's verify
sample = conn.execute("SELECT snapshot_date, season FROM torvik_daily WHERE snapshot_date='20151112' LIMIT 3").fetchall()
print(f"snapshot_date=20151112 has season: {[r[1] for r in sample]}")
sample2 = conn.execute("SELECT snapshot_date, season FROM torvik_daily WHERE snapshot_date='20160115' LIMIT 3").fetchall()
print(f"snapshot_date=20160115 has season: {[r[1] for r in sample2]}")

# Check what season a Jan 2016 game has in the games table
game_sample = conn.execute("SELECT game_date, season FROM games WHERE game_date LIKE '2016-01%' LIMIT 3").fetchall()
print(f"\nGames in Jan 2016 have season: {[(r[0], r[1]) for r in game_sample]}")

conn.close()
