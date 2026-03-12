"""Check if season tag on torvik_daily snapshots matches what lookup expects."""
import sqlite3, os
DB = os.path.join(os.getcwd(), 'data', 'basketball.db')
conn = sqlite3.connect(DB)

# Show what season the games table uses for November 2015 games
print("=== Games in Nov 2015 — what season? ===")
rows = conn.execute("""
    SELECT game_date, season, COUNT(*) as n 
    FROM games 
    WHERE game_date LIKE '2015-11%' 
    GROUP BY game_date, season 
    ORDER BY game_date LIMIT 10
""").fetchall()
for r in rows: print(f"  game_date={r[0]}  season={r[1]}  games={r[2]}")

print("\n=== Snapshots in torvik_daily for Nov 2015 dates — what season? ===")
rows = conn.execute("""
    SELECT snapshot_date, season, COUNT(DISTINCT team) as teams
    FROM torvik_daily 
    WHERE snapshot_date LIKE '201511%'
    GROUP BY snapshot_date, season 
    ORDER BY snapshot_date LIMIT 10
""").fetchall()
for r in rows: print(f"  snapshot={r[0]}  season={r[1]}  teams={r[2]}")

print("\n=== What season are Apr 2016 game snapshots stored under? ===")
rows = conn.execute("""
    SELECT snapshot_date, season, COUNT(DISTINCT team) as teams
    FROM torvik_daily 
    WHERE snapshot_date LIKE '201604%'
    GROUP BY snapshot_date, season 
    ORDER BY snapshot_date LIMIT 5
""").fetchall()
for r in rows: print(f"  snapshot={r[0]}  season={r[1]}  teams={r[2]}")

# The key question: for a game on 2016-01-15 (season=2016 in games table),
# what season is the snapshot_date=20160114 stored under?
print("\n=== Snapshot for 20160114 — season stored? ===")
rows = conn.execute("""
    SELECT snapshot_date, season, COUNT(DISTINCT team) as teams
    FROM torvik_daily WHERE snapshot_date='20160114'
""").fetchall()
for r in rows: print(f"  snapshot={r[0]}  season={r[1]}  teams={r[2]}")

conn.close()
