"""Check raw torvik_daily content for specific teams and seasons."""
import sqlite3, os
DB = os.path.join(os.getcwd(), 'data', 'basketball.db')
conn = sqlite3.connect(DB)

# Check Louisiana Monroe specifically
print("=== Louisiana Monroe in torvik_daily ===")
rows = conn.execute(
    "SELECT season, team, snapshot_date FROM torvik_daily WHERE team LIKE '%Louisiana%' ORDER BY season, snapshot_date LIMIT 20"
).fetchall()
for r in rows: print(r)

print("\n=== All distinct teams in torvik_daily for season=2016 (sample) ===")
teams = conn.execute(
    "SELECT DISTINCT team FROM torvik_daily WHERE season=2016 ORDER BY team"
).fetchall()
print(f"Total teams in torvik_daily season 2016: {len(teams)}")
print("First 20:", [r[0] for r in teams[:20]])
print("Last 20:", [r[0] for r in teams[-20:]])

print("\n=== Seasons present in torvik_daily ===")
seasons = conn.execute(
    "SELECT season, COUNT(DISTINCT team) as teams, COUNT(*) as rows FROM torvik_daily GROUP BY season ORDER BY season"
).fetchall()
for r in seasons: print(f"  season={r[0]}  teams={r[1]}  rows={r[2]}")

print("\n=== Sample snapshot_dates for season=2016 ===")
dates = conn.execute(
    "SELECT DISTINCT snapshot_date FROM torvik_daily WHERE season=2016 ORDER BY snapshot_date LIMIT 10"
).fetchall()
for r in dates: print(r[0])

conn.close()
