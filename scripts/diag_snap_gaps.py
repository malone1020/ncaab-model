"""Find the actual date gaps in torvik_daily snapshots."""
import sqlite3, os
from datetime import datetime, timedelta
DB = os.path.join(os.getcwd(), 'data', 'basketball.db')
conn = sqlite3.connect(DB)

# Get ALL distinct snapshot dates
all_snaps = sorted(r[0] for r in conn.execute(
    "SELECT DISTINCT snapshot_date FROM torvik_daily"
).fetchall())

print(f"Total distinct snapshot dates: {len(all_snaps)}")
print(f"First: {all_snaps[0]}  Last: {all_snaps[-1]}")

# Find gaps > 1 day
gaps = []
for i in range(1, len(all_snaps)):
    d1 = datetime.strptime(all_snaps[i-1], '%Y%m%d').date()
    d2 = datetime.strptime(all_snaps[i],   '%Y%m%d').date()
    delta = (d2 - d1).days
    if delta > 3:
        gaps.append((all_snaps[i-1], all_snaps[i], delta))

print(f"\nGaps > 3 days between consecutive snapshots ({len(gaps)} found):")
for g in gaps:
    print(f"  {g[0]} → {g[1]}  ({g[2]} days missing)")

# Now check which game dates fall in those gaps
game_dates = [r[0] for r in conn.execute(
    "SELECT DISTINCT game_date FROM games WHERE home_score IS NOT NULL ORDER BY game_date"
).fetchall()]
snap_set = set(all_snaps)
stranded = []
for gd_str in game_dates:
    gd = datetime.strptime(gd_str[:10], '%Y-%m-%d').date()
    snap = (gd - timedelta(days=1)).strftime('%Y%m%d')
    if snap not in snap_set:
        stranded.append((gd_str, snap))

print(f"\nGame dates whose day-before snap is missing: {len(stranded)}")
for g in stranded[:20]:
    print(f"  game={g[0]}  needed={g[1]}")

conn.close()
