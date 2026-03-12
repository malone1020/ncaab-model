"""Find what fraction of game dates have a snapshot the day before."""
import sqlite3, os
from datetime import datetime, timedelta
DB = os.path.join(os.getcwd(), 'data', 'basketball.db')
conn = sqlite3.connect(DB)

# All unique game dates
game_dates = [r[0] for r in conn.execute(
    "SELECT DISTINCT game_date FROM games WHERE home_score IS NOT NULL ORDER BY game_date"
).fetchall()]

# All snapshot dates we have
existing_snaps = set(r[0] for r in conn.execute(
    "SELECT DISTINCT snapshot_date FROM torvik_daily"
).fetchall())

print(f"Total unique game dates: {len(game_dates)}")
print(f"Existing snapshot dates: {len(existing_snaps)}")

missing = []
for gd_str in game_dates:
    gd = datetime.strptime(gd_str[:10], '%Y-%m-%d').date()
    snap = (gd - timedelta(days=1)).strftime('%Y%m%d')
    if snap not in existing_snaps:
        missing.append((gd_str, snap))

print(f"Game dates with NO snapshot the day before: {len(missing)} / {len(game_dates)}")
print("\nFirst 20 missing:")
for gd, snap in missing[:20]:
    print(f"  game={gd}  needed_snap={snap}")
