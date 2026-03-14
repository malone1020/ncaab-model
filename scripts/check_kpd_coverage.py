import sqlite3, os, pandas as pd
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

# Load team-season coverage from kenpom_daily and torvik_daily
# Check which (season, team) combos have any data at all
kpd_teams = set(conn.execute(
    "SELECT DISTINCT season, team FROM kenpom_daily").fetchall())
tvd_teams = set(conn.execute(
    "SELECT DISTINCT season, team FROM torvik_daily").fetchall())

games = conn.execute(
    "SELECT season, home_team, away_team FROM games").fetchall()
conn.close()

print(f"KPD unique season-team pairs: {len(kpd_teams):,}")
print(f"TVD unique season-team pairs: {len(tvd_teams):,}")
print()

from collections import defaultdict
kpd_by_season = defaultdict(int)
tvd_by_season = defaultdict(int)
total_by_season = defaultdict(int)

for season, home, away in games:
    total_by_season[season] += 1
    if (season, home) in kpd_teams and (season, away) in kpd_teams:
        kpd_by_season[season] += 1
    if (season, home) in tvd_teams and (season, away) in tvd_teams:
        tvd_by_season[season] += 1

print(f"{'Season':>8} {'Games':>7} {'KPD%':>7} {'TVD%':>7}")
print("-" * 35)
for s in sorted(total_by_season):
    n = total_by_season[s]
    kp = kpd_by_season[s]/n*100
    tv = tvd_by_season[s]/n*100
    print(f"  {int(s):>6} {n:>7,} {kp:>6.0f}% {tv:>6.0f}%")
