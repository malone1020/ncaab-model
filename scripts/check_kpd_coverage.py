import sqlite3, os
from collections import defaultdict
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

kpd_teams = set(r[0] for r in conn.execute(
    "SELECT DISTINCT team FROM kenpom_daily").fetchall())

games = conn.execute(
    "SELECT season, home_team, away_team FROM games").fetchall()

# Find teams in games that have NO KPD data at all
missing = set()
for season, home, away in games:
    if home not in kpd_teams: missing.add(home)
    if away not in kpd_teams: missing.add(away)

print(f"Teams in games table with no KPD data: {len(missing):,}")
print(f"\nSample missing teams:")
for t in sorted(missing)[:30]:
    print(f"  {t}")

# How many games involve at least one non-D1 team?
non_d1_games = sum(1 for s,h,a in games
                   if h not in kpd_teams or a not in kpd_teams)
d1_games = len(games) - non_d1_games
print(f"\nTotal games: {len(games):,}")
print(f"Games with both teams in KPD: {d1_games:,} ({d1_games/len(games)*100:.0f}%)")
print(f"Games with >=1 non-D1 team:   {non_d1_games:,} ({non_d1_games/len(games)*100:.0f}%)")
print(f"\nNote: non-D1 games have no spread/line and won't be bet on anyway.")
print(f"What matters is D1 vs D1 game coverage.")

# Check D1 only games — do they have good KPD coverage?
d1_only = [(s,h,a) for s,h,a in games
           if h in kpd_teams and a in kpd_teams]
print(f"\nAmong D1-only games: {len(d1_only):,}")
print(f"These should all have KPD data available.")
conn.close()
