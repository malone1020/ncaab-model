import sqlite3, os, sys
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

# All unique team names in rolling_efficiency
roll_teams = set(r[0] for r in conn.execute("SELECT DISTINCT team FROM rolling_efficiency").fetchall())
# All unique team names in games (both home and away)
game_teams = set(r[0] for r in conn.execute(
    "SELECT DISTINCT home_team FROM games UNION SELECT DISTINCT away_team FROM games"
).fetchall())

print(f"Rolling unique teams: {len(roll_teams)}")
print(f"Games unique teams:   {len(game_teams)}")

exact_match = roll_teams & game_teams
only_rolling = roll_teams - game_teams
only_games = game_teams - roll_teams

print(f"Exact matches:        {len(exact_match)}")
print(f"Only in rolling:      {len(only_rolling)}")
print(f"Only in games:        {len(only_games)}")

print(f"\nSample rolling teams NOT in games (first 30):")
for t in sorted(only_rolling)[:30]:
    print(f"  '{t}'")

print(f"\nSample games teams NOT in rolling (first 30):")
for t in sorted(only_games)[:30]:
    print(f"  '{t}'")

conn.close()
