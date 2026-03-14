import sqlite3, os
from collections import defaultdict
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

cols = [c[1] for c in conn.execute("PRAGMA table_info(team_travel)").fetchall()]
print(f"team_travel cols: {cols}")
print()

total_games = conn.execute("SELECT COUNT(*) FROM games WHERE home_score IS NOT NULL").fetchone()[0]
travel_rows = conn.execute("SELECT COUNT(*) FROM team_travel").fetchone()[0]
print(f"Total games: {total_games:,}")
print(f"team_travel rows: {travel_rows:,}")
print()

# Coverage by season
rows = conn.execute("""
    SELECT g.season, COUNT(*) as games,
           COUNT(tt.away_travel_miles) as with_travel
    FROM games g
    LEFT JOIN team_travel tt ON g.game_date=tt.game_date
        AND g.home_team=tt.home_team AND g.away_team=tt.away_team
    WHERE g.home_score IS NOT NULL
    GROUP BY g.season ORDER BY g.season
""").fetchall()
print(f"{'Season':>8} {'Games':>7} {'Travel':>8} {'Pct':>6}")
print("-" * 35)
for r in rows:
    pct = r[2]/r[1]*100 if r[1] > 0 else 0
    print(f"  {int(r[0]):>6} {r[1]:>7,} {r[2]:>8,} {pct:>5.0f}%")
conn.close()
