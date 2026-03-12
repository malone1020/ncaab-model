import sqlite3, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

# Rolling teams on a specific date
r = conn.execute("SELECT game_date, team FROM rolling_efficiency WHERE game_date='2015-11-19' LIMIT 10").fetchall()
print('Rolling on 2015-11-19:')
for x in r: print(f'  {x}')

# Games on that same date
g = conn.execute("SELECT game_date, home_team, away_team FROM games WHERE game_date='2015-11-19' LIMIT 10").fetchall()
print('\nGames on 2015-11-19:')
for x in g: print(f'  {x}')

# How many rolling (date,team) pairs have an exact match in games as home OR away?
hits = conn.execute("""
    SELECT COUNT(*) FROM rolling_efficiency r
    WHERE EXISTS (
        SELECT 1 FROM games g
        WHERE g.game_date = r.game_date
          AND (g.home_team = r.team OR g.away_team = r.team)
    )
""").fetchone()[0]
total = conn.execute("SELECT COUNT(*) FROM rolling_efficiency").fetchone()[0]
print(f'\nRolling rows with exact team match in games: {hits:,} / {total:,}')

# Sample rolling teams that have NO match in games at all
no_match = conn.execute("""
    SELECT DISTINCT r.team FROM rolling_efficiency r
    WHERE NOT EXISTS (
        SELECT 1 FROM games g WHERE g.home_team = r.team OR g.away_team = r.team
    )
    LIMIT 20
""").fetchall()
print(f'\nRolling teams with no match anywhere in games:')
for x in no_match: print(f'  {x[0]}')

conn.close()
