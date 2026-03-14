import sqlite3, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

rows = conn.execute("""
    SELECT g.tournament, g.season_type,
           strftime('%m', g.game_date) as month,
           COUNT(*) as n_games,
           COUNT(gl.over_under) as with_lines
    FROM games g
    LEFT JOIN game_lines gl ON g.game_date=gl.game_date
        AND g.home_team=gl.home_team AND g.away_team=gl.away_team
    WHERE g.season >= 2019
    GROUP BY g.tournament, g.season_type, month
    ORDER BY month, g.tournament
""").fetchall()
for r in rows:
    print(r)

print()
print("Sample March game_lines rows:")
rows2 = conn.execute("""
    SELECT gl.game_date, gl.home_team, gl.away_team, gl.over_under
    FROM game_lines gl
    WHERE strftime('%m', gl.game_date) IN ('03','04')
    AND gl.season >= 2019
    LIMIT 10
""").fetchall()
for r in rows2:
    print(r)
conn.close()
