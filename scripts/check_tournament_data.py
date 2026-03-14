import sqlite3, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

# Check if tournament games have lines
print("Tournament games in games table (2019+):")
rows = conn.execute("""
    SELECT tournament, COUNT(*) as n_games
    FROM games WHERE season >= 2019 AND tournament IS NOT NULL
    GROUP BY tournament
""").fetchall()
for r in rows: print(f"  {r}")

print("\nTournament games with lines:")
rows = conn.execute("""
    SELECT g.tournament, COUNT(*) as n
    FROM games g
    JOIN game_lines gl ON g.game_date=gl.game_date
        AND g.home_team=gl.home_team AND g.away_team=gl.away_team
    WHERE g.season >= 2019 AND g.tournament IS NOT NULL
    GROUP BY g.tournament
""").fetchall()
for r in rows: print(f"  {r}")

print("\nSample NCAA tournament game:")
rows = conn.execute("""
    SELECT g.game_date, g.home_team, g.away_team, g.tournament,
           gl.over_under, gl.home_score, gl.away_score
    FROM games g
    JOIN game_lines gl ON g.game_date=gl.game_date
        AND g.home_team=gl.home_team AND g.away_team=gl.away_team
    WHERE g.tournament = 'ncaa_tournament'
    LIMIT 5
""").fetchall()
for r in rows: print(f"  {r}")

print("\nSample NCAA tournament game WITHOUT lines:")
rows = conn.execute("""
    SELECT g.game_date, g.home_team, g.away_team, g.tournament
    FROM games g
    LEFT JOIN game_lines gl ON g.game_date=gl.game_date
        AND g.home_team=gl.home_team AND g.away_team=gl.away_team
    WHERE g.tournament = 'ncaa_tournament'
    AND gl.game_date IS NULL
    LIMIT 5
""").fetchall()
for r in rows: print(f"  {r}")
conn.close()
