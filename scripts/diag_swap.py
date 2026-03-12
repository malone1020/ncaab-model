import sqlite3, pandas as pd, os, sys
sys.path.insert(0, '.')

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

# Look at spread>0 games: do the team names match between game_lines and games?
df = pd.read_sql("""
    SELECT gl.game_id, gl.game_date, 
           gl.home_team as gl_home, gl.away_team as gl_away,
           gl.spread,
           g.home_team as g_home, g.away_team as g_away,
           g.home_score, g.away_score,
           (g.home_score - g.away_score) AS actual_margin
    FROM game_lines gl JOIN games g ON gl.game_id = g.id
    WHERE gl.spread > 3
    AND g.home_score IS NOT NULL
    GROUP BY gl.game_id
    LIMIT 20
""", conn)
conn.close()

print("spread>0 games — comparing team names in game_lines vs games table:")
print(df[['game_date','gl_home','gl_away','spread','g_home','g_away','home_score','away_score','actual_margin']].to_string())
