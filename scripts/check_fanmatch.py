import sqlite3, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

print("kenpom_fanmatch schema:")
print(conn.execute("PRAGMA table_info(kenpom_fanmatch)").fetchall())
print()
print("Sample rows:")
rows = conn.execute("SELECT * FROM kenpom_fanmatch LIMIT 3").fetchall()
for r in rows: print(r)
print()
print("Date range:")
print(conn.execute("SELECT MIN(game_date), MAX(game_date), COUNT(*) FROM kenpom_fanmatch").fetchone())
print()
print("Games vs fanmatch match by season (checking join key):")
rows = conn.execute("""
    SELECT g.season, COUNT(DISTINCT g.game_date||g.home_team||g.away_team) as games,
           COUNT(DISTINCT kf.game_date||kf.home_team||kf.away_team) as fanmatch
    FROM games g
    LEFT JOIN kenpom_fanmatch kf ON g.game_date=kf.game_date
        AND g.home_team=kf.home_team AND g.away_team=kf.away_team
    GROUP BY g.season ORDER BY g.season
""").fetchall()
for r in rows:
    print(f"  {int(r[0])}: games={r[1]}, fanmatch={r[2]} ({r[2]/r[1]*100:.0f}%)")
conn.close()
