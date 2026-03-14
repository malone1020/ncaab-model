import sqlite3, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

print("KPD coverage — games with both home+away KPD snapshot available:")
rows = conn.execute("""
    SELECT g.season,
           COUNT(*) as games,
           SUM(CASE WHEN h.team IS NOT NULL AND a.team IS NOT NULL THEN 1 ELSE 0 END) as both_kpd,
           SUM(CASE WHEN h.team IS NOT NULL THEN 1 ELSE 0 END) as home_kpd,
           SUM(CASE WHEN a.team IS NOT NULL THEN 1 ELSE 0 END) as away_kpd
    FROM games g
    LEFT JOIN kenpom_daily h ON h.team = g.home_team
        AND h.date < g.game_date
        AND h.season = g.season
    LEFT JOIN kenpom_daily a ON a.team = g.away_team
        AND a.date < g.game_date
        AND a.season = g.season
    GROUP BY g.season ORDER BY g.season
""").fetchall()

print(f"{'Season':>8} {'Games':>7} {'Both KPD':>10} {'Pct':>6}")
print("-" * 40)
for r in rows:
    pct = r[2]/r[1]*100 if r[1] > 0 else 0
    print(f"  {int(r[0]):>6} {r[1]:>7,} {r[2]:>10,} {pct:>5.0f}%")

print()
print("TVD coverage:")
rows2 = conn.execute("""
    SELECT g.season,
           COUNT(*) as games,
           SUM(CASE WHEN h.team IS NOT NULL AND a.team IS NOT NULL THEN 1 ELSE 0 END) as both_tvd
    FROM games g
    LEFT JOIN torvik_daily h ON h.team = g.home_team
        AND h.snapshot_date < replace(g.game_date, '-', '')
        AND h.season = g.season
    LEFT JOIN torvik_daily a ON a.team = g.away_team
        AND a.snapshot_date < replace(g.game_date, '-', '')
        AND a.season = g.season
    GROUP BY g.season ORDER BY g.season
""").fetchall()

print(f"{'Season':>8} {'Games':>7} {'Both TVD':>10} {'Pct':>6}")
print("-" * 40)
for r in rows2:
    pct = r[2]/r[1]*100 if r[1] > 0 else 0
    print(f"  {int(r[0]):>6} {r[1]:>7,} {r[2]:>10,} {pct:>5.0f}%")

conn.close()
