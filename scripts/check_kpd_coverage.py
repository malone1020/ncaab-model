import sqlite3, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

# Check schema first
kpd_cols = [c[1] for c in conn.execute("PRAGMA table_info(kenpom_daily)").fetchall()]
tvd_cols  = [c[1] for c in conn.execute("PRAGMA table_info(torvik_daily)").fetchall()]
print(f"kenpom_daily cols: {kpd_cols[:8]}")
print(f"torvik_daily cols: {tvd_cols[:8]}")
print()

# KPD date column
kpd_date = 'date' if 'date' in kpd_cols else ('snapshot_date' if 'snapshot_date' in kpd_cols else kpd_cols[0])
print(f"Using KPD date col: {kpd_date}")

rows = conn.execute(f"""
    SELECT g.season, COUNT(*) as games,
           SUM(CASE WHEN h.team IS NOT NULL AND a.team IS NOT NULL THEN 1 ELSE 0 END) as both_kpd
    FROM games g
    LEFT JOIN kenpom_daily h ON h.team = g.home_team
        AND h.{kpd_date} < g.game_date AND h.season = g.season
    LEFT JOIN kenpom_daily a ON a.team = g.away_team
        AND a.{kpd_date} < g.game_date AND a.season = g.season
    GROUP BY g.season ORDER BY g.season
""").fetchall()

print(f"\nKPD coverage:")
print(f"{'Season':>8} {'Games':>7} {'Both KPD':>10} {'Pct':>6}")
print("-" * 38)
for r in rows:
    pct = r[2]/r[1]*100 if r[1] > 0 else 0
    print(f"  {int(r[0]):>6} {r[1]:>7,} {r[2]:>10,} {pct:>5.0f}%")

# TVD
rows2 = conn.execute("""
    SELECT g.season, COUNT(*) as games,
           SUM(CASE WHEN h.team IS NOT NULL AND a.team IS NOT NULL THEN 1 ELSE 0 END) as both_tvd
    FROM games g
    LEFT JOIN torvik_daily h ON h.team = g.home_team
        AND h.snapshot_date < replace(g.game_date,'-','') AND h.season = g.season
    LEFT JOIN torvik_daily a ON a.team = g.away_team
        AND a.snapshot_date < replace(g.game_date,'-','') AND a.season = g.season
    GROUP BY g.season ORDER BY g.season
""").fetchall()

print(f"\nTVD coverage:")
print(f"{'Season':>8} {'Games':>7} {'Both TVD':>10} {'Pct':>6}")
print("-" * 38)
for r in rows2:
    pct = r[2]/r[1]*100 if r[1] > 0 else 0
    print(f"  {int(r[0]):>6} {r[1]:>7,} {r[2]:>10,} {pct:>5.0f}%")

conn.close()
