"""Quick audit of data coverage gaps across all sources."""
import sqlite3, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

print("=" * 55)
print("DATA COVERAGE AUDIT")
print("=" * 55)

total = conn.execute("SELECT COUNT(*) FROM games").fetchone()[0]
print(f"\nTotal games: {total:,}")

# game_lines coverage
r = conn.execute("""
    SELECT COUNT(*) as total,
           SUM(CASE WHEN spread IS NOT NULL THEN 1 ELSE 0 END) as has_spread,
           SUM(CASE WHEN over_under IS NOT NULL THEN 1 ELSE 0 END) as has_ou,
           SUM(CASE WHEN home_moneyline IS NOT NULL THEN 1 ELSE 0 END) as has_ml
    FROM game_lines
""").fetchone()
print(f"\ngame_lines: {r[0]:,} rows | spread={r[1]:,} | O/U={r[2]:,} | ML={r[3]:,}")

# KP fanmatch
r = conn.execute("SELECT COUNT(*) FROM kenpom_fanmatch").fetchone()[0]
print(f"kenpom_fanmatch: {r:,} rows ({r/total*100:.0f}% of games)")

# recency_eff — how many unique game-team pairs covered
r = conn.execute("SELECT COUNT(*) FROM recency_eff WHERE rew_adj_em IS NOT NULL").fetchone()[0]
print(f"recency_eff (with rew_adj_em): {r:,} rows ({r/(total*2)*100:.0f}% of team-game pairs)")

# recency trend coverage
r = conn.execute("SELECT COUNT(*) FROM recency_eff WHERE trend_adj_em IS NOT NULL").fetchone()[0]
print(f"recency_eff (with trend): {r:,} rows ({r/(total*2)*100:.0f}% of team-game pairs)")

# referee coverage
r = conn.execute("SELECT COUNT(*) FROM referee_game WHERE ref_1 IS NOT NULL").fetchone()[0]
print(f"referee_game (with refs): {r:,} rows ({r/total*100:.0f}% of games)")

# ESPN IDs
r = conn.execute("SELECT COUNT(*) FROM games WHERE espn_id IS NOT NULL AND espn_id != ''").fetchone()[0]
print(f"games with ESPN ID: {r:,} ({r/total*100:.0f}%)")

# line_movement
r = conn.execute("SELECT COUNT(*) FROM line_movement WHERE total_open IS NOT NULL").fetchone()[0]
print(f"line_movement (with total_open): {r:,} ({r/total*100:.0f}% of games)")

# Season breakdown of key gaps
print(f"\n{'─'*55}")
print("KP FANMATCH COVERAGE BY SEASON")
print(f"{'─'*55}")
rows = conn.execute("""
    SELECT g.season,
           COUNT(*) as games,
           COUNT(kf.game_date) as with_fanmatch
    FROM games g
    LEFT JOIN kenpom_fanmatch kf ON g.game_date=kf.game_date
        AND g.home_team=kf.home_team AND g.away_team=kf.away_team
    GROUP BY g.season ORDER BY g.season
""").fetchall()
for r in rows:
    pct = r[2]/r[1]*100 if r[1] > 0 else 0
    bar = '█' * int(pct/5)
    print(f"  {int(r[0])}: {r[2]:>5}/{r[1]:>5} ({pct:>4.0f}%) {bar}")

print(f"\n{'─'*55}")
print("REFEREE COVERAGE BY SEASON")
print(f"{'─'*55}")
rows = conn.execute("""
    SELECT g.season,
           COUNT(*) as games,
           COUNT(CASE WHEN rg.ref_1 IS NOT NULL THEN 1 END) as with_refs
    FROM games g
    LEFT JOIN referee_game rg ON g.game_date=rg.game_date
        AND g.home_team=rg.home_team AND g.away_team=rg.away_team
    GROUP BY g.season ORDER BY g.season
""").fetchall()
for r in rows:
    pct = r[2]/r[1]*100 if r[1] > 0 else 0
    bar = '█' * int(pct/5)
    print(f"  {int(r[0])}: {r[2]:>5}/{r[1]:>5} ({pct:>4.0f}%) {bar}")

conn.close()
