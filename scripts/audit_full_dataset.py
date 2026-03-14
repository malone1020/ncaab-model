"""Full dataset completeness audit across all seasons."""
import sqlite3, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

print("=" * 70)
print("FULL DATASET AUDIT — All Seasons")
print("=" * 70)

def pct(n, d): return f"{n/d*100:.0f}%" if d > 0 else "N/A"

games = {r[0]: r[1] for r in conn.execute(
    "SELECT season, COUNT(*) FROM games GROUP BY season").fetchall()}
total_games = sum(games.values())
print(f"\nTotal games: {total_games:,} across {len(games)} seasons")

print(f"\n{'Season':>8} {'Games':>7} {'KPD%':>6} {'TVD%':>6} {'Refs%':>6} {'Lines%':>7} {'Features%':>10}")
print("-" * 60)

for season in sorted(games.keys()):
    n = games[season]

    kpd = conn.execute("""
        SELECT COUNT(*) FROM game_features_v2
        WHERE season=? AND h_kpd_adj_o IS NOT NULL""", (season,)).fetchone()[0]

    tvd = conn.execute("""
        SELECT COUNT(*) FROM game_features_v2
        WHERE season=? AND h_tvd_barthag IS NOT NULL""", (season,)).fetchone()[0]

    refs = conn.execute("""
        SELECT COUNT(*) FROM referee_game rg
        JOIN games g ON rg.game_date=g.game_date
            AND rg.home_team=g.home_team AND rg.away_team=g.away_team
        WHERE g.season=?""", (season,)).fetchone()[0]

    lines = conn.execute("""
        SELECT COUNT(DISTINCT game_date||home_team) FROM game_lines
        WHERE season=? AND spread IS NOT NULL""", (season,)).fetchone()[0]

    features = conn.execute("""
        SELECT COUNT(*) FROM game_features_v2 WHERE season=?""", (season,)).fetchone()[0]

    print(f"  {int(season):>6} {n:>7,} {pct(kpd,n):>6} {pct(tvd,n):>6} "
          f"{pct(refs,n):>6} {pct(lines,n):>7} {pct(features,n):>10}")

# Summary gaps
lm = conn.execute("SELECT COUNT(*) FROM line_movement WHERE spread_open IS NOT NULL").fetchone()[0]
ref_total = conn.execute("SELECT COUNT(*) FROM referee_game").fetchone()[0]
lm_2026 = conn.execute("SELECT COUNT(*) FROM line_movement WHERE game_date >= '2025-11-01' AND spread_open IS NOT NULL").fetchone()[0]

print(f"\n{'─'*60}")
print(f"  line_movement (all, with open): {lm:,} ({pct(lm,total_games)} of games)")
print(f"  line_movement (2026 season):    {lm_2026:,}")
print(f"  referee_game assignments:       {ref_total:,} ({pct(ref_total,total_games)} of games)")
print(f"\n  Structural gaps (not fixable):")
print(f"  → Refs: ESPN only covers ~37% of games")
print(f"  → Line movement: OddsAPI historical limit; grows daily")
print(f"  → Haslametrics 2026: XML not published mid-season")
conn.close()
