"""Full dataset completeness audit across all seasons."""
import sqlite3, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

print("=" * 70)
print("FULL DATASET AUDIT — All Seasons")
print("=" * 70)

def pct(n, d): return f"{n/d*100:.0f}%" if d > 0 else "N/A"

# Games per season
games = {r[0]: r[1] for r in conn.execute(
    "SELECT season, COUNT(*) FROM games GROUP BY season").fetchall()}
total_games = sum(games.values())
print(f"\nTotal games: {total_games:,} across {len(games)} seasons")

# Feature coverage per season for key sources
print(f"\n{'Season':>8} {'Games':>7} {'KPD%':>6} {'TVD%':>6} {'Refs%':>6} {'Lines%':>7} {'Recency%':>9} {'Features%':>10}")
print("-" * 75)

for season in sorted(games.keys()):
    n = games[season]
    
    kpd = conn.execute("""
        SELECT COUNT(DISTINCT gf.game_date||gf.home_team) FROM game_features_v2 gf
        WHERE gf.season=? AND gf.h_kpd_adj_o IS NOT NULL""", (season,)).fetchone()[0]
    
    tvd = conn.execute("""
        SELECT COUNT(DISTINCT gf.game_date||gf.home_team) FROM game_features_v2 gf
        WHERE gf.season=? AND gf.h_tvd_adj_o IS NOT NULL""", (season,)).fetchone()[0]
    
    refs = conn.execute("""
        SELECT COUNT(*) FROM referee_game rg
        JOIN games g ON rg.game_date=g.game_date 
            AND rg.home_team=g.home_team AND rg.away_team=g.away_team
        WHERE g.season=?""", (season,)).fetchone()[0]
    
    lines = conn.execute("""
        SELECT COUNT(DISTINCT game_date||home_team) FROM game_lines
        WHERE season=? AND spread IS NOT NULL""", (season,)).fetchone()[0]
    
    recency = conn.execute("""
        SELECT COUNT(DISTINCT game_date||home_team) FROM recency_eff re
        JOIN games g ON re.game_date=g.game_date AND re.home_team=g.home_team
        WHERE g.season=?""", (season,)).fetchone()[0]
    
    features = conn.execute("""
        SELECT COUNT(*) FROM game_features_v2 WHERE season=?""", (season,)).fetchone()[0]
    
    print(f"  {int(season):>6} {n:>7,} {pct(kpd,n):>6} {pct(tvd,n):>6} "
          f"{pct(refs,n):>6} {pct(lines,n):>7} {pct(recency,n):>9} {pct(features,n):>10}")

print(f"\n{'─'*70}")
print("GAPS WORTH ADDRESSING:")

# Check line movement coverage
lm = conn.execute("SELECT COUNT(*) FROM line_movement WHERE spread_open IS NOT NULL").fetchone()[0]
print(f"\n  line_movement open lines: {lm:,} ({pct(lm, total_games)} of all games)")
print(f"  → OddsAPI historical limit; grows daily going forward")

# Check refs coverage
ref_total = conn.execute("SELECT COUNT(DISTINCT game_date||home_team) FROM referee_game").fetchone()[0]
print(f"\n  referee_game coverage: {ref_total:,} ({pct(ref_total, total_games)} of all games)")
print(f"  → ESPN only covers ~37% of games; grows daily going forward")

# Check 2026 specifically  
print(f"\n  2026 season gaps:")
for col, label in [('h_kpd_adj_o','KenPom'), ('h_tvd_adj_o','Torvik'), 
                   ('h_recency_rew_adj_em','Recency')]:
    try:
        n26 = conn.execute(f"""
            SELECT COUNT(*) FROM game_features_v2 
            WHERE season=2026 AND {col} IS NOT NULL""").fetchone()[0]
        t26 = games.get(2026, 0)
        print(f"    {label}: {n26:,}/{t26:,} ({pct(n26,t26)})")
    except: pass

conn.close()
