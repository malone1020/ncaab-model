"""
diag_db_audit.py — Full database coverage audit
Checks every table for season coverage, row counts, and key gaps.
Run from project root: python scripts/diag_db_audit.py
"""
import sqlite3, os, pandas as pd, numpy as np

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

SEP = "=" * 65

def section(title):
    print(f"\n{SEP}\n{title}\n{SEP}")

# ── 1. All tables ─────────────────────────────────────────────────────────────
section("ALL TABLES IN DATABASE")
tables = [r[0] for r in conn.execute(
    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
for t in tables:
    n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({t})")]
    print(f"  {t:<35} {n:>8,} rows   {len(cols)} cols")

# ── 2. games table ────────────────────────────────────────────────────────────
section("GAMES TABLE — season coverage")
df = pd.read_sql("SELECT season, COUNT(*) n, SUM(CASE WHEN home_score IS NOT NULL THEN 1 ELSE 0 END) scored FROM games GROUP BY season ORDER BY season", conn)
print(df.to_string(index=False))
print(f"\n  Total: {df['n'].sum():,} games, {df['scored'].sum():,} with scores")

# ── 3. game_lines ─────────────────────────────────────────────────────────────
section("GAME_LINES — season coverage & spread availability")
gl = pd.read_sql("""
    SELECT season,
           COUNT(DISTINCT game_date||home_team||away_team) n_games,
           SUM(CASE WHEN spread IS NOT NULL THEN 1 ELSE 0 END) has_spread,
           SUM(CASE WHEN over_under IS NOT NULL THEN 1 ELSE 0 END) has_ou,
           SUM(CASE WHEN home_moneyline IS NOT NULL THEN 1 ELSE 0 END) has_ml
    FROM game_lines
    GROUP BY season ORDER BY season
""", conn)
print(gl.to_string(index=False))

# ── 4. games vs game_lines match rate ─────────────────────────────────────────
section("GAME_LINES MATCH RATE vs GAMES (by date+team)")
gf = pd.read_sql("SELECT * FROM game_features_v2 WHERE season >= 2016", conn)
total = len(gf)
has_spread = gf['spread'].notna().sum()
print(f"  game_features_v2 total rows:     {total:,}")
print(f"  Rows with spread matched:        {has_spread:,} ({has_spread/total:.1%})")
print(f"  Rows WITHOUT spread:             {total-has_spread:,} ({(total-has_spread)/total:.1%})")

print("\n  By season:")
for s, g in gf.groupby('season'):
    n = len(g)
    hs = g['spread'].notna().sum()
    print(f"    {s}: {hs:>5}/{n:>5} matched ({hs/n:.0%})")

# ── 5. Unmatched games — sample to find name mismatches ───────────────────────
section("UNMATCHED GAMES — sample (games with no spread data)")
no_spread = gf[gf['spread'].isna()][['season','game_date','home_team','away_team']].copy()
no_spread['game_date'] = no_spread['game_date'].astype(str).str[:10]

# Find games_lines entries for same dates that didn't match
gl_raw = pd.read_sql("""
    SELECT DISTINCT game_date, home_team, away_team
    FROM game_lines WHERE spread IS NOT NULL
""", conn)
gl_raw['game_date'] = gl_raw['game_date'].astype(str).str[:10]

# Find cases where same date exists in game_lines but team name differs
sample_dates = no_spread['game_date'].unique()[:50]
gl_sample = gl_raw[gl_raw['game_date'].isin(sample_dates)]
ns_sample  = no_spread[no_spread['game_date'].isin(sample_dates)]

print("  Games table (no spread) vs game_lines on same date — name mismatches:")
mismatches = []
for _, row in ns_sample.iterrows():
    same_day = gl_sample[gl_sample['game_date'] == row['game_date']]
    for _, lr in same_day.iterrows():
        ht_match = row['home_team'].lower() in lr['home_team'].lower() or lr['home_team'].lower() in row['home_team'].lower()
        at_match = row['away_team'].lower() in lr['away_team'].lower() or lr['away_team'].lower() in row['away_team'].lower()
        if (ht_match or at_match) and not (ht_match and at_match):
            mismatches.append({
                'date': row['game_date'],
                'games_home': row['home_team'], 'games_away': row['away_team'],
                'lines_home': lr['home_team'],  'lines_away': lr['away_team'],
            })
if mismatches:
    mm = pd.DataFrame(mismatches[:20])
    print(mm.to_string(index=False))
else:
    print("  No obvious partial matches found in sample — gaps may be genuinely unlined games")

# ── 6. torvik_season ──────────────────────────────────────────────────────────
section("TORVIK_SEASON — coverage by season")
ts = pd.read_sql("SELECT season, COUNT(DISTINCT team) n_teams FROM torvik_season GROUP BY season ORDER BY season", conn)
print(ts.to_string(index=False))
print(f"\n  In game_features_v2:")
for s, g in gf.groupby('season'):
    n = len(g)
    ht = g['h_tvs_adj_em'].notna().sum()
    print(f"    {s}: {ht:>5}/{n:>5} home teams matched ({ht/n:.0%})")

# ── 7. torvik_daily ───────────────────────────────────────────────────────────
section("TORVIK_DAILY — coverage by season")
td = pd.read_sql("SELECT season, COUNT(DISTINCT snapshot_date||team) n FROM torvik_daily GROUP BY season ORDER BY season", conn)
print(td.to_string(index=False))
print(f"\n  In game_features_v2 (h_tvd_adj_em):")
for s, g in gf.groupby('season'):
    n = len(g)
    ht = g['h_tvd_adj_em'].notna().sum()
    print(f"    {s}: {ht:>5}/{n:>5} ({ht/n:.0%})")

# ── 8. torvik_game_preds ──────────────────────────────────────────────────────
section("TORVIK_GAME_PREDS — coverage by season")
try:
    tp = pd.read_sql("""
        SELECT p.season, COUNT(*) n_preds,
               COUNT(DISTINCT p.game_date||p.home_team||p.away_team) n_games
        FROM torvik_game_preds p
        GROUP BY p.season ORDER BY p.season
    """, conn)
    print(tp.to_string(index=False))
    cols = [r[1] for r in conn.execute("PRAGMA table_info(torvik_game_preds)")]
    print(f"\n  Columns: {cols}")
except Exception as e:
    print(f"  ERROR: {e}")

# ── 9. kenpom_ratings ─────────────────────────────────────────────────────────
section("KENPOM_RATINGS — coverage by season")
kp = pd.read_sql("SELECT season, COUNT(DISTINCT team) n_teams FROM kenpom_ratings GROUP BY season ORDER BY season", conn)
print(kp.to_string(index=False))
print(f"\n  In game_features_v2 (h_kp_adj_em):")
for s, g in gf.groupby('season'):
    n = len(g)
    ht = g['h_kp_adj_em'].notna().sum()
    print(f"    {s}: {ht:>5}/{n:>5} ({ht/n:.0%})")

# ── 10. haslametrics ──────────────────────────────────────────────────────────
section("HASLAMETRICS — coverage by season")
try:
    hm = pd.read_sql("""
        SELECT season, variant, COUNT(DISTINCT team) n_teams
        FROM haslametrics_full
        GROUP BY season, variant ORDER BY season, variant
    """, conn)
    print(hm.to_string(index=False))
    print(f"\n  In game_features_v2 (h_ha_o_eff):")
    for s, g in gf.groupby('season'):
        n = len(g)
        ht = g['h_ha_o_eff'].notna().sum()
        print(f"    {s}: {ht:>5}/{n:>5} ({ht/n:.0%})")
    # Check what sections/metrics are stored
    cols_hm = [r[1] for r in conn.execute("PRAGMA table_info(haslametrics_full)")]
    print(f"\n  haslametrics_full columns: {cols_hm}")
except Exception as e:
    print(f"  ERROR: {e}")

# ── 11. box scores / game_team_stats ──────────────────────────────────────────
section("BOX SCORES (game_team_stats) — coverage by season")
try:
    bs = pd.read_sql("""
        SELECT g.season, COUNT(DISTINCT s.game_id) n_games
        FROM game_team_stats s JOIN games g ON s.game_id = g.id
        GROUP BY g.season ORDER BY g.season
    """, conn)
    print(bs.to_string(index=False))
except Exception as e:
    print(f"  ERROR: {e}")

# ── 12. Summary of gaps ───────────────────────────────────────────────────────
section("SUMMARY — % of games with each data source (betting universe 0.5-9pt spread)")
bet = gf[gf['spread'].notna() & gf['spread'].abs().between(0.5, 9.0)]
print(f"  Betting universe (0.5-9pt spread): {len(bet):,} games\n")
checks = [
    ('spread',         'Spread line'),
    ('h_tvs_adj_em',   'Torvik season (home)'),
    ('h_tvd_adj_em',   'Torvik daily (home)'),
    ('h_kp_adj_em',    'KenPom (home)'),
    ('h_ha_o_eff',     'Haslametrics efficiency (home)'),
    ('h_ha_o_3par',    'Haslametrics shot quality (home)'),
    ('torvik_pred',    'Torvik game prediction'),
    ('over_under',     'Over/under line'),
]
for col, label in checks:
    if col in bet.columns:
        n = bet[col].notna().sum()
        print(f"  {label:<35} {n:>6,} / {len(bet):>6,}  ({n/len(bet):.0%})")
    else:
        print(f"  {label:<35} column missing")

conn.close()
