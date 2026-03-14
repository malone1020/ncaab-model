import sqlite3, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

# Check what TVD columns exist in game_features_v2
cols = [c[1] for c in conn.execute("PRAGMA table_info(game_features_v2)").fetchall()]
tvd_cols = [c for c in cols if 'tvd' in c.lower()]
print(f"TVD columns in game_features_v2: {tvd_cols[:10]}")

# Check coverage of each TVD column
print("\nTVD column coverage (2024 season):")
for col in tvd_cols[:5]:
    n = conn.execute(f"SELECT COUNT(*) FROM game_features_v2 WHERE season=2024 AND {col} IS NOT NULL").fetchone()[0]
    total = conn.execute("SELECT COUNT(*) FROM game_features_v2 WHERE season=2024").fetchone()[0]
    print(f"  {col}: {n}/{total} ({n/total*100:.0f}%)")

# Check torvik_daily coverage for 2024
tvd_teams = conn.execute("SELECT COUNT(DISTINCT team) FROM torvik_daily WHERE season=2024").fetchone()[0]
print(f"\ntorvik_daily 2024: {tvd_teams} unique teams")

# Sample a game from 2024 and check if Torvik data exists for those teams
sample = conn.execute("""
    SELECT g.game_date, g.home_team, g.away_team
    FROM games g WHERE g.season=2024 LIMIT 3
""").fetchall()
print("\nSample 2024 games + Torvik lookup:")
for game_date, home, away in sample:
    snap = conn.execute("""
        SELECT COUNT(*) FROM torvik_daily
        WHERE team=? AND season=2024
        AND snapshot_date <= ?
    """, (home, game_date.replace('-',''))).fetchone()[0]
    print(f"  {game_date} {home}: {snap} snapshots before game")

conn.close()
