"""
diag_daily.py — Run this to find out why torvik_daily isn't joining.
Run from scripts/ folder: python diag_daily.py
"""
import sqlite3, os, pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

print("=" * 60)
print("TORVIK DAILY DIAGNOSTIC")
print("=" * 60)

# 1. What does torvik_daily actually look like?
td = pd.read_sql("SELECT * FROM torvik_daily LIMIT 5", conn)
print("\n[1] torvik_daily sample rows:")
print(td[['season','snapshot_date','team','adj_em']].to_string())
print(f"\n    snapshot_date dtype: {td['snapshot_date'].dtype}")
print(f"    season dtype:        {td['season'].dtype}")
print(f"    season values:       {sorted(td['season'].unique())}")

# 2. What does games look like?
gm = pd.read_sql("SELECT season, game_date, home_team FROM games LIMIT 5", conn)
print("\n[2] games sample rows:")
print(gm.to_string())
print(f"\n    game_date dtype: {gm['game_date'].dtype}")
print(f"    season dtype:    {gm['season'].dtype}")

# 3. Pick a specific team from torvik_daily and check if it appears in games
sample_team_td = conn.execute("SELECT DISTINCT team FROM torvik_daily LIMIT 1").fetchone()[0]
print(f"\n[3] Sample team from torvik_daily: {sample_team_td!r}")

# Check if that team name appears in games
matches_home = conn.execute(
    "SELECT COUNT(*) FROM games WHERE home_team=?", (sample_team_td,)
).fetchone()[0]
matches_away = conn.execute(
    "SELECT COUNT(*) FROM games WHERE away_team=?", (sample_team_td,)
).fetchone()[0]
print(f"    In games.home_team: {matches_home} rows")
print(f"    In games.away_team: {matches_away} rows")

# 4. Check season overlap
td_seasons = set(r[0] for r in conn.execute("SELECT DISTINCT season FROM torvik_daily"))
gm_seasons = set(r[0] for r in conn.execute("SELECT DISTINCT season FROM games"))
print(f"\n[4] torvik_daily seasons: {sorted(td_seasons)}")
print(f"    games seasons:        {sorted(gm_seasons)}")
print(f"    Overlap:              {sorted(td_seasons & gm_seasons)}")

# 5. Try the actual mask logic for one known team/season/date
# Pick a game from 2024 season
game = conn.execute(
    "SELECT game_date, home_team, season FROM games WHERE season=2024 LIMIT 1"
).fetchone()
if game:
    gdate, team, season = game
    print(f"\n[5] Testing lookup for: team={team!r}, season={season}, date={gdate!r}")
    
    # Try exact query
    rows = conn.execute(
        "SELECT COUNT(*) FROM torvik_daily WHERE team=? AND season=? AND snapshot_date<?",
        (team, season, gdate)
    ).fetchone()[0]
    print(f"    Rows matching (team, season, date<game_date): {rows}")
    
    # Try just team match
    rows2 = conn.execute(
        "SELECT COUNT(*) FROM torvik_daily WHERE team=?", (team,)
    ).fetchone()[0]
    print(f"    Rows matching team only: {rows2}")
    
    # Try just season match
    rows3 = conn.execute(
        "SELECT COUNT(*) FROM torvik_daily WHERE season=?", (season,)
    ).fetchone()[0]
    print(f"    Rows matching season only: {rows3}")
    
    # Show what teams ARE in daily for that season
    sample_daily_teams = [r[0] for r in conn.execute(
        "SELECT DISTINCT team FROM torvik_daily WHERE season=? LIMIT 5", (season,)
    ).fetchall()]
    print(f"    Sample teams in daily for season {season}: {sample_daily_teams}")

# 6. Date format comparison
td_date = conn.execute("SELECT snapshot_date FROM torvik_daily LIMIT 1").fetchone()[0]
gm_date = conn.execute("SELECT game_date FROM games LIMIT 1").fetchone()[0]
print(f"\n[6] snapshot_date format: {td_date!r}  (type: {type(td_date).__name__})")
print(f"    game_date format:     {gm_date!r}  (type: {type(gm_date).__name__})")

# 7. Team name overlap
td_teams = set(r[0] for r in conn.execute("SELECT DISTINCT team FROM torvik_daily"))
gm_teams = set(r[0] for r in conn.execute("SELECT DISTINCT home_team FROM games"))
overlap = td_teams & gm_teams
print(f"\n[7] torvik_daily unique teams: {len(td_teams)}")
print(f"    games unique home teams:   {len(gm_teams)}")
print(f"    Exact name overlap:        {len(overlap)}")
print(f"    In daily but NOT games:    {sorted(td_teams - gm_teams)[:10]}")
print(f"    In games but NOT daily:    {sorted(gm_teams - td_teams)[:10]}")

conn.close()
print("\n" + "=" * 60)
