import sqlite3, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

# How was rolling built? Check game_team_stats date vs games date for same game
print("=== Date consistency: game_team_stats vs games ===")
mismatch = conn.execute("""
    SELECT gts.game_date as gts_date, g.game_date as g_date,
           gts.team, g.home_team, g.away_team
    FROM game_team_stats gts
    JOIN games g ON gts.game_id = g.id
    WHERE gts.game_date != g.game_date
    LIMIT 10
""").fetchall()
print(f"Date mismatches between gts and games: {len(mismatch)}")
for r in mismatch:
    print(f"  gts:{r[0]} vs games:{r[1]}  team:{r[2]}  home:{r[3]} away:{r[4]}")

# How many rolling rows have a date that exists in games?
roll_dates = set(r[0] for r in conn.execute("SELECT DISTINCT game_date FROM rolling_efficiency").fetchall())
game_dates = set(r[0] for r in conn.execute("SELECT DISTINCT game_date FROM games").fetchall())
print(f"\nRolling unique dates: {len(roll_dates)}")
print(f"Games unique dates:   {len(game_dates)}")
print(f"Rolling dates in games: {len(roll_dates & game_dates)}")
print(f"Rolling dates NOT in games: {len(roll_dates - game_dates)}")
print(f"Sample rolling dates not in games: {sorted(roll_dates - game_dates)[:10]}")

# Key insight: rolling stores the date OF the game (the game we computed stats for)
# but lookup uses that date to find rolling stats FOR that game
# Rolling should store the date as a lookup key meaning "stats as of before this game"
# Check: does rolling row for game_date=X contain stats from games BEFORE X?
print("\n=== Sample: rolling row for a team on a date ===")
sample = conn.execute("""
    SELECT r.game_date, r.team, r.games_played, r.r5_margin
    FROM rolling_efficiency r
    WHERE r.team = 'Duke' 
    ORDER BY r.game_date
    LIMIT 5
""").fetchall()
for r in sample:
    print(f"  date:{r[0]} team:{r[1]} games_played:{r[2]} r5_margin:{r[3]}")

# Check if games table has Duke on those dates
print("\n=== Duke games in games table (first 5) ===")
duke = conn.execute("""
    SELECT game_date, home_team, away_team FROM games
    WHERE home_team='Duke' OR away_team='Duke'
    ORDER BY game_date LIMIT 5
""").fetchall()
for r in duke:
    print(f"  {r}")

conn.close()
