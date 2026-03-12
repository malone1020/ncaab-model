import sqlite3
import pandas as pd
import re

DB = r"..\data\basketball.db"
conn = sqlite3.connect(DB)

# Load map from current 04_build_features.py
with open("04_build_features.py") as f:
    content = f.read()
m = re.search(r"CBBD_TO_TORVIK = \{(.*?)\n\}", content, re.DOTALL)
exec("CBBD_TO_TORVIK = {" + m.group(1) + "}")
def norm(name):
    if not name: return name
    return CBBD_TO_TORVIK.get(str(name).strip(), str(name).strip())

# Load torvik_daily exactly as 04 does
df = pd.read_sql(
    "SELECT season, team, adj_em, "
    "SUBSTR(snapshot_date,1,4)||\'-\'||SUBSTR(snapshot_date,5,2)||\'-\'||SUBSTR(snapshot_date,7,2) AS snapshot_date "
    "FROM torvik_daily LIMIT 5", conn)
print("SQL SUBSTR test:")
print(df)
print()

# Load full daily
df2 = pd.read_sql("SELECT season, team, adj_em, snapshot_date FROM torvik_daily LIMIT 3", conn)
print("Raw snapshot_date values:", df2["snapshot_date"].tolist())
print("snapshot_date dtype:", df2["snapshot_date"].dtype)
print()

# Try parsing
df3 = pd.read_sql("SELECT season, team, adj_em, snapshot_date FROM torvik_daily", conn)
df3["team"] = df3["team"].apply(norm)
df3["snapshot_date"] = pd.to_datetime(df3["snapshot_date"].astype(str), format="%Y%m%d", errors="coerce")
df3["season"] = df3["season"].astype(int)
valid = df3["snapshot_date"].notna().sum()
print(f"Parsed: {len(df3)} rows, {valid} valid dates")
print("Sample parsed dates:", df3["snapshot_date"].iloc[:3].tolist())
print()

# Now test a specific lookup for a team we know is in both
# Get a game with its date and teams
game = pd.read_sql(
    "SELECT id, season, game_date, home_team, away_team FROM games "
    "WHERE season=2024 AND home_team IN (SELECT DISTINCT team FROM torvik_daily) LIMIT 1", conn)
print("Test game:")
print(game)

if not game.empty:
    g = game.iloc[0]
    team = norm(g["home_team"])
    gdate = pd.Timestamp(g["game_date"]).normalize()
    season = int(g["season"])
    print(f"\nLooking up: team={team!r}, season={season}, gdate={gdate}")
    mask = (df3["team"]==team) & (df3["season"]==season) & (df3["snapshot_date"]<gdate)
    print(f"Mask matches: {mask.sum()}")
    if mask.sum() == 0:
        # Check each condition separately
        t_match = (df3["team"]==team).sum()
        s_match = (df3["season"]==season).sum()
        both = ((df3["team"]==team) & (df3["season"]==season)).sum()
        print(f"  team match: {t_match}, season match: {s_match}, both: {both}")
        # Sample what teams/seasons look like
        sample = df3[df3["team"]==team][["team","season","snapshot_date"]].head(3)
        print(f"  Sample rows for {team!r}:")
        print(sample)

conn.close()
