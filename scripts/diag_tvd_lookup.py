import sqlite3, os, pandas as pd
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

# Replicate exactly what load_torvik_daily does
df = pd.read_sql(
    "SELECT season, team, adj_em, snapshot_date FROM torvik_daily LIMIT 10000", conn)

print("Raw snapshot_date dtype:", df['snapshot_date'].dtype)
print("Sample raw:", df['snapshot_date'].head(5).tolist())

def parse_snap(s):
    s = str(s).strip()
    if len(s) == 8 and '-' not in s:
        return pd.Timestamp(f"{s[:4]}-{s[4:6]}-{s[6:8]}")
    return pd.Timestamp(s)

df['snapshot_date'] = df['snapshot_date'].apply(parse_snap)
print("\nAfter parse_snap dtype:", df['snapshot_date'].dtype)
print("Sample parsed:", df['snapshot_date'].head(5).tolist())

# Now simulate a game lookup: Kansas, 2019-01-15, season 2019
team = 'Kansas'
gdate = pd.Timestamp('2019-01-15').normalize()
season = 2019

mask = (df['team']==team) & (df['season']==season) & (df['snapshot_date'] < gdate)
cands = df[mask]
print(f"\nLookup: {team}, season {season}, before {gdate.date()}")
print(f"Candidates found: {len(cands)}")
if not cands.empty:
    best = cands.loc[cands['snapshot_date'].idxmax()]
    print(f"Best snapshot: {best['snapshot_date'].date()}, adj_em={best['adj_em']}")
else:
    # Why no candidates?
    all_team = df[df['team']==team]
    print(f"Total rows for {team}: {len(all_team)}")
    print(f"Seasons available: {sorted(all_team['season'].unique())}")
    # Check if team name is normalized differently
    kansases = df[df['team'].str.contains('ansas', case=False, na=False)]['team'].unique()
    print(f"Team names containing 'ansas': {kansases}")

conn.close()
