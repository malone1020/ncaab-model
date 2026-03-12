import sqlite3, os, pandas as pd

# Must be run from project root: python scripts/diag_trace_lookup.py
DB = os.path.join(os.getcwd(), 'data', 'basketball.db')
print(f"DB path: {DB}")
print(f"DB exists: {os.path.exists(DB)}")

conn = sqlite3.connect(DB)

# Check tables
tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
print(f"Tables: {tables}")

# Check torvik_daily
n = conn.execute("SELECT COUNT(*) FROM torvik_daily").fetchone()[0]
print(f"torvik_daily rows: {n:,}")

# Sample Kansas 2019 snapshots
df = pd.read_sql(
    "SELECT season, team, snapshot_date FROM torvik_daily "
    "WHERE season=2019 AND team='Kansas' ORDER BY snapshot_date LIMIT 5", conn)
print(f"\nKansas 2019 snapshots:\n{df.to_string()}")

# Check team name normalization — what does 'Kansas' look like in torvik_daily?
sample = pd.read_sql(
    "SELECT DISTINCT team FROM torvik_daily WHERE season=2019 AND team LIKE '%ansas%'", conn)
print(f"\nTeams like 'ansas' in torvik_daily: {sample['team'].tolist()}")

# Check what Kansas looks like in games table
games_sample = pd.read_sql(
    "SELECT DISTINCT home_team FROM games WHERE season=2019 AND home_team LIKE '%ansas%' LIMIT 5", conn)
print(f"Teams like 'ansas' in games: {games_sample['home_team'].tolist()}")

# Now simulate the full index build for just Kansas 2019
df2 = pd.read_sql(
    "SELECT season, team, adj_em, snapshot_date FROM torvik_daily "
    "WHERE season=2019 AND team='Kansas'", conn)

def snap_to_int(s):
    return int(str(s).strip().replace('-', '')[:8])

df2['snap_int'] = df2['snapshot_date'].apply(snap_to_int)
df2 = df2.sort_values('snap_int')
snaps = df2['snap_int'].tolist()
print(f"\nKansas 2019 snap_ints: {snaps[:5]} ... {snaps[-3:]}")

# Simulate game date lookup
game = conn.execute(
    "SELECT game_date FROM games WHERE season=2019 AND home_team='Kansas' LIMIT 1"
).fetchone()
if game:
    gd = pd.Timestamp(game[0])
    gdate_int = int(gd.strftime('%Y%m%d'))
    print(f"\nGame date: {gd.date()}, gdate_int: {gdate_int}")
    print(f"Snaps before game: {[s for s in snaps if s < gdate_int]}")

conn.close()
