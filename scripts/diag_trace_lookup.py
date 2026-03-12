import sqlite3, os, pandas as pd, sys
sys.path.insert(0, 'scripts')

ROOT = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

# Replicate build of index exactly as 04_build_features does
df = pd.read_sql(
    "SELECT season, team, adj_o, adj_d, barthag, adj_em, "
    "efg_o, efg_d, tov_o, tov_d, orb, drb, ftr_o, ftr_d, "
    "snapshot_date FROM torvik_daily WHERE season=2019 AND team='Kansas'", conn)

print(f"Raw rows for Kansas 2019: {len(df)}")
print("Sample snapshot_date values:", df['snapshot_date'].head(5).tolist())
print("dtype:", df['snapshot_date'].dtype)

def snap_to_int(s):
    return int(str(s).strip().replace('-', '')[:8])

df['snap_int'] = df['snapshot_date'].apply(snap_to_int)
df_s = df.sort_values('snap_int')
snaps = list(zip(df_s['snap_int'].tolist(), [{}]*len(df_s)))

print(f"\nsnap_ints for Kansas 2019: {[s[0] for s in snaps[:5]]} ... {[s[0] for s in snaps[-3:]]}")

# Now simulate a game on 2019-01-15
game_rows = conn.execute("SELECT game_date, season FROM games WHERE season=2019 AND home_team='Kansas' LIMIT 3").fetchall()
print(f"\nSample Kansas 2019 games: {game_rows}")

# What does gd look like in the loop?
games_df = pd.read_sql("SELECT game_date FROM games WHERE season=2019 AND home_team='Kansas' LIMIT 3", conn)
games_df['game_date'] = pd.to_datetime(games_df['game_date'])
for gd in games_df['game_date']:
    print(f"\ngd type={type(gd)}, value={gd}")
    if hasattr(gd, 'strftime'):
        gdate_int = int(gd.strftime('%Y%m%d'))
    else:
        gdate_int = int(str(gd).replace('-', '')[:8])
    print(f"gdate_int={gdate_int}")
    print(f"snap_ints sample: {[s[0] for s in snaps[:5]]}")
    print(f"Would find: {any(s[0] < gdate_int for s in snaps)}")

conn.close()
