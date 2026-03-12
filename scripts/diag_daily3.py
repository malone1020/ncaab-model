import sqlite3, pandas as pd, re

DB = r"..\data\basketball.db"
conn = sqlite3.connect(DB)

# Load norm() map from 04_build_features.py
with open("04_build_features.py") as f:
    content = f.read()
m = re.search(r"CBBD_TO_TORVIK = \{(.*?)\n\}", content, re.DOTALL)
exec("CBBD_TO_TORVIK = {" + m.group(1) + "}")
def norm(name):
    if not name: return name
    return CBBD_TO_TORVIK.get(str(name).strip(), str(name).strip())

# ── Load torvik_daily exactly as 04 does ──
tv_d = pd.read_sql(
    "SELECT season, team, adj_em, "
    "SUBSTR(snapshot_date,1,4)||'-'||SUBSTR(snapshot_date,5,2)||'-'||SUBSTR(snapshot_date,7,2) AS snapshot_date "
    "FROM torvik_daily", conn)
tv_d['team'] = tv_d['team'].apply(norm)
tv_d['snapshot_date'] = pd.to_datetime(tv_d['snapshot_date'])
tv_d['season'] = tv_d['season'].astype(int)

print(f"tv_d shape: {tv_d.shape}")
print(f"snapshot_date dtype: {tv_d['snapshot_date'].dtype}")
print(f"Sample dates: {tv_d['snapshot_date'].iloc[:3].tolist()}")
print(f"Season dtype: {tv_d['season'].dtype}")
print(f"Sample teams: {tv_d['team'].iloc[:5].tolist()}")
print()

# ── Pick a known team+season+date ──
team = 'Duke'
season = 2024
gdate = pd.Timestamp('2024-01-15').normalize()

print(f"Testing: team={team!r}, season={season}, gdate={gdate}")
mask = (tv_d['team']==team) & (tv_d['season']==int(season)) & (tv_d['snapshot_date']<gdate)
print(f"Mask hits: {mask.sum()}")

# Check each condition
print(f"  team==Duke: {(tv_d['team']==team).sum()}")
print(f"  season==2024: {(tv_d['season']==season).sum()}")
print(f"  date<2024-01-15: {(tv_d['snapshot_date']<gdate).sum()}")
print(f"  team+season: {((tv_d['team']==team) & (tv_d['season']==season)).sum()}")
if mask.sum() == 0:
    # Show Duke rows to understand what's there
    duke_rows = tv_d[tv_d['team']=='Duke']
    print(f"\nAll Duke rows ({len(duke_rows)}):")
    print(duke_rows[['team','season','snapshot_date','adj_em']].head(5))
conn.close()
