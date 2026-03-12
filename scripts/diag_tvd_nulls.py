"""Check how many adj_em values are NULL in torvik_daily."""
import sqlite3, os
DB = os.path.join(os.getcwd(), 'data', 'basketball.db')
conn = sqlite3.connect(DB)

print("=== NULL rates in torvik_daily ===")
cols = ['adj_o', 'adj_d', 'adj_em', 'barthag', 'efg_o', 'efg_d']
total = conn.execute("SELECT COUNT(*) FROM torvik_daily").fetchone()[0]
for col in cols:
    nulls = conn.execute(f"SELECT COUNT(*) FROM torvik_daily WHERE {col} IS NULL").fetchone()[0]
    print(f"  {col}: {nulls:,} NULL / {total:,} total ({100*nulls/total:.1f}%)")

print("\n=== Sample rows where adj_em IS NULL ===")
rows = conn.execute(
    "SELECT season, team, snapshot_date, adj_o, adj_d, adj_em FROM torvik_daily "
    "WHERE adj_em IS NULL LIMIT 5"
).fetchall()
for r in rows: print(f"  {r}")

print("\n=== Sample rows where adj_em IS NOT NULL ===")
rows = conn.execute(
    "SELECT season, team, snapshot_date, adj_o, adj_d, adj_em FROM torvik_daily "
    "WHERE adj_em IS NOT NULL AND season=2019 AND team='Kansas' LIMIT 3"
).fetchall()
for r in rows: print(f"  {r}")
conn.close()
