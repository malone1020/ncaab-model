import sqlite3, os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, 'data', 'basketball.db')
conn = sqlite3.connect(DB)

print("KenPom snapshot_types:")
for r in conn.execute("SELECT snapshot_type, COUNT(*) FROM kenpom_ratings GROUP BY snapshot_type"):
    print(f"  {r[0]}: {r[1]} rows")

print("\nKenPom seasons:")
for r in conn.execute("SELECT season, snapshot_type, COUNT(*) FROM kenpom_ratings GROUP BY season, snapshot_type ORDER BY season"):
    print(f"  {r[0]} {r[1]}: {r[2]} teams")

print("\nSample - 2023 Duke all snapshot types:")
for r in conn.execute("SELECT season, snapshot_type, team, adj_em, adj_o, adj_d FROM kenpom_ratings WHERE season=2023 AND team LIKE '%Duke%'"):
    print(f"  {r}")
conn.close()
