import sqlite3
DB = r"..\data\basketball.db"
conn = sqlite3.connect(DB)

print("=== torvik_season ===")
for season in range(2016, 2026):
    ct = conn.execute("SELECT COUNT(*) FROM torvik_season WHERE season=?", (season,)).fetchone()[0]
    null = conn.execute("SELECT COUNT(*) FROM torvik_season WHERE season=? AND adj_o IS NULL", (season,)).fetchone()[0]
    s = conn.execute("SELECT team, adj_o, adj_d FROM torvik_season WHERE season=? AND adj_o IS NOT NULL LIMIT 1", (season,)).fetchone()
    print(f"  {season}: {ct} rows, {null} null | sample: {s}")

print()
print("=== adj_o range (should be 85-135) ===")
r = conn.execute("SELECT MIN(adj_o), MAX(adj_o), AVG(adj_o) FROM torvik_season WHERE adj_o IS NOT NULL").fetchone()
print(f"  min={r[0]:.1f}  max={r[1]:.1f}  avg={r[2]:.1f}")

print()
print("=== torvik_daily ===")
for season in range(2016, 2026):
    ct = conn.execute("SELECT COUNT(*) FROM torvik_daily WHERE season=?", (season,)).fetchone()[0]
    print(f"  {season}: {ct} rows")

print()
print("=== daily snapshot_date samples ===")
rows = conn.execute("SELECT DISTINCT snapshot_date FROM torvik_daily ORDER BY snapshot_date LIMIT 10").fetchall()
for r in rows: print(f"  {r[0]}")

conn.close()
