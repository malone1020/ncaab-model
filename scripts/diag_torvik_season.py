import sqlite3
DB = r"..\data\basketball.db"
conn = sqlite3.connect(DB)

print("torvik_season row count:", conn.execute("SELECT COUNT(*) FROM torvik_season").fetchone()[0])
print("torvik_season NULL adj_o:", conn.execute("SELECT COUNT(*) FROM torvik_season WHERE adj_o IS NULL").fetchone()[0])
print("torvik_season schema:")
for row in conn.execute("PRAGMA table_info(torvik_season)"):
    print(f"  {row[1]}: {row[2]}")
print()
print("Sample row:")
row = conn.execute("SELECT * FROM torvik_season LIMIT 1").fetchone()
cols = [r[1] for r in conn.execute("PRAGMA table_info(torvik_season)")]
for c, v in zip(cols, row):
    print(f"  {c}: {v}")
conn.close()
