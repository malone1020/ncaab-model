"""Check exactly what's in torvik_daily around Jan 2016."""
import sqlite3, os
DB = os.path.join(os.getcwd(), 'data', 'basketball.db')
conn = sqlite3.connect(DB)

# Raw query — no filtering — just show all snapshot_dates near Jan 2016
print("=== All snapshot_dates in torvik_daily between 20151201 and 20160201 ===")
rows = conn.execute("""
    SELECT DISTINCT snapshot_date, season, COUNT(*) as n
    FROM torvik_daily
    WHERE snapshot_date >= '20151201' AND snapshot_date <= '20160201'
    GROUP BY snapshot_date, season
    ORDER BY snapshot_date
    LIMIT 30
""").fetchall()
print(f"Rows returned: {len(rows)}")
for r in rows: print(f"  snap={r[0]!r}  season={r[1]}  teams={r[2]}")

# Also check the raw bytes/length of snapshot_date values
print("\n=== snapshot_date length distribution ===")
rows2 = conn.execute("""
    SELECT length(snapshot_date), COUNT(*) 
    FROM torvik_daily 
    GROUP BY length(snapshot_date)
""").fetchall()
for r in rows2: print(f"  len={r[0]}  count={r[1]:,}")

# Show a few raw values of different lengths
print("\n=== Sample snapshot_date values by length ===")
for length in [8, 10]:
    rows3 = conn.execute(f"""
        SELECT snapshot_date, season FROM torvik_daily 
        WHERE length(snapshot_date)={length} LIMIT 3
    """).fetchall()
    if rows3:
        print(f"  len={length}: {[r[0] for r in rows3]}")

conn.close()
