"""Print a full sample row from the backfilled data to see what's populated."""
import sqlite3, os
DB = os.path.join(os.getcwd(), 'data', 'basketball.db')
conn = sqlite3.connect(DB)

# Get all columns in torvik_daily
cols = [r[1] for r in conn.execute("PRAGMA table_info(torvik_daily)").fetchall()]
print("Columns:", cols)

# Show a fully populated backfilled row (non-monthly — pick one from mid-Jan)
print("\nBackfilled row (20190115, Kansas):")
row = conn.execute(
    "SELECT * FROM torvik_daily WHERE snapshot_date='20190115' AND team='Kansas'"
).fetchone()
if row:
    for c, v in zip(cols, row): print(f"  {c}: {v}")
else:
    print("  not found — trying nearby date")
    row = conn.execute(
        "SELECT * FROM torvik_daily WHERE snapshot_date LIKE '201901%' AND team='Kansas' LIMIT 1"
    ).fetchone()
    if row:
        for c, v in zip(cols, row): print(f"  {c}: {v}")

# Verify: monthly row has adj_em, backfilled row does not
print("\nMonthly row (20190201, Kansas):")
row2 = conn.execute(
    "SELECT * FROM torvik_daily WHERE snapshot_date='20190201' AND team='Kansas'"
).fetchone()
if row2:
    for c, v in zip(cols, row2): print(f"  {c}: {v}")
conn.close()
