import sqlite3, pandas as pd

DB = r"..\data\basketball.db"
conn = sqlite3.connect(DB)

# Check null rates in torvik_daily
result = pd.read_sql("""
    SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN adj_em IS NULL THEN 1 ELSE 0 END) as null_adj_em,
        SUM(CASE WHEN adj_o IS NULL THEN 1 ELSE 0 END) as null_adj_o,
        SUM(CASE WHEN barthag IS NULL THEN 1 ELSE 0 END) as null_barthag,
        SUM(CASE WHEN efg_o IS NULL THEN 1 ELSE 0 END) as null_efg_o
    FROM torvik_daily
""", conn)
print("Null counts in torvik_daily:")
print(result.T)

# Sample non-null rows
sample = pd.read_sql("""
    SELECT season, team, snapshot_date, adj_em, adj_o, barthag, efg_o
    FROM torvik_daily
    WHERE adj_em IS NOT NULL
    LIMIT 5
""", conn)
print("\nSample rows with non-null adj_em:")
print(sample)

# How many rows have at least some data?
nonull = pd.read_sql("""
    SELECT COUNT(*) as ct FROM torvik_daily WHERE adj_em IS NOT NULL
""", conn)
print(f"\nRows with non-null adj_em: {nonull.iloc[0,0]:,} / ", end="")
total = pd.read_sql("SELECT COUNT(*) as ct FROM torvik_daily", conn)
print(f"{total.iloc[0,0]:,}")
conn.close()
