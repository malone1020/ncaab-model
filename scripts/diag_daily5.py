import sqlite3, pandas as pd

DB = r"..\data\basketball.db"
conn = sqlite3.connect(DB)

# Show the actual schema of torvik_daily
schema = pd.read_sql("PRAGMA table_info(torvik_daily)", conn)
print("torvik_daily schema:")
print(schema[['name','type']].to_string())

print()

# Show a sample row with ALL columns
row = pd.read_sql("SELECT * FROM torvik_daily WHERE barthag IS NOT NULL LIMIT 1", conn)
print("Sample row (all columns):")
for col in row.columns:
    print(f"  {col}: {row.iloc[0][col]}")

conn.close()
