import pandas as pd, os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV  = os.path.join(ROOT, 'data', 'torvik_timemachine.csv')

df = pd.read_csv(CSV)
print(f"Rows: {len(df)}, Cols: {len(df.columns)}")
print(f"\nAll column names:")
for i, c in enumerate(df.columns):
    print(f"  [{i}]: {c}")
print(f"\nFirst row values:")
for i, v in enumerate(df.iloc[0]):
    print(f"  [{i}]: {repr(v)}")
