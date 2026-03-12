"""
fix_torvik_columns.py — Fix column shift in torvik_daily using pure SQL.

From the DB sample, the original INSERT stored values shifted by one position:
  barthag column  <- received adj_o value (~119)  
  adj_em column   <- received adj_d value (~95)
  efg_o column    <- received barthag value (~0.97)
  efg_d column    <- received efg_o value (~0.52)
  tov_o column    <- received efg_d value (~0.52)
  tov_d column    <- received tov_o value (~14.5)
  orb column      <- received tov_d value
  drb column      <- received orb value
  ftr_o column    <- received drb value
  ftr_d column    <- received ftr_o value
  adj_o/adj_d/adj_t -> NULL (index was wrong, fell off the array)

Wait - let's verify this theory against the sample:
  barthag: 119.1  <- adj_o for Kansas 2016 (plausible, Kansas was top offense)
  efg_o: 0.97     <- barthag for Kansas (0.97 = very high, top team in 2016, plausible)
  tov_o: 14.5     <- efg_o as decimal? No, 14.5 is a valid tov% 
  tov_d: 3.47     <- this is too low for tov_d (should be ~15-25%)
  orb: None
  drb: 0.0

Actually drb=0.0 and orb=None suggests the shift isn't uniform.
Let's use a SQL approach: reconstruct adj_o from barthag (it has it),
compute adj_d from adj_em (which has adj_d), compute adj_em properly.

Safest fix: just UPDATE adj_o=barthag for all rows, then 
adj_em = adj_o - adj_d... but we don't have adj_d either.

Actually the cleanest fix is to look at what we DO have reliably:
- barthag has adj_o values (90-130 range) ✓  
- efg_o has barthag values (0-1 range) ✓
- The tov/ftr/orb/drb values may be shifted too

Let me verify ranges first, then decide.
"""
import sqlite3
import pandas as pd

DB = r"..\data\basketball.db"
conn = sqlite3.connect(DB)

print("Checking actual value ranges in torvik_daily columns:")
stats = pd.read_sql("""
    SELECT 
        MIN(barthag) as barthag_min, MAX(barthag) as barthag_max, AVG(barthag) as barthag_avg,
        MIN(efg_o) as efg_o_min, MAX(efg_o) as efg_o_max, AVG(efg_o) as efg_o_avg,
        MIN(efg_d) as efg_d_min, MAX(efg_d) as efg_d_max, AVG(efg_d) as efg_d_avg,
        MIN(tov_o) as tov_o_min, MAX(tov_o) as tov_o_max, AVG(tov_o) as tov_o_avg,
        MIN(tov_d) as tov_d_min, MAX(tov_d) as tov_d_max, AVG(tov_d) as tov_d_avg,
        MIN(orb) as orb_min, MAX(orb) as orb_max, AVG(orb) as orb_avg,
        MIN(drb) as drb_min, MAX(drb) as drb_max, AVG(drb) as drb_avg,
        MIN(ftr_o) as ftr_o_min, MAX(ftr_o) as ftr_o_max, AVG(ftr_o) as ftr_o_avg,
        MIN(ftr_d) as ftr_d_min, MAX(ftr_d) as ftr_d_max, AVG(ftr_d) as ftr_d_avg
    FROM torvik_daily WHERE barthag IS NOT NULL
""", conn)

for col_base in ['barthag','efg_o','efg_d','tov_o','tov_d','orb','drb','ftr_o','ftr_d']:
    mn = stats[f'{col_base}_min'].iloc[0]
    mx = stats[f'{col_base}_max'].iloc[0]
    av = stats[f'{col_base}_avg'].iloc[0]
    if mn is not None:
        print(f"  {col_base:8s}: min={mn:.3f}  max={mx:.3f}  avg={av:.3f}")
    else:
        print(f"  {col_base:8s}: ALL NULL")

conn.close()
