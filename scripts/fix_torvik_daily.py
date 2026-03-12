"""
fix_torvik_daily.py — Wipe and re-pull torvik_daily with correct column mapping.

Run from scripts/ folder:  python fix_torvik_daily.py

Takes ~15-25 min. Pulls one date snapshot per week per season (not every day).
"""
import sqlite3, requests, time
from datetime import datetime, timedelta

DB = r"..\data\basketball.db"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) research"}

def fv(x):
    try: return float(x) if x not in (None, '', 'None', 'nan') else None
    except: return None

def parse_entry(td):
    """
    Torvik trankings JSON entry layout (verified from DB cross-check):
    Each team entry is a list. From our DB sample we know:
      - barthag column received ~119  -> that's adj_o
      - efg_o column received ~0.97   -> that's barthag  
      - tov_o column received ~14.5   -> that's efg_o (as decimal *100 = 52-62%)
    
    So the actual layout is:
    [0]=team, [1]=conf, [2]=record_str,
    [3]=adj_o, [4]=adj_d, [5]=adj_t,
    [6]=barthag, [7]=adj_em_rank, [8]=?,
    [9]=efg_o, [10]=efg_d, [11]=tov_o, [12]=tov_d,
    [13]=orb, [14]=drb, [15]=ftr_o, [16]=ftr_d
    
    BUT the original 03 used DAILY_IDX which was wrong, storing:
      adj_o slot <- nothing (None)
      barthag slot <- td[adj_o position] which was ~119
    
    We detect columns by value ranges:
    - adj_o: 88-140 (offensive efficiency)
    - adj_d: 78-120 (defensive efficiency, lower=better)  
    - adj_t: 55-85  (tempo, possessions per game)
    - barthag: 0.0-1.0 (win probability vs avg)
    - efg_o/d: 0.35-0.75 (effective FG%, as decimal)
    - tov_o/d: 8-25 (turnover rate %)
    - orb/drb: 15-50 (rebound rate %)
    - ftr_o/d: 15-55 (free throw attempt rate %)
    """
    if not isinstance(td, (list, tuple)) or len(td) < 6:
        return None
    team = str(td[0]).strip()
    if not team or team.isdigit():
        return None

    # Scan positions 1-20 and classify by value range
    adj_o = adj_d = adj_t = barthag = efg_o = efg_d = None
    tov_o = tov_d = orb = drb = ftr_o = ftr_d = None
    
    # Use ordered scan — first match wins for each bucket
    tempo_found = False
    barthag_found = False
    efg_count = 0
    tov_count = 0
    orb_count = 0
    ftr_count = 0
    
    for pos in range(1, min(25, len(td))):
        v = fv(td[pos])
        if v is None: continue
        
        if adj_o is None and 88 <= v <= 145:
            adj_o = v
        elif adj_o is not None and adj_d is None and 78 <= v <= 125 and v != adj_o:
            adj_d = v
        elif not tempo_found and 55 <= v <= 88:
            adj_t = v; tempo_found = True
        elif not barthag_found and 0.001 <= v <= 0.999:
            barthag = v; barthag_found = True
        elif barthag_found and efg_count < 2 and 0.30 <= v <= 0.75:
            if efg_o is None: efg_o = v
            else: efg_d = v
            efg_count += 1
        elif efg_count == 2 and tov_count < 2 and 8 <= v <= 28:
            if tov_o is None: tov_o = v
            else: tov_d = v
            tov_count += 1
        elif tov_count == 2 and orb_count < 2 and 15 <= v <= 55:
            if orb is None: orb = v
            else: drb = v
            orb_count += 1
        elif orb_count == 2 and ftr_count < 2 and 15 <= v <= 65:
            if ftr_o is None: ftr_o = v
            else: ftr_d = v
            ftr_count += 1

    if adj_o is None or adj_d is None:
        return None

    adj_em = round(adj_o - adj_d, 4) if adj_o and adj_d else None
    return (team, adj_o, adj_d, adj_t, barthag, adj_em, efg_o, efg_d,
            tov_o, tov_d, orb, drb, ftr_o, ftr_d)


def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    # Drop and recreate torvik_daily
    print("Dropping and recreating torvik_daily...")
    cur.execute("DROP TABLE IF EXISTS torvik_daily")
    cur.execute("""
        CREATE TABLE torvik_daily (
            season        INTEGER,
            snapshot_date TEXT,
            team          TEXT,
            adj_o         REAL,
            adj_d         REAL,
            adj_t         REAL,
            barthag       REAL,
            adj_em        REAL,
            efg_o         REAL,
            efg_d         REAL,
            tov_o         REAL,
            tov_d         REAL,
            orb           REAL,
            drb           REAL,
            ftr_o         REAL,
            ftr_d         REAL,
            PRIMARY KEY (season, snapshot_date, team)
        )
    """)
    conn.commit()

    total_rows = 0

    for season in range(2016, 2026):
        print(f"\n[Season {season}]")
        season_rows = 0

        # Sample ~twice per week throughout the season (Nov through April)
        # Season year = ending year (2016 = 2015-16)
        start = datetime(season - 1, 11, 1)
        end   = datetime(season, 4, 15)

        dates = []
        d = start
        while d <= end:
            dates.append(d.strftime("%Y%m%d"))
            d += timedelta(days=4)  # every 4 days = ~twice/week

        batch = []
        for snap_date in dates:
            url = (f"https://barttorvik.com/trankings.php"
                   f"?date={snap_date}&top=0&conlimit=All&year={season}&json=1")
            try:
                r = requests.get(url, timeout=30, headers=HEADERS)
                r.raise_for_status()
                data = r.json()
                if not isinstance(data, list) or len(data) == 0:
                    continue
            except Exception as e:
                continue

            for td in data:
                parsed = parse_entry(td)
                if parsed is None:
                    continue
                team = parsed[0]
                batch.append((
                    season, snap_date, team,
                    parsed[1], parsed[2], parsed[3], parsed[4], parsed[5],
                    parsed[6], parsed[7], parsed[8], parsed[9], parsed[10],
                    parsed[11], parsed[12], parsed[13]
                ))

            if len(batch) >= 2000:
                cur.executemany("""
                    INSERT OR REPLACE INTO torvik_daily
                    (season, snapshot_date, team, adj_o, adj_d, adj_t, barthag, adj_em,
                     efg_o, efg_d, tov_o, tov_d, orb, drb, ftr_o, ftr_d)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, batch)
                conn.commit()
                season_rows += len(batch)
                batch = []

            time.sleep(0.25)

        if batch:
            cur.executemany("""
                INSERT OR REPLACE INTO torvik_daily
                (season, snapshot_date, team, adj_o, adj_d, adj_t, barthag, adj_em,
                 efg_o, efg_d, tov_o, tov_d, orb, drb, ftr_o, ftr_d)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, batch)
            conn.commit()
            season_rows += len(batch)

        # Spot-check: sample a row and verify values look right
        sample = cur.execute(
            "SELECT team, adj_o, adj_d, adj_t, barthag, adj_em FROM torvik_daily "
            "WHERE season=? AND adj_o IS NOT NULL LIMIT 1", (season,)
        ).fetchone()
        total_rows += season_rows
        if sample:
            print(f"  {season_rows:,} rows | sample: {sample[0]} "
                  f"adjO={sample[1]:.1f} adjD={sample[2]:.1f} "
                  f"tempo={sample[3]:.1f} barthag={sample[4]:.3f} em={sample[5]:.1f}")
        else:
            print(f"  {season_rows:,} rows (no sample found)")

    print(f"\nTotal rows inserted: {total_rows:,}")
    
    # Final null check
    nulls = cur.execute("SELECT COUNT(*) FROM torvik_daily WHERE adj_o IS NULL").fetchone()[0]
    print(f"Rows with NULL adj_o: {nulls:,}")
    conn.close()

if __name__ == "__main__":
    main()
