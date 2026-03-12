"""
Script 03c: Pull Torvik (T-Rank) Data
=======================================
Pulls two things from barttorvik.com:

1. TIME MACHINE RATINGS — T-Rank ratings as they existed on specific dates
   during each season. We pull one snapshot per week so we have ratings
   that reflect what was known on or before each game date. No leakage.

   URL: barttorvik.com/YYYY_team_results.csv  (season-final, free)
   URL: barttorvik.com/timemachine/team_results/YYYYMMDD_team_results.json.gz
        (daily snapshots — we pull weekly)

2. GAME PREDICTIONS — Torvik's own predicted margin for every historical game.
   When our model disagrees with Torvik's prediction in a specific direction,
   that disagreement IS the exploitable signal vs DraftKings lines.

   URL: barttorvik.com/gamepreview.php?... (individual game, slow)
   Better: barttorvik.com/YYYY_game_results.json (all games in season)

Tables created:
  - torvik_ratings   : team ratings by season + snapshot_date
  - torvik_game_preds: Torvik's predicted margin per game

Usage:
    python scripts/03c_pull_torvik.py
"""

import sqlite3
import requests
import pandas as pd
import numpy as np
import json
import gzip
import time
import io
from pathlib import Path
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR / "data" / "basketball.db"

SEASONS  = list(range(2016, 2026))   # 2015-16 through 2024-25
HEADERS  = {"User-Agent": "Mozilla/5.0 (research project, not scraping)"}
SLEEP    = 1.5   # seconds between requests — be respectful

# ── Season date ranges ─────────────────────────────────────────────────────────
# Torvik season year = the year the season ENDS (e.g. 2024 = 2023-24 season)
SEASON_DATES = {
    2016: ("20151101", "20160401"),
    2017: ("20161101", "20170401"),
    2018: ("20171101", "20180401"),
    2019: ("20181101", "20190401"),
    2020: ("20191101", "20200401"),
    2021: ("20201101", "20210401"),
    2022: ("20211101", "20220401"),
    2023: ("20221101", "20230401"),
    2024: ("20231101", "20240401"),
    2025: ("20241101", "20250401"),
}

# ── Column names for Torvik season CSV ────────────────────────────────────────
# From barttorvik.com/2024_team_results.csv
TORVIK_COLS = [
    "rank", "team", "conf", "g", "rec",
    "adj_o", "adj_o_rank",
    "adj_d", "adj_d_rank",
    "adj_t", "adj_t_rank",
    "barthag", "barthag_rank",
    "efg_o", "efg_d",
    "tov_o", "tov_d",
    "orb", "drb",
    "ftr_o", "ftr_d",
    "two_pt_o", "two_pt_d",
    "three_pt_o", "three_pt_d",
    "adj_o_close", "adj_d_close",
    "wins_ari", "wins_bpd", "wab",
]


def init_tables(conn):
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS torvik_ratings (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            season          INTEGER NOT NULL,
            snapshot_date   TEXT NOT NULL,
            snapshot_type   TEXT NOT NULL,  -- 'final', 'weekly', 'preseason'
            team            TEXT NOT NULL,
            conf            TEXT,
            games           INTEGER,
            adj_o           REAL,
            adj_d           REAL,
            adj_t           REAL,
            barthag         REAL,
            adj_em          REAL,           -- adj_o - adj_d (like KenPom)
            efg_o           REAL,
            efg_d           REAL,
            tov_o           REAL,
            tov_d           REAL,
            orb             REAL,
            drb             REAL,
            ftr_o           REAL,
            ftr_d           REAL,
            wab             REAL,
            UNIQUE(season, snapshot_date, team)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS torvik_game_preds (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id         INTEGER,
            season          INTEGER,
            game_date       TEXT,
            home_team       TEXT,
            away_team       TEXT,
            torvik_home_pts REAL,
            torvik_away_pts REAL,
            torvik_margin   REAL,          -- home - away (Torvik prediction)
            torvik_win_prob REAL,          -- home win probability
            actual_margin   REAL,          -- home - away (actual)
            UNIQUE(game_id)
        )
    """)

    conn.commit()
    print("✓ Tables initialized")


# ══════════════════════════════════════════════════════════════════════════════
# PART 1: SEASON-FINAL RATINGS
# ══════════════════════════════════════════════════════════════════════════════

def pull_season_final_ratings(conn, season):
    """
    Pull end-of-season T-Rank ratings from barttorvik.com/YYYY_team_results.csv
    These are the final ratings after all games are played.
    Used as reference and for feature engineering (prior season -> current season).
    """
    url = f"https://barttorvik.com/{season}_team_results.csv"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            print(f"    ✗ HTTP {r.status_code} for {url}")
            return 0

        # Parse CSV — Torvik's CSV has no header, columns are positional
        lines = r.text.strip().split("\n")
        rows = []
        for line in lines:
            parts = line.split(",")
            if len(parts) < 5:
                continue
            rows.append(parts)

        if not rows:
            return 0

        df = pd.DataFrame(rows)
        # First two cols are rank and team, rest are stats
        # Torvik format: rank, team, conf, g, rec, adj_o, adj_o_r, adj_d, adj_d_r,
        #                adj_t, adj_t_r, barthag, barthag_r, ...
        df.columns = range(len(df.columns))

        # Extract key columns by position (robust to extra columns)
        result_rows = []
        for _, row in df.iterrows():
            try:
                team    = str(row[1]).strip().strip('"')
                conf    = str(row[2]).strip().strip('"') if len(row) > 2 else None
                games   = int(float(row[3])) if len(row) > 3 and row[3] else None
                adj_o   = float(row[5])  if len(row) > 5  and row[5]  else None
                adj_d   = float(row[7])  if len(row) > 7  and row[7]  else None
                adj_t   = float(row[9])  if len(row) > 9  and row[9]  else None
                barthag = float(row[11]) if len(row) > 11 and row[11] else None
                efg_o   = float(row[13]) if len(row) > 13 and row[13] else None
                efg_d   = float(row[14]) if len(row) > 14 and row[14] else None
                tov_o   = float(row[15]) if len(row) > 15 and row[15] else None
                tov_d   = float(row[16]) if len(row) > 16 and row[16] else None
                orb     = float(row[17]) if len(row) > 17 and row[17] else None
                drb     = float(row[18]) if len(row) > 18 and row[18] else None
                ftr_o   = float(row[19]) if len(row) > 19 and row[19] else None
                ftr_d   = float(row[20]) if len(row) > 20 and row[20] else None
                adj_em  = (adj_o - adj_d) if adj_o and adj_d else None

                result_rows.append({
                    "season": season, "snapshot_date": f"{season}0401",
                    "snapshot_type": "final", "team": team, "conf": conf,
                    "games": games, "adj_o": adj_o, "adj_d": adj_d,
                    "adj_t": adj_t, "barthag": barthag, "adj_em": adj_em,
                    "efg_o": efg_o, "efg_d": efg_d, "tov_o": tov_o,
                    "tov_d": tov_d, "orb": orb, "drb": drb,
                    "ftr_o": ftr_o, "ftr_d": ftr_d, "wab": None,
                })
            except (ValueError, IndexError):
                continue

        if not result_rows:
            return 0

        c = conn.cursor()
        inserted = 0
        for row in result_rows:
            try:
                c.execute("""
                    INSERT OR IGNORE INTO torvik_ratings
                    (season, snapshot_date, snapshot_type, team, conf, games,
                     adj_o, adj_d, adj_t, barthag, adj_em,
                     efg_o, efg_d, tov_o, tov_d, orb, drb, ftr_o, ftr_d, wab)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    row["season"], row["snapshot_date"], row["snapshot_type"],
                    row["team"], row["conf"], row["games"],
                    row["adj_o"], row["adj_d"], row["adj_t"],
                    row["barthag"], row["adj_em"],
                    row["efg_o"], row["efg_d"], row["tov_o"], row["tov_d"],
                    row["orb"], row["drb"], row["ftr_o"], row["ftr_d"], row["wab"],
                ))
                inserted += c.rowcount
            except Exception:
                continue

        conn.commit()
        return inserted

    except Exception as e:
        print(f"    ✗ Error: {e}")
        return 0


# ══════════════════════════════════════════════════════════════════════════════
# PART 2: TIME MACHINE WEEKLY SNAPSHOTS
# ══════════════════════════════════════════════════════════════════════════════

def get_weekly_dates(start_str, end_str):
    """Generate weekly dates between start and end."""
    start = datetime.strptime(start_str, "%Y%m%d")
    end   = datetime.strptime(end_str,   "%Y%m%d")
    dates = []
    d = start
    while d <= end:
        dates.append(d.strftime("%Y%m%d"))
        d += timedelta(weeks=1)
    return dates


def pull_time_machine_snapshot(conn, season, date_str):
    """
    Pull T-Rank ratings as they existed on a specific date.
    URL: barttorvik.com/timemachine/team_results/YYYYMMDD_team_results.json.gz
    """
    url = f"https://barttorvik.com/timemachine/team_results/{date_str}_team_results.json.gz"
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            return 0

        # Decompress and parse JSON
        with gzip.open(io.BytesIO(r.content), "rt", encoding="utf-8") as f:
            data = json.load(f)

        if not data:
            return 0

        c = conn.cursor()
        inserted = 0

        for entry in data:
            try:
                # JSON structure varies — try common field names
                team    = entry.get("team") or entry.get("Team", "")
                conf    = entry.get("conf") or entry.get("Conf")
                games   = entry.get("g") or entry.get("games")
                adj_o   = entry.get("adjO") or entry.get("adj_o") or entry.get("AdjOE")
                adj_d   = entry.get("adjD") or entry.get("adj_d") or entry.get("AdjDE")
                adj_t   = entry.get("adjT") or entry.get("adj_t") or entry.get("AdjTempo")
                barthag = entry.get("barthag") or entry.get("Barthag")
                efg_o   = entry.get("eFG%") or entry.get("efg_o")
                efg_d   = entry.get("eFG%D") or entry.get("efg_d")
                tov_o   = entry.get("TO%") or entry.get("tov_o")
                tov_d   = entry.get("TO%D") or entry.get("tov_d")
                orb     = entry.get("OR%") or entry.get("orb")
                drb     = entry.get("DR%") or entry.get("drb")
                ftr_o   = entry.get("FTR") or entry.get("ftr_o")
                ftr_d   = entry.get("FTRD") or entry.get("ftr_d")
                wab     = entry.get("WAB") or entry.get("wab")
                adj_em  = (float(adj_o) - float(adj_d)) if adj_o and adj_d else None

                if not team:
                    continue

                c.execute("""
                    INSERT OR IGNORE INTO torvik_ratings
                    (season, snapshot_date, snapshot_type, team, conf, games,
                     adj_o, adj_d, adj_t, barthag, adj_em,
                     efg_o, efg_d, tov_o, tov_d, orb, drb, ftr_o, ftr_d, wab)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    season, date_str, "weekly",
                    str(team).strip(), str(conf).strip() if conf else None,
                    int(games) if games else None,
                    float(adj_o) if adj_o else None,
                    float(adj_d) if adj_d else None,
                    float(adj_t) if adj_t else None,
                    float(barthag) if barthag else None,
                    round(adj_em, 4) if adj_em else None,
                    float(efg_o) if efg_o else None,
                    float(efg_d) if efg_d else None,
                    float(tov_o) if tov_o else None,
                    float(tov_d) if tov_d else None,
                    float(orb)   if orb   else None,
                    float(drb)   if drb   else None,
                    float(ftr_o) if ftr_o else None,
                    float(ftr_d) if ftr_d else None,
                    float(wab)   if wab   else None,
                ))
                inserted += c.rowcount
            except Exception:
                continue

        conn.commit()
        return inserted

    except Exception as e:
        return 0


# ══════════════════════════════════════════════════════════════════════════════
# PART 3: TORVIK GAME PREDICTIONS
# ══════════════════════════════════════════════════════════════════════════════

def pull_torvik_game_predictions(conn, season):
    """
    Pull Torvik's game-by-game predictions and results.
    URL: barttorvik.com/YYYY_game_results.json
    Contains: predicted score, actual score, win probability for each game.
    """
    url = f"https://barttorvik.com/{season}_game_results.json"
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200:
            print(f"    ✗ HTTP {r.status_code}")
            return 0

        data = r.json()
        if not data:
            return 0

        # Match games to our games table
        games_df = pd.read_sql("""
            SELECT cbbd_id, game_date, home_team, away_team
            FROM games WHERE season = ?
        """, conn, params=(season,))

        c = conn.cursor()
        inserted = 0

        for entry in data:
            try:
                # Torvik game results JSON structure
                # Varies by year — extract what we can
                home_team  = entry.get("home") or entry.get("hteam", "")
                away_team  = entry.get("away") or entry.get("vteam", "")
                game_date  = str(entry.get("date", "")).replace("-", "")
                pred_home  = entry.get("predH") or entry.get("proj_home_score")
                pred_away  = entry.get("predV") or entry.get("proj_away_score")
                win_prob   = entry.get("hWinPct") or entry.get("home_win_pct")
                act_home   = entry.get("home_score") or entry.get("hscore")
                act_away   = entry.get("away_score") or entry.get("vscore")

                if not pred_home or not pred_away:
                    continue

                pred_margin = float(pred_home) - float(pred_away)
                act_margin  = (float(act_home) - float(act_away)) if act_home and act_away else None

                # Try to match to our game IDs by date + teams
                match = games_df[
                    (games_df["game_date"].str.replace("-","") == game_date) &
                    (
                        games_df["home_team"].str.contains(home_team[:6], case=False, na=False) |
                        games_df["away_team"].str.contains(away_team[:6], case=False, na=False)
                    )
                ]
                game_id = int(match["cbbd_id"].iloc[0]) if len(match) > 0 else None

                c.execute("""
                    INSERT OR IGNORE INTO torvik_game_preds
                    (game_id, season, game_date, home_team, away_team,
                     torvik_home_pts, torvik_away_pts, torvik_margin,
                     torvik_win_prob, actual_margin)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                """, (
                    game_id, season,
                    f"{game_date[:4]}-{game_date[4:6]}-{game_date[6:]}",
                    home_team, away_team,
                    float(pred_home), float(pred_away),
                    round(pred_margin, 2),
                    float(win_prob) if win_prob else None,
                    round(act_margin, 2) if act_margin else None,
                ))
                inserted += c.rowcount
            except Exception:
                continue

        conn.commit()
        return inserted

    except Exception as e:
        print(f"    ✗ Error: {e}")
        return 0



# ══════════════════════════════════════════════════════════════════════════════
# PART 4: HASLAMETRICS RATINGS
# ══════════════════════════════════════════════════════════════════════════════
# Haslametrics measures SHOT QUALITY DISTRIBUTION — fundamentally different
# from KenPom/Torvik efficiency metrics. Key unique columns:
#   Eff    = composite adjusted performance (like adj_em)
#   FTAR   = free throw attempt rate
#   FGAR   = field goal attempt rate  
#   MRAR   = mid-range attempt rate
#   NPAR   = non-paint, non-three attempt rate
#   NP%    = non-paint, non-three FG%
#   PPSt   = points per shot type
#   PPSC   = points per scoring chance
#   SCC%   = scoring chance conversion %
#   %3PA   = % of shots that are 3s
#   %MRA   = % of shots that are mid-range
#   %NPA   = % of shots that are non-paint
#   Prox   = proximity (avg shot distance)
#   AP%    = adjusted performance % (their overall metric, 1.0 = avg)
#
# Both time-dependent (recency weighted) and time-independent (full season)
# ratings available back to 2014-15.
# URL: haslametrics.com/ratings.php?yr=YYYY

def init_hasla_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS haslametrics_ratings (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            season        INTEGER NOT NULL,
            rating_type   TEXT NOT NULL,   -- 'td' (time-dependent) or 'ti' (time-independent)
            team          TEXT NOT NULL,
            wins          INTEGER,
            losses        INTEGER,
            eff           REAL,    -- composite adjusted performance
            ftar          REAL,    -- free throw attempt rate
            ft_pct        REAL,    -- free throw %
            fgar          REAL,    -- field goal attempt rate
            fg_pct        REAL,    -- FG%
            three_par     REAL,    -- 3pt attempt rate
            three_pct     REAL,    -- 3pt %
            mrar          REAL,    -- mid-range attempt rate
            mr_pct        REAL,    -- mid-range %
            npar          REAL,    -- non-paint non-3 attempt rate
            np_pct        REAL,    -- non-paint non-3 %
            ppst          REAL,    -- points per shot type
            ppsc          REAL,    -- points per scoring chance
            scc_pct       REAL,    -- scoring chance conversion %
            pct_3pa       REAL,    -- % of shots that are 3s
            pct_mra       REAL,    -- % of shots that are mid-range
            pct_npa       REAL,    -- % of shots that are non-paint
            prox          REAL,    -- proximity (avg shot distance)
            ap_pct        REAL,    -- adjusted performance % (primary metric)
            UNIQUE(season, rating_type, team)
        )
    """)
    conn.commit()


def pull_haslametrics(conn, season, rating_type='td'):
    """
    Pull Haslametrics ratings for a given season.
    rating_type: 'td' = time-dependent (recency weighted)
                 'ti' = time-independent (equal weight all games)
    URL pattern: haslametrics.com/ratings.php?yr=YYYY
    (time-independent: haslametrics.com/ratings_ti.php?yr=YYYY)
    """
    if rating_type == 'td':
        url = f"https://haslametrics.com/ratings.php?yr={season}"
    else:
        url = f"https://haslametrics.com/ratings_ti.php?yr={season}"

    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            print(f"    ✗ HTTP {r.status_code} for {url}")
            return 0

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(r.text, "html.parser")

        # Find the main ratings table
        tables = soup.find_all("table")
        if not tables:
            return 0

        # Find table with team data (has many rows)
        main_table = None
        for t in tables:
            rows = t.find_all("tr")
            if len(rows) > 50:
                main_table = t
                break

        if not main_table:
            return 0

        c = conn.cursor()
        inserted = 0

        for row in main_table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 20:
                continue

            try:
                # Column order: Rk, Team(w-l), Eff, FTAR, FT%, FGAR, FG%,
                #               3PAR, 3P%, MRAR, MR%, NPAR, NP%, PPSt, PPSC,
                #               SCC%, %3PA, %MRA, %NPA, Prox, AP%
                team_cell = cells[1].get_text(strip=True)
                # Extract team name and record from "Duke (29-2)" format
                import re
                m = re.match(r"(.+?)\s*\((\d+)-(\d+)\)", team_cell)
                if m:
                    team  = m.group(1).strip()
                    wins  = int(m.group(2))
                    losses= int(m.group(3))
                else:
                    team  = team_cell
                    wins  = losses = None

                def f(idx):
                    try: return float(cells[idx].get_text(strip=True))
                    except: return None

                c.execute("""
                    INSERT OR IGNORE INTO haslametrics_ratings
                    (season, rating_type, team, wins, losses,
                     eff, ftar, ft_pct, fgar, fg_pct, three_par, three_pct,
                     mrar, mr_pct, npar, np_pct, ppst, ppsc, scc_pct,
                     pct_3pa, pct_mra, pct_npa, prox, ap_pct)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    season, rating_type, team, wins, losses,
                    f(2),  f(3),  f(4),  f(5),  f(6),
                    f(7),  f(8),  f(9),  f(10), f(11),
                    f(12), f(13), f(14), f(15), f(16),
                    f(17), f(18), f(19), f(20),
                ))
                inserted += c.rowcount
            except Exception:
                continue

        conn.commit()
        return inserted

    except Exception as e:
        print(f"    ✗ Error: {e}")
        return 0

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print(f"Database: {DB_PATH}\n")
    conn = sqlite3.connect(DB_PATH)
    init_tables(conn)

    # ── Part 1: Season-final ratings ─────────────────────────────────────────
    print("\n── Pulling season-final T-Rank ratings ──────────────────────")
    for season in SEASONS:
        n = pull_season_final_ratings(conn, season)
        print(f"  {season}: {n} teams inserted")
        time.sleep(SLEEP)

    # Check what we got
    existing = pd.read_sql(
        "SELECT season, snapshot_type, COUNT(*) as n FROM torvik_ratings "
        "GROUP BY season, snapshot_type ORDER BY season", conn
    )
    print("\n  Ratings summary:")
    print(existing.to_string(index=False))

    # ── Part 2: Time machine weekly snapshots ────────────────────────────────
    print("\n── Pulling Time Machine weekly snapshots ────────────────────")
    print("  (This takes ~10-15 min — one request per week per season)")

    total_snapshots = 0
    for season in SEASONS:
        start, end = SEASON_DATES[season]
        dates = get_weekly_dates(start, end)
        season_inserted = 0

        for date_str in dates:
            # Skip if we already have this snapshot
            existing_check = pd.read_sql(
                "SELECT COUNT(*) as n FROM torvik_ratings "
                "WHERE season=? AND snapshot_date=? AND snapshot_type='weekly'",
                conn, params=(season, date_str)
            )
            if existing_check["n"].iloc[0] > 0:
                continue

            n = pull_time_machine_snapshot(conn, season, date_str)
            season_inserted += n
            time.sleep(SLEEP)

        total_snapshots += season_inserted
        print(f"  {season}: {season_inserted} new team-snapshots")

    print(f"  Total new time machine rows: {total_snapshots:,}")

    # ── Part 3: Game predictions ─────────────────────────────────────────────
    print("\n── Pulling Torvik game predictions ──────────────────────────")
    total_preds = 0
    for season in SEASONS:
        n = pull_torvik_game_predictions(conn, season)
        print(f"  {season}: {n} game predictions")
        total_preds += n
        time.sleep(SLEEP)

    print(f"\n  Total game predictions: {total_preds:,}")

    # ── Summary ──────────────────────────────────────────────────────────────
    print("\n── Final Summary ────────────────────────────────────────────")
    ratings_count = pd.read_sql("SELECT COUNT(*) as n FROM torvik_ratings", conn)["n"].iloc[0]
    preds_count   = pd.read_sql("SELECT COUNT(*) as n FROM torvik_game_preds", conn)["n"].iloc[0]
    print(f"  torvik_ratings rows    : {ratings_count:,}")
    print(f"  torvik_game_preds rows : {preds_count:,}")

    # Validate: compare Torvik predictions vs actual (should be ~8-9pt MAE)
    if preds_count > 0:
        val = pd.read_sql("""
            SELECT ROUND(AVG(ABS(torvik_margin - actual_margin)), 2) as torvik_mae,
                   COUNT(*) as n
            FROM torvik_game_preds
            WHERE actual_margin IS NOT NULL AND torvik_margin IS NOT NULL
        """, conn)
        if val["n"].iloc[0] > 0:
            print(f"  Torvik prediction MAE  : {val['torvik_mae'].iloc[0]} pts "
                  f"(n={val['n'].iloc[0]:,}) — market benchmark is ~9.0")

    # ── Part 4: Haslametrics ratings ────────────────────────────────────────
    print("\n── Pulling Haslametrics ratings ─────────────────────────────")
    init_hasla_table(conn)
    total_hasla = 0
    for season in SEASONS:
        for rtype in ['td', 'ti']:
            n = pull_haslametrics(conn, season, rtype)
            total_hasla += n
            time.sleep(SLEEP)
        print(f"  {season}: td+ti rows inserted")
    hasla_count = pd.read_sql("SELECT COUNT(*) as n FROM haslametrics_ratings", conn)["n"].iloc[0]
    print(f"  Total haslametrics_ratings rows: {hasla_count:,}")

    conn.close()
    print("\n✓ Done — run 04_feature_engineering.py next")


if __name__ == "__main__":
    main()
