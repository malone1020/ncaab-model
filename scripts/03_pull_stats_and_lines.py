"""
Script 03: Pull Game Stats + Betting Lines from CBBD API
=========================================================
Pulls two endpoints for each season:
  1. /games/teams  — per-game four-factor box scores
  2. /lines        — historical spreads and totals

Usage:
    python scripts/03_pull_stats_and_lines.py
"""

import sqlite3
import requests
import time
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR     = Path(__file__).resolve().parent.parent
DB_PATH      = BASE_DIR / "data" / "basketball.db"
API_KEY      = os.getenv("CBBD_API_KEY")
BASE_URL     = "https://api.collegebasketballdata.com"
START_SEASON = 2016
END_SEASON   = 2026
SLEEP_SEC    = 1.5
HEADERS      = {"Authorization": f"Bearer {API_KEY}"}
SEASON_TYPES = ["regular", "postseason"]


# ══════════════════════════════════════════════════════════════════════════════
# SCHEMA
# ══════════════════════════════════════════════════════════════════════════════

def ensure_schema(conn):
    c = conn.cursor()

    c.execute("DROP TABLE IF EXISTS game_team_stats")
    c.execute("""
        CREATE TABLE IF NOT EXISTS game_team_stats (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id         INTEGER,
            season          INTEGER,
            game_date       TEXT,
            team            TEXT,
            opponent        TEXT,
            is_home         INTEGER,
            neutral_site    INTEGER,
            conf_game       INTEGER,
            season_type     TEXT,
            pace            REAL,
            possessions     INTEGER,
            game_minutes    INTEGER,
            true_shooting   REAL,
            points          INTEGER,
            assists         INTEGER,
            steals          INTEGER,
            blocks          INTEGER,
            turnovers       INTEGER,
            total_rebounds  INTEGER,
            off_rebounds    INTEGER,
            def_rebounds    INTEGER,
            fouls           INTEGER,
            fg_made         INTEGER,
            fg_att          INTEGER,
            fg_pct          REAL,
            three_made      INTEGER,
            three_att       INTEGER,
            three_pct       REAL,
            ft_made         INTEGER,
            ft_att          INTEGER,
            ft_pct          REAL,
            efg_pct         REAL,
            tov_pct         REAL,
            orb_pct         REAL,
            ft_rate         REAL,
            rating          REAL,
            game_score      REAL,
            UNIQUE(game_id, team)
        )
    """)

    c.execute("DROP TABLE IF EXISTS game_lines")
    c.execute("""
        CREATE TABLE IF NOT EXISTS game_lines (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id         INTEGER,
            season          INTEGER,
            game_date       TEXT,
            home_team       TEXT,
            away_team       TEXT,
            home_score      INTEGER,
            away_score      INTEGER,
            provider        TEXT,
            spread          REAL,
            over_under      REAL,
            home_moneyline  INTEGER,
            away_moneyline  INTEGER,
            spread_open     REAL,
            over_under_open REAL,
            home_margin     INTEGER,
            home_covered    INTEGER,
            went_over       INTEGER,
            UNIQUE(game_id, provider)
        )
    """)

    conn.commit()
    print("✓ Tables created")


# ══════════════════════════════════════════════════════════════════════════════
# API
# ══════════════════════════════════════════════════════════════════════════════

def api_get(endpoint, params):
    url = f"{BASE_URL}{endpoint}"
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        if r.status_code == 200:
            return r.json()
        elif r.status_code == 429:
            print("  ⚠  Rate limited — sleeping 30s")
            time.sleep(30)
            r = requests.get(url, headers=HEADERS, params=params, timeout=30)
            return r.json() if r.status_code == 200 else []
        else:
            print(f"  ⚠  {endpoint} {r.status_code}: {r.text[:100]}")
            return []
    except Exception as e:
        print(f"  ⚠  {e}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
# GAME TEAM STATS
# ══════════════════════════════════════════════════════════════════════════════

def parse_team_stats(g):
    ts  = g.get("teamStats") or {}

    # Every sub-field is a nested dict — extract explicitly
    pts = ts.get("points")               or {}
    fg  = ts.get("fieldGoals")           or {}
    tp  = ts.get("threePointFieldGoals") or {}
    ft  = ts.get("freeThrows")           or {}
    rb  = ts.get("rebounds")             or {}
    tov = ts.get("turnovers")            or {}
    fls = ts.get("fouls")                or {}
    ff  = ts.get("fourFactors")          or {}  # EFG, FTR, TOV%, ORB% pre-computed

    return {
        "game_id"       : g.get("gameId"),
        "season"        : g.get("season"),
        "game_date"     : (g.get("startDate") or "")[:10],
        "team"          : g.get("team"),
        "opponent"      : g.get("opponent"),
        "is_home"       : 1 if g.get("isHome") else 0,
        "neutral_site"  : 1 if g.get("neutralSite") else 0,
        "conf_game"     : 1 if g.get("conferenceGame") else 0,
        "season_type"   : g.get("seasonType"),
        "pace"          : g.get("pace"),
        "possessions"   : g.get("possessions"),
        "game_minutes"  : g.get("gameMinutes"),
        "true_shooting" : g.get("trueShooting"),
        "points"        : pts.get("total"),
        "assists"       : ts.get("assists"),
        "steals"        : ts.get("steals"),
        "blocks"        : ts.get("blocks"),
        "turnovers"     : tov.get("total"),
        "total_rebounds": rb.get("total"),
        "off_rebounds"  : rb.get("offensive"),
        "def_rebounds"  : rb.get("defensive"),
        "fouls"         : fls.get("total"),
        "fg_made"       : fg.get("made"),
        "fg_att"        : fg.get("attempted"),
        "fg_pct"        : fg.get("pct"),
        "three_made"    : tp.get("made"),
        "three_att"     : tp.get("attempted"),
        "three_pct"     : tp.get("pct"),
        "ft_made"       : ft.get("made"),
        "ft_att"        : ft.get("attempted"),
        "ft_pct"        : ft.get("pct"),
        "efg_pct"       : ff.get("effectiveFieldGoalPct"),
        "tov_pct"       : ff.get("turnoverRatio"),
        "orb_pct"       : ff.get("offensiveReboundPct"),
        "ft_rate"       : ff.get("freeThrowRate"),
        "rating"        : g.get("rating"),
        "game_score"    : g.get("gameScore"),
    }


def insert_team_stats(conn, rows):
    c = conn.cursor()
    inserted = skipped = 0
    cols = [
        "game_id", "season", "game_date", "team", "opponent",
        "is_home", "neutral_site", "conf_game", "season_type",
        "pace", "possessions", "game_minutes", "true_shooting",
        "points", "assists", "steals", "blocks", "turnovers",
        "total_rebounds", "off_rebounds", "def_rebounds", "fouls",
        "fg_made", "fg_att", "fg_pct",
        "three_made", "three_att", "three_pct",
        "ft_made", "ft_att", "ft_pct",
        "efg_pct", "tov_pct", "orb_pct", "ft_rate",
        "rating", "game_score",
    ]
    for row in rows:
        try:
            c.execute(
                f"INSERT OR IGNORE INTO game_team_stats ({', '.join(cols)}) "
                f"VALUES ({', '.join([':'+col for col in cols])})",
                row
            )
            if c.rowcount:
                inserted += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"  Stats insert error: {e} | points type: {type(row.get('points'))}")
    conn.commit()
    return inserted, skipped


# ══════════════════════════════════════════════════════════════════════════════
# BETTING LINES
# ══════════════════════════════════════════════════════════════════════════════

def parse_lines(g):
    rows = []
    home_score = g.get("homeScore")
    away_score = g.get("awayScore")
    game_date  = (g.get("startDate") or "")[:10]
    home_margin = (home_score - away_score) if (home_score is not None and away_score is not None) else None

    for line in (g.get("lines") or []):
        spread = line.get("spread")
        ou     = line.get("overUnder")

        home_covered = None
        if home_margin is not None and spread is not None:
            home_covered = 1 if (home_margin + spread) > 0 else 0

        went_over = None
        if home_score is not None and away_score is not None and ou is not None:
            went_over = 1 if (home_score + away_score) > ou else 0

        rows.append({
            "game_id"        : g.get("gameId"),
            "season"         : g.get("season"),
            "game_date"      : game_date,
            "home_team"      : g.get("homeTeam"),
            "away_team"      : g.get("awayTeam"),
            "home_score"     : home_score,
            "away_score"     : away_score,
            "provider"       : line.get("provider"),
            "spread"         : spread,
            "over_under"     : ou,
            "home_moneyline" : line.get("homeMoneyline"),
            "away_moneyline" : line.get("awayMoneyline"),
            "spread_open"    : line.get("spreadOpen"),
            "over_under_open": line.get("overUnderOpen"),
            "home_margin"    : home_margin,
            "home_covered"   : home_covered,
            "went_over"      : went_over,
        })
    return rows


def insert_lines(conn, rows):
    c = conn.cursor()
    inserted = skipped = 0
    cols = [
        "game_id", "season", "game_date",
        "home_team", "away_team", "home_score", "away_score",
        "provider", "spread", "over_under",
        "home_moneyline", "away_moneyline",
        "spread_open", "over_under_open",
        "home_margin", "home_covered", "went_over",
    ]
    for row in rows:
        try:
            c.execute(
                f"INSERT OR IGNORE INTO game_lines ({', '.join(cols)}) "
                f"VALUES ({', '.join([':'+col for col in cols])})",
                row
            )
            if c.rowcount:
                inserted += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"  Lines insert error: {e}")
    conn.commit()
    return inserted, skipped


# ══════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

def print_summary(conn):
    c = conn.cursor()

    print("\n── game_team_stats ───────────────────────────────────")
    c.execute("SELECT season, COUNT(*) FROM game_team_stats GROUP BY season ORDER BY season")
    for r in c.fetchall():
        print(f"   {r[0]}: {r[1]:,} team-game rows")

    print("\n── game_lines ────────────────────────────────────────")
    c.execute("""
        SELECT season, COUNT(*) as lines, COUNT(DISTINCT game_id) as games,
               SUM(home_covered) as home_cvr, SUM(went_over) as overs
        FROM game_lines GROUP BY season ORDER BY season
    """)
    print(f"   {'Season':<8} {'Lines':>7} {'Games':>7} {'HomeCvr':>9} {'Overs':>7}")
    for r in c.fetchall():
        print(f"   {r[0]:<8} {r[1]:>7} {r[2]:>7} {str(r[3]):>9} {str(r[4]):>7}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    if not API_KEY:
        print("⚠  CBBD_API_KEY not found in .env")
        return

    print(f"Database : {DB_PATH}")
    print(f"Seasons  : {START_SEASON}–{END_SEASON}")
    print()

    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    ensure_schema(conn)
    print()

    for season in range(START_SEASON, END_SEASON + 1):
        print(f"── Season {season} ──────────────────────────────────────")

        for stype in SEASON_TYPES:
            # Game team stats
            raw    = api_get("/games/teams", {"season": season, "seasonType": stype})
            parsed = [parse_team_stats(g) for g in raw]
            ins, skp = insert_team_stats(conn, parsed)
            print(f"  stats  {stype:<12} fetched: {len(raw):5,}  inserted: {ins:5,}  skipped: {skp:4,}")
            time.sleep(SLEEP_SEC)

            # Betting lines
            raw      = api_get("/lines", {"season": season, "seasonType": stype})
            line_rows = []
            for g in raw:
                line_rows.extend(parse_lines(g))
            ins, skp = insert_lines(conn, line_rows)
            print(f"  lines  {stype:<12} fetched: {len(raw):5,} games  inserted: {ins:5,}  skipped: {skp:4,}")
            time.sleep(SLEEP_SEC)

        print()

    print_summary(conn)
    conn.close()
    print("\n✓ Done")


if __name__ == "__main__":
    main()
