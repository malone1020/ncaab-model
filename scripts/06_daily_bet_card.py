"""
Script 06: Daily Bet Card Generator
=====================================
Pulls today's games and lines, builds features for each matchup,
runs the trained model, and outputs a ranked bet card.

Run daily during the season:
    python scripts/06_daily_bet_card.py

Optional: specify a date
    python scripts/06_daily_bet_card.py --date 2026-03-15
"""

import sqlite3
import requests
import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import date
from dotenv import load_dotenv
import os
import warnings
warnings.filterwarnings("ignore")

try:
    import xgboost as xgb
except ImportError:
    print("pip install xgboost")
    raise

load_dotenv()

BASE_DIR   = Path(__file__).resolve().parent.parent
DB_PATH    = BASE_DIR / "data" / "basketball.db"
MODEL_PATH = BASE_DIR / "models" / "margin_model.json"
API_KEY    = os.getenv("CBBD_API_KEY")
BASE_URL   = "https://api.collegebasketballdata.com"
HEADERS    = {"Authorization": f"Bearer {API_KEY}"}

EDGE_THRESHOLD = 3.0   # minimum points of edge to flag a bet
ROLLING_WINDOW = 10

FEATURE_COLS = [
    "em_gap", "o_gap", "d_gap", "tempo_gap",
    "efg_gap", "tov_gap", "orb_gap", "ftr_gap",
    "pts_gap", "margin_gap",
    "def_efg_gap", "def_tov_gap", "def_orb_gap",
    "home_roll_efg", "home_roll_tov", "home_roll_orb", "home_roll_ftr",
    "home_roll_pts", "home_roll_margin", "home_roll_pace",
    "away_roll_efg", "away_roll_tov", "away_roll_orb", "away_roll_ftr",
    "away_roll_pts", "away_roll_margin", "away_roll_pace",
    "neutral_site", "conf_game",
]


# ══════════════════════════════════════════════════════════════════════════════
# FETCH TODAY'S GAMES WITH LINES
# ══════════════════════════════════════════════════════════════════════════════

def get_todays_games(target_date: str):
    """Pull games scheduled for target_date from CBBD /lines endpoint."""
    # CBBD uses season parameter — infer season from date
    year = int(target_date[:4])
    month = int(target_date[5:7])
    season = year if month >= 10 else year - 1  # CBB season starts in Nov

    r = requests.get(f"{BASE_URL}/lines", headers=HEADERS,
                     params={"season": season + 1, "seasonType": "regular"},
                     timeout=30)
    if r.status_code != 200:
        print(f"⚠  Lines API returned {r.status_code}")
        return []

    games = r.json()
    # Filter to target date
    todays = [g for g in games
              if g.get("startDate", "")[:10] == target_date
              and g.get("lines")]
    print(f"Found {len(todays)} games with lines on {target_date}")
    return todays


# ══════════════════════════════════════════════════════════════════════════════
# BUILD ROLLING STATS FOR A TEAM (from DB)
# ══════════════════════════════════════════════════════════════════════════════

def get_team_rolling(conn, team: str, before_date: str, window: int = 10) -> dict:
    """Get rolling average of last N games for a team before a given date."""
    df = pd.read_sql("""
        SELECT game_date, efg_pct, tov_pct, orb_pct, ft_rate,
               points, pace, game_id, opponent
        FROM game_team_stats
        WHERE team = ?
          AND game_date < ?
          AND efg_pct IS NOT NULL
        ORDER BY game_date DESC
        LIMIT ?
    """, conn, params=(team, before_date, window))

    if len(df) < 3:
        return {}

    # Get opponent stats for same games (for defensive metrics)
    game_ids = df["game_id"].tolist()
    placeholders = ",".join("?" * len(game_ids))
    opp_df = pd.read_sql(f"""
        SELECT g.game_id, g.efg_pct as opp_efg, g.tov_pct as opp_tov,
               g.orb_pct as opp_orb, g.points as opp_points
        FROM game_team_stats g
        WHERE g.game_id IN ({placeholders})
          AND g.team != ?
    """, conn, params=game_ids + [team])

    merged = df.merge(opp_df, on="game_id", how="left")
    merged["margin"] = merged["points"] - merged["opp_points"]

    return {
        "roll_efg"     : merged["efg_pct"].mean(),
        "roll_tov"     : merged["tov_pct"].mean(),
        "roll_orb"     : merged["orb_pct"].mean(),
        "roll_ftr"     : merged["ft_rate"].mean(),
        "roll_pts"     : merged["points"].mean(),
        "roll_margin"  : merged["margin"].mean(),
        "roll_pace"    : merged["pace"].mean(),
        "roll_opp_efg" : merged["opp_efg"].mean(),
        "roll_opp_tov" : merged["opp_tov"].mean(),
        "roll_opp_orb" : merged["opp_orb"].mean(),
        "n_games"      : len(df),
    }


# ══════════════════════════════════════════════════════════════════════════════
# GET KENPOM FOR TEAM/SEASON
# ══════════════════════════════════════════════════════════════════════════════

def get_kenpom(conn, team: str, season: int) -> dict:
    c = conn.cursor()
    c.execute("""
        SELECT adj_em, adj_o, adj_d, adj_t
        FROM kenpom_ratings
        WHERE team = ? AND season = ? AND snapshot_type = 'final'
        LIMIT 1
    """, (team, season))
    row = c.fetchone()
    if row:
        return {"adj_em": row[0], "adj_o": row[1],
                "adj_d": row[2],  "adj_t": row[3]}
    return {}


# ══════════════════════════════════════════════════════════════════════════════
# BUILD FEATURES FOR ONE GAME
# ══════════════════════════════════════════════════════════════════════════════

def build_game_features(conn, game: dict, target_date: str, season: int) -> dict:
    home = game["homeTeam"]
    away = game["awayTeam"]

    home_roll = get_team_rolling(conn, home, target_date)
    away_roll = get_team_rolling(conn, away, target_date)
    home_kp   = get_kenpom(conn, home, season)
    away_kp   = get_kenpom(conn, away, season)

    # Consensus line
    lines = game.get("lines") or []
    if not lines:
        return {}
    spread = np.mean([l["spread"] for l in lines if l.get("spread") is not None])
    over_under = np.mean([l["overUnder"] for l in lines if l.get("overUnder") is not None])
    home_ml = np.mean([l["homeMoneyline"] for l in lines if l.get("homeMoneyline") is not None])
    away_ml = np.mean([l["awayMoneyline"] for l in lines if l.get("awayMoneyline") is not None])

    if not home_roll or not away_roll:
        return {}

    row = {
        "game_id"      : game.get("gameId"),
        "home_team"    : home,
        "away_team"    : away,
        "spread"       : spread,
        "over_under"   : over_under,
        "home_moneyline": home_ml,
        "away_moneyline": away_ml,
        "neutral_site" : 0,
        "conf_game"    : 0,
        "n_providers"  : len(lines),

        # KenPom gaps
        "em_gap"   : home_kp.get("adj_em", 0) - away_kp.get("adj_em", 0),
        "o_gap"    : home_kp.get("adj_o", 0)  - away_kp.get("adj_o", 0),
        "d_gap"    : away_kp.get("adj_d", 0)  - home_kp.get("adj_d", 0),
        "tempo_gap": home_kp.get("adj_t", 0)  - away_kp.get("adj_t", 0),

        # Home rolling
        "home_roll_efg"    : home_roll.get("roll_efg"),
        "home_roll_tov"    : home_roll.get("roll_tov"),
        "home_roll_orb"    : home_roll.get("roll_orb"),
        "home_roll_ftr"    : home_roll.get("roll_ftr"),
        "home_roll_pts"    : home_roll.get("roll_pts"),
        "home_roll_margin" : home_roll.get("roll_margin"),
        "home_roll_pace"   : home_roll.get("roll_pace"),
        "home_roll_opp_efg": home_roll.get("roll_opp_efg"),
        "home_roll_opp_tov": home_roll.get("roll_opp_tov"),
        "home_roll_opp_orb": home_roll.get("roll_opp_orb"),

        # Away rolling
        "away_roll_efg"    : away_roll.get("roll_efg"),
        "away_roll_tov"    : away_roll.get("roll_tov"),
        "away_roll_orb"    : away_roll.get("roll_orb"),
        "away_roll_ftr"    : away_roll.get("roll_ftr"),
        "away_roll_pts"    : away_roll.get("roll_pts"),
        "away_roll_margin" : away_roll.get("roll_margin"),
        "away_roll_pace"   : away_roll.get("roll_pace"),
        "away_roll_opp_efg": away_roll.get("roll_opp_efg"),
        "away_roll_opp_tov": away_roll.get("roll_opp_tov"),
        "away_roll_opp_orb": away_roll.get("roll_opp_orb"),
    }

    # Four-factor matchup gaps
    row["efg_gap"]     = row["home_roll_efg"]     - row["away_roll_efg"]
    row["tov_gap"]     = row["home_roll_tov"]     - row["away_roll_tov"]
    row["orb_gap"]     = row["home_roll_orb"]     - row["away_roll_orb"]
    row["ftr_gap"]     = row["home_roll_ftr"]     - row["away_roll_ftr"]
    row["pts_gap"]     = row["home_roll_pts"]     - row["away_roll_pts"]
    row["margin_gap"]  = row["home_roll_margin"]  - row["away_roll_margin"]
    row["def_efg_gap"] = row["away_roll_opp_efg"] - row["home_roll_opp_efg"]
    row["def_tov_gap"] = row["home_roll_opp_tov"] - row["away_roll_opp_tov"]
    row["def_orb_gap"] = row["away_roll_opp_orb"] - row["home_roll_opp_orb"]

    return row


# ══════════════════════════════════════════════════════════════════════════════
# FORMAT BET CARD
# ══════════════════════════════════════════════════════════════════════════════

def ml_to_implied_prob(ml):
    if ml is None or np.isnan(ml):
        return None
    if ml > 0:
        return 100 / (ml + 100)
    return abs(ml) / (abs(ml) + 100)


def format_bet_card(df, target_date):
    print(f"\n{'='*65}")
    print(f"  NCAAB BET CARD — {target_date}")
    print(f"{'='*65}")

    bets = df[df["has_bet"]].sort_values("abs_edge", ascending=False)

    if len(bets) == 0:
        print("  No bets meet the edge threshold today.")
        print(f"  ({len(df)} games analyzed, threshold: {EDGE_THRESHOLD} pts)")
        return

    print(f"  {len(bets)} bet(s) flagged from {len(df)} games analyzed")
    print(f"  Edge threshold: >{EDGE_THRESHOLD} pts\n")

    for _, row in bets.iterrows():
        side      = row["bet_side"]
        team      = row["home_team"] if side == "HOME" else row["away_team"]
        opp       = row["away_team"] if side == "HOME" else row["home_team"]
        spread    = row["spread"]
        # Convert spread to team-specific form
        if side == "HOME":
            team_spread = f"{spread:+.1f}" if spread else "PK"
        else:
            team_spread = f"{-spread:+.1f}" if spread else "PK"

        ml = row["home_moneyline"] if side == "HOME" else row["away_moneyline"]
        ml_str = f"{ml:+.0f}" if ml and not np.isnan(ml) else "N/A"

        print(f"  ► {team} {team_spread} (ML: {ml_str})")
        print(f"    vs {opp}")
        print(f"    Model: {row['predicted_margin']:+.1f}  "
              f"Market: {row['market_margin']:+.1f}  "
              f"Edge: {row['edge']:+.1f} pts")
        print(f"    O/U: {row['over_under']}  |  "
              f"KenPom gap: {row['em_gap']:+.1f}  |  "
              f"Providers: {int(row['n_providers'])}")
        print()

    print(f"{'='*65}")
    print(f"  All lines are consensus across {df['n_providers'].median():.0f}+ books")
    print(f"  Model trained on 2016-2025 walk-forward validation")
    print(f"  Bet sizing: use 1-2u. Never >3% of bankroll per game.")
    print(f"{'='*65}\n")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=str(date.today()),
                        help="Target date YYYY-MM-DD (default: today)")
    parser.add_argument("--threshold", type=float, default=EDGE_THRESHOLD,
                        help="Edge threshold in points (default: 3.0)")
    args = parser.parse_args()

    target_date = args.date
    threshold   = args.threshold

    # Infer season
    year  = int(target_date[:4])
    month = int(target_date[5:7])
    season = year if month >= 10 else year - 1

    print(f"Date   : {target_date}")
    print(f"Season : {season + 1}")
    print(f"Model  : {MODEL_PATH}")

    if not MODEL_PATH.exists():
        print(f"⚠  Model not found at {MODEL_PATH}")
        print("   Run: python scripts/05_train_model.py first")
        return

    # Load model
    model = xgb.XGBRegressor()
    model.load_model(str(MODEL_PATH))

    # Load DB
    conn = sqlite3.connect(DB_PATH)

    # Get today's games
    games = get_todays_games(target_date)
    if not games:
        print("No games found for today.")
        conn.close()
        return

    # Build features for each game
    print(f"\nBuilding features for {len(games)} games...")
    rows = []
    for g in games:
        feat = build_game_features(conn, g, target_date, season + 1)
        if feat:
            rows.append(feat)
        else:
            home = g.get("homeTeam", "?")
            away = g.get("awayTeam", "?")
            print(f"  ⚠  Skipped {away} @ {home} — insufficient data")

    conn.close()

    if not rows:
        print("No games had sufficient data for predictions.")
        return

    df = pd.DataFrame(rows)

    # Predict
    X = df[FEATURE_COLS].fillna(0)
    df["predicted_margin"] = model.predict(X)
    df["market_margin"]    = -df["spread"]
    df["edge"]             = df["predicted_margin"] - df["market_margin"]
    df["abs_edge"]         = df["edge"].abs()
    df["bet_home"]         = df["edge"] >  threshold
    df["bet_away"]         = df["edge"] < -threshold
    df["has_bet"]          = df["bet_home"] | df["bet_away"]
    df["bet_side"]         = np.where(df["bet_home"], "HOME",
                             np.where(df["bet_away"], "AWAY", "NONE"))

    # Print full card
    format_bet_card(df, target_date)

    # Save to CSV
    out_dir = BASE_DIR / "outputs"
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / f"bet_card_{target_date}.csv"
    df.to_csv(out_path, index=False)
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
