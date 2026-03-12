"""
Script 04: Feature Engineering (v2 — Torvik-primary)
======================================================
Builds game_features using the best available data at each point in time.

DATA HIERARCHY:
  1. Torvik Time Machine ratings — adj_o, adj_d, barthag, tempo AS OF game date
     Zero leakage. Better than end-of-season KenPom. Free.
  2. Torvik season-final ratings (prior season) — fallback
  3. KenPom end-of-season ratings (prior season) — supplemental cross-check
  4. CBBD four-factor rolling averages — opponent-quality adjusted via Torvik
  5. Torvik's own predicted margin — disagreement with Torvik IS the signal
  6. Per-venue home court advantage (leave-one-season-out)
  7. Rest / back-to-back differential

Run 03c_pull_torvik.py first to populate torvik_ratings and torvik_game_preds.
KenPom CSVs still used as supplemental signal but no longer primary.

Usage:
    python scripts/04_feature_engineering.py
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
import sys
import warnings
warnings.filterwarnings("ignore")

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR / "data" / "basketball.db"

sys.path.insert(0, str(Path(__file__).resolve().parent))
try:
    from team_name_map import normalize_for_kenpom
except ImportError:
    def normalize_for_kenpom(x): return x

ROLLING_WINDOW = 10
MIN_ROLL_GAMES = 3


def check_table_exists(conn, table):
    r = conn.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'"
    ).fetchone()
    return r is not None


# ── Load base game/lines data ──────────────────────────────────────────────

def load_lines(conn):
    df = pd.read_sql("""
        SELECT DISTINCT
            g.cbbd_id          AS game_id,
            g.season,
            g.game_date,
            g.home_team,
            g.away_team,
            g.home_score,
            g.away_score,
            g.neutral_site,
            g.conf_game,
            g.tournament,
            g.season_type,
            gl.spread,
            gl.over_under,
            gl.home_moneyline,
            gl.away_moneyline,
            (g.home_score - g.away_score)  AS actual_margin,
            CASE WHEN (g.home_score - g.away_score) > -gl.spread THEN 1.0
                 WHEN (g.home_score - g.away_score) = -gl.spread THEN 0.5
                 ELSE 0.0 END             AS home_covered
        FROM games g
        JOIN game_lines gl ON gl.game_id = g.cbbd_id
        WHERE g.home_score IS NOT NULL
          AND gl.spread    IS NOT NULL
          AND gl.provider  = 'consensus'
        ORDER BY g.game_date
    """, conn)
    df["game_date"] = pd.to_datetime(df["game_date"])
    return df


# ── Torvik ratings: primary team quality signal ────────────────────────────

def load_torvik_ratings(conn):
    if not check_table_exists(conn, "torvik_ratings"):
        print("  ⚠  torvik_ratings not found — run 03c_pull_torvik.py")
        return None, None

    df = pd.read_sql("""
        SELECT season, snapshot_date, snapshot_type, team,
               adj_o, adj_d, adj_t, barthag, adj_em,
               efg_o, efg_d, tov_o, tov_d, orb, drb, ftr_o, ftr_d
        FROM torvik_ratings ORDER BY snapshot_date
    """, conn)
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"], format="%Y%m%d",
                                          errors="coerce")
    weekly = df[df["snapshot_type"] == "weekly"].copy()
    finals = df[df["snapshot_type"] == "final"].copy()
    print(f"  Torvik: {len(weekly):,} weekly snapshots, {len(finals):,} final ratings")
    return weekly, finals


def get_torvik_for_team(weekly, finals, team, game_date, season):
    """Most recent weekly snapshot before game date, else prior-season final."""
    if weekly is not None:
        w = weekly[
            (weekly["team"] == team) &
            (weekly["season"] == season) &
            (weekly["snapshot_date"] < game_date)
        ]
        if len(w) > 0:
            return w.sort_values("snapshot_date").iloc[-1]

    if finals is not None:
        f = finals[
            (finals["team"] == team) &
            (finals["season"] == season - 1)
        ]
        if len(f) > 0:
            return f.iloc[-1]
    return None


def add_torvik_features(df, weekly, finals):
    print("  Adding Torvik rating features (primary signal)...")

    fields = ["adj_em","barthag","adj_o","adj_d","adj_t",
              "efg_o","efg_d","tov_o","tov_d","orb","ftr_o"]

    home_vals = {f: [] for f in fields}
    away_vals = {f: [] for f in fields}

    for _, row in df.iterrows():
        hr = get_torvik_for_team(weekly, finals, row["home_team"],
                                  row["game_date"], row["season"])
        ar = get_torvik_for_team(weekly, finals, row["away_team"],
                                  row["game_date"], row["season"])
        for f in fields:
            home_vals[f].append(hr[f] if hr is not None and pd.notna(hr.get(f)) else np.nan)
            away_vals[f].append(ar[f] if ar is not None and pd.notna(ar.get(f)) else np.nan)

    for f in fields:
        df[f"home_torvik_{f}"] = home_vals[f]
        df[f"away_torvik_{f}"] = away_vals[f]

    df["torvik_em_gap"]      = df["home_torvik_adj_em"] - df["away_torvik_adj_em"]
    df["torvik_barthag_gap"] = df["home_torvik_barthag"] - df["away_torvik_barthag"]
    df["torvik_o_gap"]       = df["home_torvik_adj_o"]  - df["away_torvik_adj_o"]
    df["torvik_d_gap"]       = df["home_torvik_adj_d"]  - df["away_torvik_adj_d"]
    df["torvik_tempo_gap"]   = df["home_torvik_adj_t"]  - df["away_torvik_adj_t"]
    df["torvik_efg_o_gap"]   = df["home_torvik_efg_o"]  - df["away_torvik_efg_o"]
    df["torvik_efg_d_gap"]   = df["home_torvik_efg_d"]  - df["away_torvik_efg_d"]
    df["torvik_tov_gap"]     = df["home_torvik_tov_o"]  - df["away_torvik_tov_o"]
    df["torvik_orb_gap"]     = df["home_torvik_orb"]    - df["away_torvik_orb"]

    h = df["home_torvik_adj_em"].notna().sum()
    a = df["away_torvik_adj_em"].notna().sum()
    print(f"  Torvik match rate: home={h:,}/{len(df):,}  away={a:,}/{len(df):,}")
    return df


# ── KenPom: supplemental signal ───────────────────────────────────────────

def add_kenpom_features(df, conn):
    print("  Adding KenPom features (supplemental)...")

    kenpom = pd.read_sql("""
        SELECT season, team, adj_em, adj_o, adj_d, adj_t
        FROM kenpom_ratings WHERE snapshot_type = 'final'
    """, conn)

    df = df.copy()
    df["home_kp_norm"]   = df["home_team"].apply(normalize_for_kenpom)
    df["away_kp_norm"]   = df["away_team"].apply(normalize_for_kenpom)
    df["kenpom_season"]  = df["season"] - 1
    kenpom["team_norm"]  = kenpom["team"].apply(normalize_for_kenpom)

    kp = kenpom.rename(columns={
        "adj_em": "kp_em", "adj_o": "kp_o", "adj_d": "kp_d", "adj_t": "kp_t",
        "team_norm": "home_kp_norm", "season": "kenpom_season"
    })
    df = df.merge(kp[["kenpom_season","home_kp_norm","kp_em","kp_o","kp_d","kp_t"]],
                  on=["kenpom_season","home_kp_norm"], how="left")
    df.rename(columns={"kp_em":"home_kp_em","kp_o":"home_kp_o",
                        "kp_d":"home_kp_d","kp_t":"home_kp_t"}, inplace=True)

    kp2 = kenpom.rename(columns={
        "adj_em": "kp_em", "adj_o": "kp_o", "adj_d": "kp_d", "adj_t": "kp_t",
        "team_norm": "away_kp_norm", "season": "kenpom_season"
    })
    df = df.merge(kp2[["kenpom_season","away_kp_norm","kp_em","kp_o","kp_d","kp_t"]],
                  on=["kenpom_season","away_kp_norm"], how="left")
    df.rename(columns={"kp_em":"away_kp_em","kp_o":"away_kp_o",
                        "kp_d":"away_kp_d","kp_t":"away_kp_t"}, inplace=True)

    df["kp_em_gap"] = df["home_kp_em"] - df["away_kp_em"]
    # Backwards compat aliases
    df["em_gap"]    = df["kp_em_gap"]
    df["o_gap"]     = df["home_kp_o"] - df["away_kp_o"]
    df["d_gap"]     = df["home_kp_d"] - df["away_kp_d"]
    df["tempo_gap"] = df["home_kp_t"] - df["away_kp_t"]

    for col in ["home_kp_norm","away_kp_norm","kenpom_season"]:
        df.drop(columns=[col], inplace=True, errors="ignore")
    return df


# ── Rolling four-factor stats, opponent-quality adjusted ──────────────────

def add_rolling_features(df, conn, finals):
    print(f"  Computing rolling {ROLLING_WINDOW}-game stats (opponent-quality adjusted)...")

    stats = pd.read_sql("""
        SELECT s.game_id, s.season, s.game_date, s.team, s.opponent,
               s.is_home, s.points, s.efg_pct, s.tov_pct, s.orb_pct,
               s.ft_rate, s.pace
        FROM game_team_stats s WHERE s.points IS NOT NULL
        ORDER BY s.game_date
    """, conn)
    stats["game_date"] = pd.to_datetime(stats["game_date"])

    # Opponent points for margin
    opp = stats[["game_id","team","points"]].rename(
        columns={"team":"opponent","points":"opp_pts"})
    stats = stats.merge(opp, on=["game_id","opponent"], how="left")
    stats["margin"] = stats["points"] - stats["opp_pts"]

    # Opponent quality weights from Torvik season-final ratings
    if finals is not None:
        opp_em = finals[["team","season","adj_em"]].copy()
        opp_em["opp_season"] = opp_em["season"]  # prior season
        stats["opp_season"] = stats["season"] - 1
        stats = stats.merge(
            opp_em[["team","opp_season","adj_em"]].rename(
                columns={"team":"opponent","opp_season":"opp_season","adj_em":"opp_adj_em"}),
            on=["opponent","opp_season"], how="left"
        )
        em_mean = stats["opp_adj_em"].mean()
        em_std  = stats["opp_adj_em"].std()
        stats["opp_weight"] = np.where(
            stats["opp_adj_em"].notna(),
            (1.0 + (stats["opp_adj_em"] - em_mean) / (em_std * 2)).clip(0.25, 2.0),
            1.0
        )
    else:
        stats["opp_adj_em"] = np.nan
        stats["opp_weight"] = 1.0

    stats["wt_margin"] = stats["margin"] * stats["opp_weight"]

    all_rolled = []
    for team, grp in stats.groupby("team"):
        grp = grp.sort_values("game_date").reset_index(drop=True)
        n = len(grp)
        res = {k: np.full(n, np.nan) for k in [
            "roll_adj_margin","roll_efg","roll_tov","roll_orb",
            "roll_ftr","roll_pts","roll_margin","roll_pace","roll_sos"
        ]}

        wm   = grp["wt_margin"].values
        w    = grp["opp_weight"].values
        efg  = grp["efg_pct"].values
        tov  = grp["tov_pct"].values
        orb  = grp["orb_pct"].values
        ftr  = grp["ft_rate"].values
        pts  = grp["points"].values
        mg   = grp["margin"].values
        pace = grp["pace"].values
        em   = grp["opp_adj_em"].values

        for i in range(1, n):
            s = max(0, i - ROLLING_WINDOW)
            valid = ~np.isnan(mg[s:i])
            if valid.sum() < MIN_ROLL_GAMES:
                continue
            ws = w[s:i][valid].sum()
            res["roll_adj_margin"][i] = wm[s:i][valid].sum() / ws if ws > 0 else np.nanmean(mg[s:i][valid])
            res["roll_efg"][i]    = np.nanmean(efg[s:i][valid])
            res["roll_tov"][i]    = np.nanmean(tov[s:i][valid])
            res["roll_orb"][i]    = np.nanmean(orb[s:i][valid])
            res["roll_ftr"][i]    = np.nanmean(ftr[s:i][valid])
            res["roll_pts"][i]    = np.nanmean(pts[s:i][valid])
            res["roll_margin"][i] = np.nanmean(mg[s:i][valid])
            pv = pace[s:i]; pv = pv[~np.isnan(pv)]
            res["roll_pace"][i]   = np.nanmean(pv) if len(pv) > 0 else np.nan
            ev = em[s:i]; ev = ev[~np.isnan(ev)]
            res["roll_sos"][i]    = np.nanmean(ev) if len(ev) > 0 else np.nan

        tmp = pd.DataFrame({"game_id": grp["game_id"].values,
                             "is_home": grp["is_home"].values, **res})
        all_rolled.append(tmp)

    rolled = pd.concat(all_rolled, ignore_index=True)

    hr = rolled[rolled["is_home"] == 1].rename(columns={
        "roll_adj_margin":"home_adj_margin","roll_efg":"home_roll_efg",
        "roll_tov":"home_roll_tov","roll_orb":"home_roll_orb",
        "roll_ftr":"home_roll_ftr","roll_pts":"home_roll_pts",
        "roll_margin":"home_roll_margin","roll_pace":"home_roll_pace",
        "roll_sos":"home_sos"}).drop(columns=["is_home"])

    ar = rolled[rolled["is_home"] == 0].rename(columns={
        "roll_adj_margin":"away_adj_margin","roll_efg":"away_roll_efg",
        "roll_tov":"away_roll_tov","roll_orb":"away_roll_orb",
        "roll_ftr":"away_roll_ftr","roll_pts":"away_roll_pts",
        "roll_margin":"away_roll_margin","roll_pace":"away_roll_pace",
        "roll_sos":"away_sos"}).drop(columns=["is_home"])

    df = df.merge(hr, on="game_id", how="left")
    df = df.merge(ar, on="game_id", how="left")

    df["adj_margin_gap"] = df["home_adj_margin"] - df["away_adj_margin"]
    df["margin_gap"]     = df["home_roll_margin"] - df["away_roll_margin"]
    df["efg_gap"]        = df["home_roll_efg"]    - df["away_roll_efg"]
    df["tov_gap"]        = df["home_roll_tov"]    - df["away_roll_tov"]
    df["orb_gap"]        = df["home_roll_orb"]    - df["away_roll_orb"]
    df["ftr_gap"]        = df["home_roll_ftr"]    - df["away_roll_ftr"]
    df["pts_gap"]        = df["home_roll_pts"]    - df["away_roll_pts"]
    df["sos_gap"]        = df["home_sos"]         - df["away_sos"]

    # Placeholder defensive gaps (need opponent efg_allowed separately)
    df["def_efg_gap"] = np.nan
    df["def_tov_gap"] = np.nan
    df["def_orb_gap"] = np.nan

    print(f"  Rolling stats: {df['adj_margin_gap'].notna().sum():,}/{len(df):,}")
    return df


# ── Torvik game predictions ────────────────────────────────────────────────

def add_torvik_predictions(df, conn):
    if not check_table_exists(conn, "torvik_game_preds"):
        print("  ⚠  torvik_game_preds not found — skipping")
        df["torvik_pred_margin"] = np.nan
        df["torvik_win_prob"]    = np.nan
        df["torvik_vs_spread"]   = np.nan
        return df

    preds = pd.read_sql("""
        SELECT game_id, torvik_margin AS torvik_pred_margin, torvik_win_prob
        FROM torvik_game_preds WHERE torvik_margin IS NOT NULL
    """, conn)

    df = df.merge(preds, on="game_id", how="left")
    if "torvik_pred_margin" not in df.columns:
        df["torvik_pred_margin"] = np.nan
        df["torvik_win_prob"]    = np.nan

    df["torvik_vs_spread"] = df["torvik_pred_margin"] - (-df["spread"])
    n = df["torvik_pred_margin"].notna().sum()
    print(f"  Torvik predictions: {n:,}/{len(df):,} matched")
    return df


# ── Home court advantage (leave-one-season-out) ───────────────────────────

def add_home_court_features(df):
    print("  Computing home court advantage...")
    home_games = df[df["neutral_site"] == 0].copy()
    league_avg_margin = home_games["actual_margin"].mean()
    league_avg_cover  = home_games["home_covered"].mean()
    shrink = 20
    all_hcv = []

    for test_season in sorted(df["season"].unique()):
        train = home_games[home_games["season"] != test_season]
        test  = df[df["season"] == test_season].copy()

        vs = train.groupby("home_team").agg(
            venue_games  = ("actual_margin","count"),
            venue_margin = ("actual_margin","mean"),
            venue_cover  = ("home_covered","mean"),
        ).reset_index()
        w = vs["venue_games"] / (vs["venue_games"] + shrink)
        vs["hcv_margin"] = w * vs["venue_margin"] + (1 - w) * league_avg_margin
        vs["hcv_cover"]  = w * vs["venue_cover"]  + (1 - w) * league_avg_cover
        vs["hcv_edge"]   = vs["hcv_cover"] - 0.5

        test = test.merge(vs[["home_team","venue_games","hcv_margin",
                               "hcv_cover","hcv_edge"]],
                          on="home_team", how="left")
        for col, default in [("hcv_margin", league_avg_margin),
                              ("hcv_cover",  league_avg_cover),
                              ("hcv_edge",   0), ("venue_games", 0)]:
            test[col] = test[col].fillna(default)
        all_hcv.append(test[["game_id","venue_games","hcv_margin",
                              "hcv_cover","hcv_edge"]])

    hcv = pd.concat(all_hcv, ignore_index=True)
    df = df.merge(hcv, on="game_id", how="left")
    df.rename(columns={"venue_games":"home_court_games","hcv_margin":"home_court_margin",
                        "hcv_cover":"home_court_cover","hcv_edge":"home_court_edge"},
              inplace=True)
    return df


# ── Rest and back-to-back ─────────────────────────────────────────────────

def add_rest_features(df, conn):
    print("  Computing rest/B2B features...")
    all_games = pd.read_sql(
        "SELECT cbbd_id, game_date, home_team, away_team FROM games ORDER BY game_date",
        conn)
    all_games["game_date"] = pd.to_datetime(all_games["game_date"])

    rows = []
    for _, r in all_games.iterrows():
        rows += [{"team": r["home_team"], "date": r["game_date"], "gid": r["cbbd_id"]},
                 {"team": r["away_team"], "date": r["game_date"], "gid": r["cbbd_id"]}]
    tg = pd.DataFrame(rows).sort_values(["team","date"])
    tg["prev"] = tg.groupby("team")["date"].shift(1)
    tg["rest"] = (tg["date"] - tg["prev"]).dt.days
    tg["b2b"]  = (tg["rest"] == 1).astype(int)

    hr = tg.rename(columns={"team":"home_team","gid":"game_id",
                              "rest":"home_rest","b2b":"home_b2b"})
    ar = tg.rename(columns={"team":"away_team","gid":"game_id",
                              "rest":"away_rest","b2b":"away_b2b"})
    df = df.merge(hr[["game_id","home_team","home_rest","home_b2b"]],
                  on=["game_id","home_team"], how="left")
    df = df.merge(ar[["game_id","away_team","away_rest","away_b2b"]],
                  on=["game_id","away_team"], how="left")
    df["rest_diff"] = df["home_rest"] - df["away_rest"]
    df["b2b_diff"]  = df["away_b2b"]  - df["home_b2b"]
    return df



# ── Haslametrics: shot quality distribution signal ────────────────────────

def add_haslametrics_features(df, conn):
    """
    Add Haslametrics shot quality features.
    Uses prior-season time-independent ratings as base signal (no leakage).
    Uses current-season time-dependent ratings when available (more current).

    Key metrics that KenPom/Torvik don't capture:
    - MRAR/MR%: mid-range reliance and efficiency
    - Prox: average shot distance (lower = more interior offense)
    - AP%: composite adjusted performance
    - SCC%: scoring chance conversion (shot quality)
    - PPSC: points per scoring chance
    """
    if not check_table_exists(conn, "haslametrics_ratings"):
        print("  ⚠  haslametrics_ratings not found — run 03c_pull_torvik.py")
        for col in ["home_hasla_ap", "away_hasla_ap", "hasla_ap_gap",
                    "hasla_prox_gap", "hasla_mrar_gap", "hasla_scc_gap"]:
            df[col] = np.nan
        return df

    print("  Adding Haslametrics shot quality features...")

    # Prefer current-season TD ratings; fall back to prior-season TI
    hasla = pd.read_sql("""
        SELECT season, rating_type, team,
               eff, ap_pct, ftar, ft_pct, fgar, fg_pct,
               three_par, three_pct, mrar, mr_pct, npar, np_pct,
               ppst, ppsc, scc_pct, pct_3pa, pct_mra, pct_npa, prox
        FROM haslametrics_ratings
    """, conn)

    td = hasla[hasla["rating_type"] == "td"]
    ti = hasla[hasla["rating_type"] == "ti"]

    def get_hasla(team, season):
        # Current season TD first
        row = td[(td["team"] == team) & (td["season"] == season)]
        if len(row) > 0:
            return row.iloc[-1]
        # Prior season TI fallback
        row = ti[(ti["team"] == team) & (ti["season"] == season - 1)]
        if len(row) > 0:
            return row.iloc[-1]
        return None

    fields = ["ap_pct","eff","ftar","ft_pct","mrar","mr_pct",
              "ppsc","scc_pct","prox","pct_3pa","pct_mra","three_pct"]

    home_vals = {f: [] for f in fields}
    away_vals = {f: [] for f in fields}

    for _, row in df.iterrows():
        hr = get_hasla(row["home_team"], row["season"])
        ar = get_hasla(row["away_team"], row["season"])
        for f in fields:
            home_vals[f].append(hr[f] if hr is not None and pd.notna(hr.get(f)) else np.nan)
            away_vals[f].append(ar[f] if ar is not None and pd.notna(ar.get(f)) else np.nan)

    for f in fields:
        df[f"home_hasla_{f}"] = home_vals[f]
        df[f"away_hasla_{f}"] = away_vals[f]

    # Key gap features
    df["hasla_ap_gap"]    = df["home_hasla_ap_pct"]   - df["away_hasla_ap_pct"]
    df["hasla_prox_gap"]  = df["home_hasla_prox"]      - df["away_hasla_prox"]
    df["hasla_mrar_gap"]  = df["home_hasla_mrar"]      - df["away_hasla_mrar"]
    df["hasla_scc_gap"]   = df["home_hasla_scc_pct"]   - df["away_hasla_scc_pct"]
    df["hasla_ppsc_gap"]  = df["home_hasla_ppsc"]      - df["away_hasla_ppsc"]
    df["hasla_3pt_gap"]   = df["home_hasla_pct_3pa"]   - df["away_hasla_pct_3pa"]
    df["hasla_ftar_gap"]  = df["home_hasla_ftar"]      - df["away_hasla_ftar"]

    h = df["home_hasla_ap_pct"].notna().sum()
    print(f"  Haslametrics match rate: {h:,}/{len(df):,}")
    return df

# ── Save and summarize ────────────────────────────────────────────────────

def save_and_summarize(df, conn):
    out = df.copy()
    out["game_date"] = out["game_date"].astype(str)
    out.to_sql("game_features", conn, if_exists="replace", index=False)
    print(f"\n  Saved {len(out):,} rows to game_features")

    print(f"\n── game_features summary ──────────────────────────────────────")
    print(f"  {'Season':>6}  {'Games':>6}  {'Torvik':>7}  {'KenPom':>7}  "
          f"{'Rolling':>8}  {'TvkPred':>8}  {'AvgMgn':>7}  {'HmCvr%':>7}")

    has_tv  = out["torvik_em_gap"].notna()
    has_kp  = out["em_gap"].notna()
    has_rl  = out["adj_margin_gap"].notna()
    has_tp  = out["torvik_pred_margin"].notna() if "torvik_pred_margin" in out.columns \
              else pd.Series(False, index=out.index)

    for season in sorted(out["season"].unique()):
        s = out[out["season"] == season]
        m = s["actual_margin"].mean()
        c = s["home_covered"].mean() * 100
        print(f"  {season:>6}  {len(s):>6,}  "
              f"{has_tv[out['season']==season].sum():>7,}  "
              f"{has_kp[out['season']==season].sum():>7,}  "
              f"{has_rl[out['season']==season].sum():>8,}  "
              f"{has_tp[out['season']==season].sum():>8,}  "
              f"{m:>+7.2f}  {c:>6.1f}%")

    fully = (has_tv | has_kp) & has_rl
    print(f"\n  Fully featured (ratings + rolling): {fully.sum():,}")


def main():
    print(f"Database: {DB_PATH}\n")
    conn = sqlite3.connect(DB_PATH)

    conn.execute("DROP TABLE IF EXISTS game_features")
    conn.commit()
    print("✓ game_features reset\n")

    print("Loading data...")
    df = load_lines(conn)
    print(f"  {len(df):,} games with spreads")

    weekly, finals = load_torvik_ratings(conn)

    print("\nBuilding features...")
    df = add_torvik_features(df, weekly, finals)
    df = add_kenpom_features(df, conn)
    df = add_rolling_features(df, conn, finals)
    df = add_torvik_predictions(df, conn)
    df = add_home_court_features(df)
    df = add_rest_features(df, conn)
    df = add_haslametrics_features(df, conn)

    print("\nSaving...")
    save_and_summarize(df, conn)
    conn.close()
    print("\n✓ Done — run 05_train_model.py next")


if __name__ == "__main__":
    main()
