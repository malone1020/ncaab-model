# REPLACEMENT for parse_team_stats function in 03_pull_stats_and_lines.py
# Replace the entire parse_team_stats function with this

def parse_team_stats(g: dict) -> dict:
    """Parse one row from /games/teams response."""
    ts  = g.get("teamStats") or {}

    # All nested dicts - extract explicitly
    pts  = ts.get("points")      or {}
    fg   = ts.get("fieldGoals")  or {}
    tp   = ts.get("threePointFieldGoals") or {}
    ft   = ts.get("freeThrows")  or {}
    rb   = ts.get("rebounds")    or {}
    tov  = ts.get("turnovers")   or {}
    fls  = ts.get("fouls")       or {}
    ff   = ts.get("fourFactors") or {}   # pre-computed four factors!

    game_date = (g.get("startDate") or "")[:10]

    return {
        "game_id"       : g.get("gameId"),
        "season"        : g.get("season"),
        "game_date"     : game_date,
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
        # Points - extract from nested dict
        "points"        : pts.get("total"),
        # Counting stats - flat
        "assists"       : ts.get("assists"),
        "steals"        : ts.get("steals"),
        "blocks"        : ts.get("blocks"),
        # Turnovers - nested
        "turnovers"     : tov.get("total"),
        # Rebounds - nested
        "total_rebounds": rb.get("total"),
        "off_rebounds"  : rb.get("offensive"),
        "def_rebounds"  : rb.get("defensive"),
        # Fouls - nested
        "fouls"         : fls.get("total"),
        # Field goals - nested
        "fg_made"       : fg.get("made"),
        "fg_att"        : fg.get("attempted"),
        "fg_pct"        : fg.get("pct"),
        # 3-pointers - nested
        "three_made"    : tp.get("made"),
        "three_att"     : tp.get("attempted"),
        "three_pct"     : tp.get("pct"),
        # Free throws - nested
        "ft_made"       : ft.get("made"),
        "ft_att"        : ft.get("attempted"),
        "ft_pct"        : ft.get("pct"),
        # Four factors - pre-computed by CBBD!
        "efg_pct"       : ff.get("effectiveFieldGoalPct"),
        "tov_pct"       : ff.get("turnoverRatio"),
        "orb_pct"       : ff.get("offensiveReboundPct"),
        "ft_rate"       : ff.get("freeThrowRate"),
        # Ratings
        "rating"        : g.get("rating"),
        "game_score"    : g.get("gameScore"),
    }
