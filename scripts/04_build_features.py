"""
04_build_features.py
====================
Build unified feature matrix from ALL sources (KenPom + Torvik + Haslametrics + rolling).
Every source gets represented. Backtest decides what's predictive.

Run: python scripts/04_build_features.py
"""

import sqlite3, os, warnings
import pandas as pd
import numpy as np

warnings.filterwarnings('ignore')

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, 'data', 'basketball.db')

# ── team name normalization ──────────────────
CBBD_TO_TORVIK = {
    'A&M-Corpus Christi': 'Texas A&M-Corpus Christi',
    'ALR': 'Little Rock',
    'AR-Pine Bluff': 'Arkansas Pine Bluff',
    'ASU': 'Arizona St.',
    'Abil Chr': 'Abilene Christian',
    'Abil Christian': 'Abilene Christian',
    'Abil Cristian': 'Abilene Christian',
    'Abil. Christian': 'Abilene Christian',
    'Abilene Chr.': 'Abilene Christian',
    'Abilene Christian Univ': 'Abilene Christian',
    'Abilene Christian University': 'Abilene Christian',
    'Alabama Birmingham': 'Alabama-Birmingham',
    'Alabama St': 'Alabama St.',
    'Alabama State': 'Alabama St.',
    'American Univ': 'American',
    'App St': 'Appalachian St.',
    'App St.': 'Appalachian St.',
    'App. St.': 'Appalachian St.',
    'Appalachian St': 'Appalachian St.',
    'Appalachian State': 'Appalachian St.',
    'Arizona St': 'Arizona St.',
    'Arizona State': 'Arizona St.',
    'Ark Pine Bluff': 'Arkansas Pine Bluff',
    'Ark.-Pine Bluff': 'Arkansas Pine Bluff',
    'Arkansas Little Rock': 'Little Rock',
    'Arkansas-LR': 'Little Rock',
    'Arkansas-Pine Bluff': 'Arkansas Pine Bluff',
    'BSU': 'Boise St.',
    'Boise St': 'Boise St.',
    'Boise State': 'Boise St.',
    'C Michigan': 'Central Michigan',
    'C. Michigan': 'Central Michigan',
    'CMU': 'Central Michigan',
    'CSU Bakersfield': 'Cal St. Bakersfield',
    'CSU-Bakersfield': 'Cal St. Bakersfield',
    'Cal State Bakersfield': 'Cal St. Bakersfield',
    'CalBaptist': 'Cal Baptist',
    'California Baptist': 'Cal Baptist',
    'Cent Conn St': 'Central Connecticut',
    'Cent Michigan': 'Central Michigan',
    'Cent. Conn. St.': 'Central Connecticut',
    'Central Mich': 'Central Michigan',
    'Central Mich.': 'Central Michigan',
    'Chicago St': 'Chicago St.',
    'Chicago State': 'Chicago St.',
    'Cleveland St': 'Cleveland St.',
    'Cleveland State': 'Cleveland St.',
    'Coast Carolina': 'Coastal Carolina',
    'Coppin St': 'Coppin St.',
    'Coppin State': 'Coppin St.',
    'Delaware St': 'Delaware St.',
    'Delaware State': 'Delaware St.',
    'E Illinois': 'Eastern Ilinois',
    'E Michigan': 'Eastern Michigan',
    'E. Illinois': 'Eastern Ilinois',
    'E. Michigan': 'Eastern Michigan',
    'ETSU': 'East Tennessee St.',
    'East Tennesse State': 'East Tennessee St.',
    'East Texas A&M': 'Texas A&M-Commerce',
    'F Dickinson': 'Farleigh Dickinson',
    'FAU': 'Florida Atlantic',
    'FGCU': 'Florida Gulf Coast',
    'FL Atlantic': 'Florida Atlantic',
    'Florida Intl': 'Florida International',
    'Fresno St': 'Fresno St.',
    'Fresno State': 'Fresno St.',
    'Houston Chr': 'Houston Christian',
    'IU Indy': 'IU Indianapolis',
    'IUPUI': 'IU Indianapolis',
    'Idaho St': 'Idaho St.',
    'Idaho State': 'Idaho St.',
    'Illinois St': 'Illinois St.',
    'Illinois State': 'Illinois St.',
    'Indiana St': 'Indiana St.',
    'Indiana State': 'Indiana St.',
    'Jacksonville': 'Jacksonville St.',
    'Jacksonville St': 'Jacksonville St.',
    'Jacksonville State': 'Jacksonville St.',
    'Kennessaw St': 'Kennesaw St.',
    'Kennessaw St.': 'Kennesaw St.',
    'Kennessaw State': 'Kennesaw St.',
    'Kent': 'Kent St.',
    'Kent St': 'Kent St.',
    'Kent State': 'Kent St.',
    'LA Tech': 'Louisiana Tech',
    'LIU Brooklyn': 'LIU',
    'La Tech': 'Louisiana Tech',
    'La. Tech': 'Louisiana Tech',
    'Lamar University': 'Lamar',
    'Long Island': 'LIU',
    'Louisiana': 'Louisiana-Lafayette',
    'MD E Shore': 'Maryland-Eastern Shore',
    'MES': 'Maryland-Eastern Shore',
    'MS Valley St': 'Mississippi Valley State',
    'MS Valley St.': 'Mississippi Valley State',
    'MS Valley State': 'Mississippi Valley State',
    'Maryland Eastern Shore': 'Maryland-Eastern Shore',
    'McNeese': 'McNeese St.',
    'McNeese St': 'McNeese St.',
    'McNeese State': 'McNeese St.',
    'Miami': 'Miami (FL)',
    'Miami FL': 'Miami (FL)',
    'Miami FL.': 'Miami (FL)',
    'Miami Florida': 'Miami (FL)',
    'Miami Oh.': 'Miami (OH)',
    'Miami Ohio': 'Miami (OH)',
    'Miami-FL': 'Miami (FL)',
    'Miami-OH': 'Miami (OH)',
    'Michigan St': 'Michigan St.',
    'Michigan State': 'Michigan St.',
    'Miss. Valley St.': 'Mississippi Valley State',
    'Mississippi Rebels': 'Mississippi',
    'Mississippi Va.': 'Mississippi Valley State',
    'Mississippi Valley St.': 'Mississippi Valley State',
    'Morgan St': 'Morgan St.',
    'Morgan State': 'Morgan St.',
    'N Dakota St': 'North Dakota St.',
    'N. Dakota St.': 'North Dakota St.',
    'N. Dakota State': 'North Dakota St.',
    'N. Illinois': 'Northern Illinois',
    'NC Central': 'North Carolina Central',
    'NCC': 'North Carolina Central',
    'NIU': 'Northern Illinois',
    'Nicholls': 'Nicholls St.',
    'Nicholls St': 'Nicholls St.',
    'Nicholls State': 'Nicholls St.',
    'Norfolk St': 'Norfolk St.',
    'Norfolk State': 'Norfolk St.',
    'North Dakota State': 'North Dakota St.',
    'Northwestern St': 'Northwestern St.',
    'Northwestern State': 'Northwestern St.',
    'Ole Miss': 'Mississippi',
    'PV A&M': 'Prairie View',
    'Portland St': 'Portland St.',
    'Portland State': 'Portland St.',
    'Prairie View A&M': 'Prairie View',
    'Queens NC': 'Queens',
    'Rhode Isl': 'Rhode Island',
    'S Carolina St': 'South Carolina St.',
    'S Illinois': 'Southern Illinois',
    'S. Carolina St.': 'South Carolina St.',
    'SE Lousiana': 'Southeastern La.',
    'SE Missouri St': 'Southeast Missouri St.',
    'SE Missouri St.': 'Southeast Missouri St.',
    'SE Missouri State': 'Southeast Missouri St.',
    'SEMO': 'Southeast Missouri St.',
    'SF Austin': 'Stephen F. Austin',
    'SFA': 'Stephen F. Austin',
    'SIU Edwardsville': 'SIU-Edwardsville',
    'SIUE': 'SIU-Edwardsville',
    'Saint Francis': 'St. Francis (PA)',
    'Saint Francis (PA)': 'St. Francis (PA)',
    'Saint Francis U': 'St. Francis (PA)',
    "Saint John's": "St. John's (NY)",
    'Sam Houston': 'Sam Houston St.',
    'Sam Houston St': 'Sam Houston St.',
    'Sam Houston State': 'Sam Houston St.',
    'So Dakota State': 'South Dakota St.',
    'So. Dakota State': 'South Dakota St.',
    'South Carolina State': 'South Carolina St.',
    'South Dakota St': 'South Dakota St.',
    'South Dakota State': 'South Dakota St.',
    'Southeast Missouri State': 'Southeast Missouri St.',
    'Southeastern LA': 'Southeastern La.',
    'Southeastern La': 'Southeastern La.',
    'Southeastern Louisiana': 'Southeastern La.',
    'Southern': 'Southern U.',
    'Southern Univ': 'Southern U.',
    'Southern University': 'Southern U.',
    "St John's": "St. John's (NY)",
    'St. Francis': 'St. Francis (PA)',
    "St. John's": "St. John's (NY)",
    'Stephen F Austin': 'Stephen F. Austin',
    'TAM C. Christi': 'Texas A&M-Corpus Christi',
    'Tarleton': 'Tarleton St.',
    'Tarleton St': 'Tarleton St.',
    'Tarleton State': 'Tarleton St.',
    'Tex. A&M-Commerce': 'Texas A&M-Commerce',
    'Texas A&M CC': 'Texas A&M-Corpus Christi',
    'Texas A&M Corpus Chris': 'Texas A&M-Corpus Christi',
    'Texas A&M Corpus Christi': 'Texas A&M-Corpus Christi',
    'Texas A&M-CC': 'Texas A&M-Corpus Christi',
    'Texas Christian': 'TCU',
    'Texas So': 'Texas Southern',
    'Texas So.': 'Texas Southern',
    'Towson (MMMT)': 'Towson',
    'Towson MMMT': 'Towson',
    'UAB': 'Alabama-Birmingham',
    'UCD': 'UC Davis',
    'UCSB': 'UC Santa Barbara',
    'UConn': 'Connecticut',
    'UIW': 'Incarnate Word',
    'UL Lafayette': 'Louisiana-Lafayette',
    'UMES': 'Maryland-Eastern Shore',
    'UMKC': 'Kansas City',
    'UNCG': 'UNC Greensboro',
    'UNCW': 'UNC Wilmington',
    'URI': 'Rhode Island',
    'USC': 'Southern California',
    'UTRGV': 'UT Rio Grande Valley',
    'Univ Southern California': 'Southern California',
    'Univ. of Southern California': 'Southern California',
    'Utah St': 'Utah St.',
    'Utah State': 'Utah St.',
    'W Carolina': 'Western Carolina',
    'W Illinois': 'Western Illinois',
    'W Michigan': 'Western Michigan',
    'W. Carolina': 'Western Carolina',
    'W. Illinois': 'Western Illinois',
    'W. Michigan': 'Western Michigan',
    'Western Caro.': 'Western Carolina',
    'Wichita St': 'Wichita St.',
    'Wichita State': 'Wichita St.',
    'abilchristian': 'Abilene Christian',
    'abilenechristian': 'Abilene Christian',
    'arizonast': 'Arizona St.',
    'arizonastate': 'Arizona St.',
    'calbaptist': 'Cal Baptist',
    'calstbakersfield': 'Cal St. Bakersfield',
    'centmichigan': 'Central Michigan',
    'centralmichigan': 'Central Michigan',
    'charlestonso': 'Charleston Southern',
    'charlestonsouthern': 'Charleston Southern',
    'coastalcarolina': 'Coastal Carolina',
    'coastcarolina': 'Coastal Carolina',
    'coppinstate': 'Coppin St.',
    'csubakersfield': 'Cal St. Bakersfield',
    'easternillinois': 'Eastern Ilinois',
    'eillinois': 'Eastern Ilinois',
    'illinoisst': 'Illinois St.',
    'illinoisstate': 'Illinois St.',
    'indianast': 'Indiana St.',
    'iuindy': 'IU Indianapolis',
    'jacksonvillestate': 'Jacksonville St.',
    'mcneese': 'McNeese St.',
    'mississippi': 'Mississippi',
    'mississippivalleystate': 'Mississippi Valley State',
    'norfolkstate': 'Norfolk St.',
    'olemiss': 'Mississippi',
    'rhodeisland': 'Rhode Island',
    'sfa': 'Stephen F. Austin',
    'tarletonst': 'Tarleton St.',
    'towsonmnmt': 'Towson',
    'umes': 'Maryland-Eastern Shore',
    'usc': 'Southern California',
    'utahtech': 'Utah Tech',
    'westernillinois': 'Western Illinois',
    'willinois': 'Western Illinois',
    'American University': 'American',
    'App State': 'Appalachian St.',
    'Alcorn State': 'Alcorn St.',
    'UL Monroe': 'Louisiana-Monroe',
    'North Dakota': 'North Dakota',


    # Full state name → Torvik abbreviated (added batch fix)
    'Alcorn State':            'Alcorn St.',
    'Arkansas State':          'Arkansas St.',
    'Ball State':              'Ball St.',
    'Bethune-Cookman':         'Bethune Cookman',
    'Cal State Fullerton':     'Cal St. Fullerton',
    'Cal State Northridge':    'Cal St. Northridge',
    'Cal State Bakersfield':   'Cal St. Bakersfield',
    'Colorado State':          'Colorado St.',
    'Coppin State':            'Coppin St.',
    'Delaware State':          'Delaware St.',
    'East Tennessee State':    'ETSU',
    'Fayetteville State':      'Fayetteville St.',
    'Florida International':   'FIU',
    'Florida State':           'Florida St.',
    'Fort Valley State':       'Fort Valley St.',
    'Gardner-Webb':            'Gardner Webb',
    'Georgia State':           'Georgia St.',
    'Grambling':               'Grambling St.',
    "Hawai'i":                 'Hawaii',
    "Hawai'i Hilo":            'Hawaii Hilo',
    'Iowa State':              'Iowa St.',
    'Jackson State':           'Jackson St.',
    'Kansas State':            'Kansas St.',
    'Kennesaw State':          'Kennesaw St.',
    'Long Beach State':        'Long Beach St.',
    'Long Island University':  'LIU',
    'Louisiana-Monroe':        'Louisiana Monroe',
    'Loyola Maryland':         'Loyola MD',
    'McNeese State':           'McNeese St.',
    'Miami (OH)':              'Miami OH',
    'Mississippi State':       'Mississippi St.',
    'Missouri State':          'Missouri St.',
    'Montana State':           'Montana St.',
    'Morehead State':          'Morehead St.',
    'Murray State':            'Murray St.',
    'NC State':                'N.C. State',
    'New Mexico State':        'New Mexico St.',
    'Nicholls State':          'Nicholls St.',
    'Norfolk State':           'Norfolk St.',
    'Ohio State':              'Ohio St.',
    'Oklahoma State':          'Oklahoma St.',
    'Omaha':                   'Nebraska Omaha',
    'Oregon State':            'Oregon St.',
    'Penn State':              'Penn St.',
    'Pennsylvania':            'Penn',
    'Prairie View A&M':        'Prairie View',
    'Queens University':       'Queens',
    'SE Louisiana':            'SE Louisiana',
    'Sacramento State':        'Sacramento St.',
    'Sam Houston State':       'Sam Houston St.',
    'San Diego State':         'San Diego St.',
    'Tennessee State':         'Tennessee St.',
    'Troy State':              'Troy',
    'Weber State':             'Weber St.',
    'Wichita State':           'Wichita St.',
    'Winston-Salem State':     'Winston-Salem St.',
    'Youngstown State':        'Youngstown St.',
}

def norm(name):
    if not name: return name
    s = str(name).strip()
    return CBBD_TO_TORVIK.get(s, s)

def db():
    conn = sqlite3.connect(DB)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn

def gap(a, b):
    try:
        if a is not None and b is not None:
            return float(a) - float(b)
    except: pass
    return None

def tbl_exists(conn, name):
    return conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()[0] > 0


# ── loaders ─────────────────────────────────

def load_kenpom(conn):
    if not tbl_exists(conn, 'kenpom_ratings'): return {}
    try:
        rows = conn.execute("""
            SELECT season, team, adj_em, adj_o, adj_d, adj_t
            FROM kenpom_ratings
        """).fetchall()
        out = {}
        for r in rows:
            out[(int(r[0]), norm(r[1]))] = {
                'kp_em': r[2], 'kp_o': r[3], 'kp_d': r[4], 'kp_t': r[5]
            }
        print(f"  KenPom: {len(out)} (season, team) entries")
        return out
    except Exception as e:
        print(f"  KenPom WARNING: {e}")
        return {}

def load_torvik_season(conn):
    if not tbl_exists(conn, 'torvik_season'): return {}
    try:
        rows = conn.execute("SELECT * FROM torvik_season").fetchall()
        out = {}
        for r in rows:
            d = dict(r)
            out[(int(d['season']), norm(d['team']))] = {f'tv_{k}': v for k, v in d.items()
                                                         if k not in ('season','team')}
        print(f"  Torvik season: {len(out)} entries")
        return out
    except Exception as e:
        print(f"  Torvik season WARNING: {e}")
        return {}

def load_torvik_daily(conn):
    """Load torvik_daily into a nested dict for O(log n) per-game lookups.
    Structure: {(season, team): sorted list of (snap_int, row_dict)}
    snap_int is YYYYMMDD as integer for fast comparison.
    """
    if not tbl_exists(conn, 'torvik_daily'): return {}
    try:
        df = pd.read_sql(
            "SELECT season, team, adj_o, adj_d, barthag, adj_em, "
            "efg_o, efg_d, tov_o, tov_d, orb, drb, ftr_o, ftr_d, "
            "two_p_o, two_p_d, three_p_o, three_p_d, "
            "blk_pct, ast_pct, three_p_rate, wins, games, "
            "snapshot_date FROM torvik_daily", conn)
        df['team'] = df['team'].apply(norm)
        df['season'] = df['season'].astype(int)
        # Convert snapshot_date to YYYYMMDD int regardless of storage format
        def snap_to_int(s):
            return int(str(s).strip().replace('-', '')[:8])
        df['snap_int'] = df['snapshot_date'].apply(snap_to_int)

        cols = ['adj_o','adj_d','barthag','adj_em','efg_o','efg_d',
                'tov_o','tov_d','orb','drb','ftr_o','ftr_d',
                'two_p_o','two_p_d','three_p_o','three_p_d',
                'blk_pct','ast_pct','three_p_rate','wins','games']
        index = {}
        for (season, team), grp in df.groupby(['season','team']):
            grp_s = grp.sort_values('snap_int')
            index[(int(season), team)] = list(zip(
                grp_s['snap_int'].tolist(),
                grp_s[cols].to_dict('records')
            ))
        print(f"  Torvik daily: {len(df):,} snapshots, {len(index):,} (season,team) keys")
        return index
    except Exception as e:
        print(f"  Torvik daily WARNING: {e}")
        return {}

def load_torvik_preds(conn):
    if not tbl_exists(conn, 'torvik_game_preds'): return pd.DataFrame()
    try:
        df = pd.read_sql("SELECT * FROM torvik_game_preds", conn)
        df['home_team'] = df['home_team'].apply(norm)
        df['away_team'] = df['away_team'].apply(norm)
        df['game_date'] = pd.to_datetime(df['game_date'])
        print(f"  Torvik preds: {len(df)} games")
        return df
    except Exception as e:
        print(f"  Torvik preds WARNING: {e}")
        return pd.DataFrame()

def load_haslametrics(conn):
    """Load haslametrics_full, merging TI and TD variants per (season, team).
    Returns dict keyed by (season, team) with all columns 05 expects."""
    table = 'haslametrics_full' if tbl_exists(conn, 'haslametrics_full') else 'haslametrics'
    if not tbl_exists(conn, table): return {}
    try:
        df = pd.read_sql(f"SELECT * FROM {table}", conn)
        df['team'] = df['team'].apply(norm)
        out = {}
        # Build per (season, team) — prefer TI for season-level signal,
        # use TD for momentum/delta features (TD minus TI = in-season change)
        for (season, team), grp in df.groupby(['season', 'team']):
            ti = grp[grp['variant'] == 'ti'].iloc[0].to_dict() if 'ti' in grp['variant'].values else {}
            td = grp[grp['variant'] == 'td'].iloc[0].to_dict() if 'td' in grp['variant'].values else {}
            base = ti if ti else td

            def fv(d, k):
                v = d.get(k)
                return float(v) if v is not None and str(v) not in ('None', 'nan', '') else None

            entry = {
                # Core efficiency (from TI)
                'o_eff':  fv(base, 'o_eff'),
                'd_eff':  fv(base, 'd_eff'),
                'pace':   fv(base, 'pace'),
                'con':    fv(base, 'con'),
                'sos':    fv(base, 'sos'),
                'rq':     fv(base, 'rq'),
                # Shot quality (from TI)
                'o_3par': fv(base, 'o_3par'),
                'd_3par': fv(base, 'd_3par'),
                'o_prox': fv(base, 'o_prox'),
                'd_prox': fv(base, 'd_prox'),
                'o_mrar': fv(base, 'o_mrar'),
                'd_mrar': fv(base, 'd_mrar'),
                'o_fg_pct': fv(base, 'o_fg_pct'),
                'd_fg_pct': fv(base, 'd_fg_pct'),
                # Delta (TD minus TI = in-season momentum)
                'delta_o_eff': (fv(td,'o_eff') - fv(ti,'o_eff'))
                    if (ti and td and fv(td,'o_eff') is not None and fv(ti,'o_eff') is not None) else None,
                'delta_d_eff': (fv(td,'d_eff') - fv(ti,'d_eff'))
                    if (ti and td and fv(td,'d_eff') is not None and fv(ti,'d_eff') is not None) else None,
                'delta_pace': (fv(td,'pace') - fv(ti,'pace'))
                    if (ti and td and fv(td,'pace') is not None and fv(ti,'pace') is not None) else None,
            }
            out[(int(season), team)] = entry
        print(f"  Haslametrics ({table}): {len(out)} (season, team) entries")
        return out
    except Exception as e:
        print(f"  Haslametrics WARNING: {e}")
        return {}

def load_games(conn):
    df = pd.read_sql("""
        SELECT id, season, game_date, home_team, away_team,
       home_score, away_score,
       CAST(neutral_site AS INTEGER) as neutral_site,
       CAST(conf_game AS INTEGER) as conf_game,
       (home_score - away_score) as actual_margin
FROM games
WHERE home_score IS NOT NULL AND away_score IS NOT NULL
    """, conn)
    df['home_team'] = df['home_team'].apply(norm)
    df['away_team'] = df['away_team'].apply(norm)
    df['game_date'] = pd.to_datetime(df['game_date'])
    print(f"  Games: {len(df)}")
    return df

def load_lines(conn):
    try:
        df = pd.read_sql("""
            SELECT game_date, home_team, away_team,
                   AVG(spread) as spread,
                   MIN(spread_open) as spread_open,
                   AVG(over_under) as over_under,
                   MIN(over_under_open) as over_under_open
            FROM game_lines
            WHERE spread IS NOT NULL
            GROUP BY game_date, home_team, away_team
        """, conn)
        df['home_team'] = df['home_team'].apply(norm)
        df['away_team'] = df['away_team'].apply(norm)
        df['game_date'] = pd.to_datetime(df['game_date'])
        df = df.set_index(['game_date','home_team','away_team'])
        print(f"  Lines: {len(df)} unique games")
        return df
    except Exception as e:
        print(f"  Lines WARNING: {e}")
        return pd.DataFrame()

def load_stats(conn):
    if not tbl_exists(conn, 'game_team_stats'): return pd.DataFrame()
    try:
        df = pd.read_sql("""
            SELECT s.game_id, s.team, s.points,
                   s.fg_made, s.fg_att,
                   s.three_made, s.three_att,
                   s.ft_made, s.ft_att,
                   s.off_rebounds, s.def_rebounds, s.turnovers,
                   g.game_date, g.season
            FROM game_team_stats s
            JOIN games g ON s.game_id = g.id
            WHERE s.points IS NOT NULL
            ORDER BY g.game_date
        """, conn)
        df['team'] = df['team'].apply(norm)
        df['game_date'] = pd.to_datetime(df['game_date'])
        print(f"  Box scores: {len(df)}")
        return df
    except Exception as e:
        print(f"  Stats WARNING: {e}")
        return pd.DataFrame()


# ── rolling stats ────────────────────────────

def build_rolling(stats_df, window=10):
    if stats_df.empty: return {}
    df = stats_df.copy()
    df['fga'] = df['fg_att'].fillna(0)
    df['fgm'] = df['fg_made'].fillna(0)
    df['tpa'] = df['three_att'].fillna(0)
    df['tpm'] = df['three_made'].fillna(0)
    df['fta'] = df['ft_att'].fillna(0)
    df['orb'] = df['off_rebounds'].fillna(0)
    df['drb'] = df['def_rebounds'].fillna(0)
    df['tov'] = df['turnovers'].fillna(0)
    df['pts'] = df['points'].fillna(0)

    df['efg'] = np.where(df['fga']>0, (df['fgm']+0.5*df['tpm'])/df['fga'], np.nan)
    df['poss'] = df['fga'] - df['orb'] + df['tov'] + 0.475*df['fta']
    df['tov_pct'] = np.where(df['poss']>0, df['tov']/df['poss'], np.nan)
    df['orb_pct'] = np.where((df['orb']+df['drb'])>0, df['orb']/(df['orb']+df['drb']), np.nan)
    df['ftr'] = np.where(df['fga']>0, df['fta']/df['fga'], np.nan)
    df['ppp'] = np.where(df['poss']>0, df['pts']/df['poss'], np.nan)

    result = {}
    for team, grp in df.groupby('team'):
        grp = grp.sort_values('game_date').reset_index(drop=True)
        for i, row in grp.iterrows():
            prior = grp.iloc[max(0, i-window):i]
            if len(prior) < 3: continue
            result[(team, row['game_date'])] = {
                'r_efg':     prior['efg'].mean(),
                'r_tov':     prior['tov_pct'].mean(),
                'r_orb':     prior['orb_pct'].mean(),
                'r_ftr':     prior['ftr'].mean(),
                'r_ppp':     prior['ppp'].mean(),
                'r_pts':     prior['pts'].mean(),
                'r_n':       len(prior),
            }
    return result


# ── home court ───────────────────────────────

def build_hca(games_df):
    hca = {}
    for test_s in games_df['season'].unique():
        train = games_df[(games_df['season'] != test_s) & (~games_df['neutral_site'].astype(bool))]
        for venue, grp in train.groupby('home_team'):
            if len(grp) >= 10:
                hca[(int(test_s), venue)] = float(grp['actual_margin'].mean())
    return hca


# ── rest ─────────────────────────────────────

def build_rest(games_df):
    rest = {}
    all_games = pd.concat([
        games_df[['home_team','game_date']].rename(columns={'home_team':'team'}),
        games_df[['away_team','game_date']].rename(columns={'away_team':'team'}),
    ]).sort_values(['team','game_date'])

    for team, grp in all_games.groupby('team'):
        dates = grp['game_date'].tolist()
        for i, d in enumerate(dates):
            rest[(team, d)] = min((d - dates[i-1]).days, 14) if i > 0 else 7
    return rest


# ── torvik daily lookup ───────────────────────

def torvik_as_of(td_index, team, gdate, season):
    """Fast O(log n) lookup: find most recent snapshot before game date."""
    if not td_index: return None
    snaps = td_index.get((int(season), team))
    if not snaps: return None
    # Convert game date to YYYYMMDD int
    if hasattr(gdate, 'strftime'):
        gdate_int = int(gdate.strftime('%Y%m%d'))
    else:
        gdate_int = int(str(gdate).replace('-', '')[:8])
    # Binary search for latest snapshot strictly before game date
    lo, hi, result = 0, len(snaps) - 1, None
    while lo <= hi:
        mid = (lo + hi) // 2
        if snaps[mid][0] < gdate_int:
            result = snaps[mid][1]
            lo = mid + 1
        else:
            hi = mid - 1
    return result


# ── main ─────────────────────────────────────

def build_features():
    conn = db()
    print("Loading data sources...")

    games    = load_games(conn)
    lines    = load_lines(conn)
    kenpom   = load_kenpom(conn)
    tv_s     = load_torvik_season(conn)
    tv_d     = load_torvik_daily(conn)
    tv_p     = load_torvik_preds(conn)
    hasla    = load_haslametrics(conn)
    stats    = load_stats(conn)
    kpd      = load_kenpom_daily(conn)
    kp_fm    = load_kenpom_fanmatch(conn)
    rolling  = load_rolling(conn)

    print("Computing derived data...")
    rolling  = build_rolling(stats)
    hca      = build_hca(games)
    rest_map = build_rest(games)

    # Merge lines on date+home+away (game_lines uses external IDs, not our games.id)
    if not lines.empty:
        lines_reset = lines.reset_index()
        games = games.merge(lines_reset, on=['game_date','home_team','away_team'], how='left')
    else:
        games['spread'] = np.nan
        games['spread_open'] = np.nan

    rows = []
    print(f"Building {len(games):,} feature rows...")

    for _, g in games.iterrows():
        s    = int(g['season'])
        home = g['home_team']
        away = g['away_team']
        gd   = pd.Timestamp(g['game_date'])  # re-cast in case merge changed dtype
        am   = g['actual_margin']
        if pd.isna(am): continue

        spread = g.get('spread') if pd.notna(g.get('spread')) else None
        spread_open = g.get('spread_open') if pd.notna(g.get('spread_open')) else None
        over_under = g.get('over_under') if pd.notna(g.get('over_under')) else None
        over_under_open = g.get('over_under_open') if pd.notna(g.get('over_under_open')) else None
        actual_total = (g.get('home_score', 0) or 0) + (g.get('away_score', 0) or 0)
        actual_total = actual_total if actual_total > 0 else None

        r = {
            'game_id': g['id'], 'season': s, 'game_date': str(gd.date()),
            'home_team': home, 'away_team': away,
            'actual_margin': am, 'spread': spread, 'spread_open': spread_open,
            'over_under': over_under, 'over_under_open': over_under_open,
            'actual_total': actual_total,
            'neutral_site': int(bool(g.get('neutral_site',0))),
            'conf_game': int(bool(g.get('conf_game',0))),
        }

        # ── KenPom (prior season) ──
        for side, team in [('h',home),('a',away)]:
            kp = kenpom.get((s-1, team), {})
            r[f'{side}_kp_adj_em'] = kp.get('kp_em')
            r[f'{side}_kp_adj_o']  = kp.get('kp_o')
            r[f'{side}_kp_adj_d']  = kp.get('kp_d')
            r[f'{side}_kp_adj_t']  = kp.get('kp_t')
        r['kp_em_gap']   = gap(r['h_kp_adj_em'], r['a_kp_adj_em'])
        r['has_kp_home'] = int(r['h_kp_adj_em'] is not None)
        r['has_kp_away'] = int(r['a_kp_adj_em'] is not None)

        # ── Torvik season (prior season) ──
        tvs_keys = ['adj_em','adj_o','adj_d','adj_t','barthag',
                    'efg_o','efg_d','tov_o','tov_d','orb','drb',
                    'ftr_o','ftr_d','two_p_o','two_p_d',
                    'three_p_o','three_p_d','blk_pct','ast_pct',
                    'avg_hgt','experience','pake','pase',
                    'talent','elite_sos','wab']
        for side, team in [('h',home),('a',away)]:
            tv = tv_s.get((s-1, team), {})
            for k in tvs_keys:
                r[f'{side}_tvs_{k}'] = tv.get(f'tv_{k}')
        r['tvs_em_gap']      = gap(r['h_tvs_adj_em'],  r['a_tvs_adj_em'])
        r['tvs_barthag_gap'] = gap(r['h_tvs_barthag'], r['a_tvs_barthag'])
        r['has_tvs_home']    = int(r['h_tvs_adj_em'] is not None)
        r['has_tvs_away']    = int(r['a_tvs_adj_em'] is not None)

        # ── Torvik daily snapshot (as-of game date) ──
        tvd_keys = ['adj_o','adj_d','barthag','adj_em',
                    'efg_o','efg_d','tov_o','tov_d','orb','drb','ftr_o','ftr_d',
                    'two_p_o','two_p_d','three_p_o','three_p_d',
                    'blk_pct','ast_pct','three_p_rate','wins','games']
        for side, team in [('h',home),('a',away)]:
            snap = torvik_as_of(tv_d, team, gd, s)
            for k in tvd_keys:
                val = snap.get(k) if snap is not None else None
                r[f'{side}_tvd_{k}'] = float(val) if val is not None else None
        r['tvd_bar_gap']  = gap(r.get('h_tvd_barthag'), r.get('a_tvd_barthag'))
        r['tvd_em_gap']   = gap(r.get('h_tvd_adj_em'),  r.get('a_tvd_adj_em'))
        r['tvd_efg_gap']  = gap(r.get('h_tvd_efg_o'),   r.get('a_tvd_efg_o'))
        r['tvd_3p_gap']   = gap(r.get('h_tvd_three_p_o'), r.get('a_tvd_three_p_o'))
        r['tvd_2p_gap']   = gap(r.get('h_tvd_two_p_o'),   r.get('a_tvd_two_p_o'))
        # Use barthag (100% populated) as primary TVD presence flag
        r['has_tvd_home'] = int(r.get('h_tvd_barthag') is not None)
        r['has_tvd_away'] = int(r.get('a_tvd_barthag') is not None)

        # ── KenPom daily (as-of game date) ──
        kpd_keys = ['adj_em','adj_o','adj_d','adj_tempo','luck',
                    'sos','sos_o','sos_d','rank_adj_em','pythag']
        for side, team in [('h',home),('a',away)]:
            snap = kenpom_as_of(kpd, team, gd, s)
            for k in kpd_keys:
                val = snap.get(k) if snap is not None else None
                r[f'{side}_kpd_{k}'] = float(val) if val is not None else None
        r['kpd_em_gap']   = gap(r.get('h_kpd_adj_em'), r.get('a_kpd_adj_em'))
        r['kpd_luck_gap'] = gap(r.get('h_kpd_luck'),   r.get('a_kpd_luck'))
        r['kpd_sos_gap']  = gap(r.get('h_kpd_sos'),    r.get('a_kpd_sos'))
        r['has_kpd_home'] = int(r.get('h_kpd_adj_em') is not None)
        r['has_kpd_away'] = int(r.get('a_kpd_adj_em') is not None)

        # ── KenPom fanmatch predictions ──
        gd_str = str(gd.date()) if hasattr(gd, 'date') else str(gd)[:10]
        fm = kp_fm.get((gd_str, home, away))
        if fm is None:
            # Try reverse (home/away may be flipped in fanmatch)
            fm_rev = kp_fm.get((gd_str, away, home))
            if fm_rev is not None:
                # Flip perspective
                r['kp_home_pred']  = fm_rev.get('away_pred')
                r['kp_away_pred']  = fm_rev.get('home_pred')
                r['kp_home_wp']    = 1.0 - float(fm_rev['home_wp'])/100 if fm_rev.get('home_wp') else None
                r['kp_pred_margin']= (r['kp_home_pred'] - r['kp_away_pred']) if r['kp_home_pred'] and r['kp_away_pred'] else None
                r['kp_pred_tempo'] = fm_rev.get('pred_tempo')
            else:
                r['kp_home_pred'] = r['kp_away_pred'] = r['kp_home_wp'] = None
                r['kp_pred_margin'] = r['kp_pred_tempo'] = None
        else:
            r['kp_home_pred']   = fm.get('home_pred')
            r['kp_away_pred']   = fm.get('away_pred')
            r['kp_home_wp']     = float(fm['home_wp'])/100 if fm.get('home_wp') else None
            r['kp_pred_margin'] = (float(fm['home_pred']) - float(fm['away_pred'])) if fm.get('home_pred') and fm.get('away_pred') else None
            r['kp_pred_tempo']  = fm.get('pred_tempo')
        r['has_kp_fanmatch'] = int(r.get('kp_pred_margin') is not None)

        # ── Rolling box score efficiency (last 5/10 games) ──
        for side, team in [('h',home),('a',away)]:
            rol = rolling.get((gd_str, team))
            if rol:
                for k in ['r5_efg','r5_tov','r5_orb','r5_ftr','r5_3pct','r5_pace',
                          'r5_pts_off','r5_pts_def','r5_margin',
                          'r10_efg','r10_tov','r10_orb','r10_pace','r10_margin',
                          'ew_efg','ew_tov','ew_orb','ew_pts_off','ew_pts_def','ew_margin',
                          'trend_efg','trend_margin','games_played']:
                    v = rol.get(k)
                    r[f'{side}_rol_{k}'] = float(v) if v is not None else None
            else:
                for k in ['r5_efg','r5_tov','r5_orb','r5_ftr','r5_3pct','r5_pace',
                          'r5_pts_off','r5_pts_def','r5_margin',
                          'r10_efg','r10_tov','r10_orb','r10_pace','r10_margin',
                          'ew_efg','ew_tov','ew_orb','ew_pts_off','ew_pts_def','ew_margin',
                          'trend_efg','trend_margin','games_played']:
                    r[f'{side}_rol_{k}'] = None
        r['rol_margin_gap'] = gap(r.get('h_rol_r10_margin'), r.get('a_rol_r10_margin'))
        r['rol_efg_gap']    = gap(r.get('h_rol_r10_efg'),    r.get('a_rol_r10_efg'))
        r['rol_trend_gap']  = gap(r.get('h_rol_trend_margin'),r.get('a_rol_trend_margin'))
        r['has_rol_home']   = int(r.get('h_rol_r5_margin') is not None)
        r['has_rol_away']   = int(r.get('a_rol_r5_margin') is not None)

        # ── Torvik game predictions ──
        if not tv_p.empty:
            tp_mask = ((tv_p['season']==s) & (tv_p['game_date']==gd) &
                       (tv_p['home_team']==home) & (tv_p['away_team']==away))
            tp_rows = tv_p[tp_mask]
            if not tp_rows.empty:
                tp = tp_rows.iloc[0]
                r['torvik_pred'] = tp.get('torvik_margin')
                r['torvik_prob'] = tp.get('torvik_win_prob')
                r['torvik_vs_spread'] = gap(tp.get('torvik_margin'), spread) if spread else None
            else:
                r['torvik_pred'] = r['torvik_prob'] = r['torvik_vs_spread'] = None

        # ── Haslametrics (prior season, from haslametrics_full) ──
        for side, team in [('h',home),('a',away)]:
            ha = hasla.get((s, team), {})  # same season — end-of-season ratings, no leakage
            # HA_CORE
            r[f'{side}_ha_o_eff']  = ha.get('o_eff')
            r[f'{side}_ha_d_eff']  = ha.get('d_eff')
            r[f'{side}_ha_pace']   = ha.get('pace')
            r[f'{side}_ha_con']    = ha.get('con')
            r[f'{side}_ha_sos']    = ha.get('sos')
            r[f'{side}_ha_rq']     = ha.get('rq')
            r[f'{side}_has_hasla'] = int(ha.get('o_eff') is not None)
            # HA_SHOT
            r[f'{side}_ha_o_3par']   = ha.get('o_3par')
            r[f'{side}_ha_d_3par']   = ha.get('d_3par')
            r[f'{side}_ha_o_prox']   = ha.get('o_prox')
            r[f'{side}_ha_d_prox']   = ha.get('d_prox')
            r[f'{side}_ha_o_mrar']   = ha.get('o_mrar')
            r[f'{side}_ha_d_mrar']   = ha.get('d_mrar')
            r[f'{side}_ha_o_fg_pct'] = ha.get('o_fg_pct')
            r[f'{side}_ha_d_fg_pct'] = ha.get('d_fg_pct')
            # HA_DELTA (TD - TI momentum)
            r[f'{side}_ha_delta_o_eff'] = ha.get('delta_o_eff')
            r[f'{side}_ha_delta_d_eff'] = ha.get('delta_d_eff')
            r[f'{side}_ha_delta_pace']  = ha.get('delta_pace')

        # HA_CORE gaps
        r['ha_gap_o_eff']  = gap(r['h_ha_o_eff'],  r['a_ha_o_eff'])
        r['ha_gap_d_eff']  = gap(r['h_ha_d_eff'],  r['a_ha_d_eff'])
        r['ha_pace_avg']   = (r['h_ha_pace'] + r['a_ha_pace']) / 2 if (
            r['h_ha_pace'] is not None and r['a_ha_pace'] is not None) else None
        # HA_SHOT gaps
        r['ha_gap_o_3par'] = gap(r['h_ha_o_3par'], r['a_ha_o_3par'])
        r['ha_gap_d_3par'] = gap(r['h_ha_d_3par'], r['a_ha_d_3par'])
        r['ha_gap_o_prox'] = gap(r['h_ha_o_prox'], r['a_ha_o_prox'])
        r['ha_gap_d_prox'] = gap(r['h_ha_d_prox'], r['a_ha_d_prox'])
        r['ha_gap_o_mrar'] = gap(r['h_ha_o_mrar'], r['a_ha_o_mrar'])
        r['ha_gap_d_mrar'] = gap(r['h_ha_d_mrar'], r['a_ha_d_mrar'])
        # HA_DELTA gaps
        r['ha_momentum_gap'] = gap(r['h_ha_delta_o_eff'], r['a_ha_delta_o_eff'])
        r['ha_def_mom_gap']  = gap(r['h_ha_delta_d_eff'], r['a_ha_delta_d_eff'])
        # HA_MATCHUP (home offense vs away defense and vice versa)
        r['ha_3par_matchup_h'] = gap(r['h_ha_o_3par'], r['a_ha_d_3par'])
        r['ha_3par_matchup_a'] = gap(r['a_ha_o_3par'], r['h_ha_d_3par'])
        r['ha_prox_matchup_h'] = gap(r['h_ha_o_prox'], r['a_ha_d_prox'])
        r['ha_prox_matchup_a'] = gap(r['a_ha_o_prox'], r['h_ha_d_prox'])
        # Summary for coverage reporting
        r['ha_eff_gap'] = r['ha_gap_o_eff']

        # ── Rolling box score ──
        for side, team in [('h',home),('a',away)]:
            rv = rolling.get((team, gd), {})
            for k in ['r_efg','r_tov','r_orb','r_ftr','r_ppp','r_pts']:
                r[f'{side}_{k}'] = rv.get(k)
        r['roll_ppp_gap'] = gap(r.get('h_r_ppp'), r.get('a_r_ppp'))
        r['roll_efg_gap'] = gap(r.get('h_r_efg'), r.get('a_r_efg'))
        r['roll_pts_gap'] = gap(r.get('h_r_pts'), r.get('a_r_pts'))

        # ── Home court ──
        r['hca']     = hca.get((s, home), 3.2)
        r['hca_adj'] = r['hca'] if not r['neutral_site'] else 0.0

        # ── Rest ──
        hr = rest_map.get((home, gd), 4)
        ar = rest_map.get((away, gd), 4)
        r['home_rest'] = hr
        r['away_rest'] = ar
        r['rest_diff'] = hr - ar
        r['home_b2b']  = int(hr <= 1)
        r['away_b2b']  = int(ar <= 1)

        # ── Line movement ──
        if spread is not None and spread_open is not None:
            try: r['line_move'] = float(spread) - float(spread_open)
            except: r['line_move'] = None
        else:
            r['line_move'] = None

        rows.append(r)

    df = pd.DataFrame(rows)

    # ATS result
    df['ats_win'] = np.where(
        df['spread'].notna(),
        ((df['actual_margin'] + df['spread']) > 0).astype(float),
        np.nan
    )

    # Save
    df.to_sql('game_features_v2', conn, if_exists='replace', index=False)
    conn.close()

    print(f"\n✅ Saved {len(df):,} rows → game_features_v2")
    print(f"   Columns: {len(df.columns)}")
    print(f"   With spread:       {df['spread'].notna().sum():,}")
    print(f"   With over/under:   {df['over_under'].notna().sum():,}")
    print(f"   With torvik daily: {df['tvd_bar_gap'].notna().sum():,}")
    print(f"   With kenpom daily: {df['kpd_em_gap'].notna().sum():,}")
    print(f"   With kp fanmatch:  {df['kp_pred_margin'].notna().sum():,}")
    print(f"   With rolling:      {df['rol_margin_gap'].notna().sum():,}")
    print(f"   With haslametrics: {df['ha_eff_gap'].notna().sum():,}")
    print(f"   With torvik pred:  {df['torvik_pred'].notna().sum():,}")
    return df


if __name__ == '__main__':
    print("="*60)
    print("NCAAB Model — Feature Engineering (All Sources)")
    print("="*60)
    build_features()
    print("\nNext: python scripts/05_backtest_all_combos.py")

# ═══════════════════════════════════════════════════════════════════
# NEW LOADERS — appended by Phase 2 build
# ═══════════════════════════════════════════════════════════════════

def load_kenpom_daily(conn):
    """Load kenpom_daily into indexed dict — same pattern as torvik_daily."""
    if not tbl_exists(conn, 'kenpom_daily'): return {}
    try:
        df = pd.read_sql("""
            SELECT season, snapshot_date, team,
                   adj_em, adj_o, adj_d, adj_tempo, luck,
                   sos, sos_o, sos_d, ncsос, rank_adj_em, pythag
            FROM kenpom_daily
        """, conn)
        df['team'] = df['team'].apply(norm)
        df['season'] = df['season'].astype(int)
        def snap_to_int(s):
            return int(str(s).strip().replace('-','')[:8])
        df['snap_int'] = df['snapshot_date'].apply(snap_to_int)
        cols = ['adj_em','adj_o','adj_d','adj_tempo','luck',
                'sos','sos_o','sos_d','ncsос','rank_adj_em','pythag']
        index = {}
        for (season, team), grp in df.groupby(['season','team']):
            grp_s = grp.sort_values('snap_int')
            index[(int(season), team)] = list(zip(
                grp_s['snap_int'].tolist(),
                grp_s[cols].to_dict('records')
            ))
        print(f"  KenPom daily: {len(df):,} snapshots, {len(index):,} (season,team) keys")
        return index
    except Exception as e:
        print(f"  KenPom daily WARNING: {e}")
        return {}


def kenpom_as_of(kpd_index, team, gdate, season):
    """Identical binary search as torvik_as_of."""
    if not kpd_index: return None
    snaps = kpd_index.get((int(season), team))
    if not snaps: return None
    if hasattr(gdate, 'strftime'):
        gdate_int = int(gdate.strftime('%Y%m%d'))
    else:
        gdate_int = int(str(gdate).replace('-','')[:8])
    lo, hi, result = 0, len(snaps)-1, None
    while lo <= hi:
        mid = (lo+hi)//2
        if snaps[mid][0] < gdate_int:
            result = snaps[mid][1]
            lo = mid+1
        else:
            hi = mid-1
    return result


def load_kenpom_fanmatch(conn):
    """Load fanmatch predictions keyed by (game_date_str, home_team, away_team)."""
    if not tbl_exists(conn, 'kenpom_fanmatch'): return {}
    try:
        df = pd.read_sql("""
            SELECT season, game_date, home_team, away_team,
                   home_rank, away_rank, home_pred, away_pred,
                   home_wp, pred_tempo, thrill_score
            FROM kenpom_fanmatch
        """, conn)
        df['home_team'] = df['home_team'].apply(norm)
        df['away_team'] = df['away_team'].apply(norm)
        index = {}
        for _, row in df.iterrows():
            key = (row['game_date'], row['home_team'], row['away_team'])
            index[key] = row.to_dict()
        print(f"  KenPom fanmatch: {len(index):,} game predictions")
        return index
    except Exception as e:
        print(f"  KenPom fanmatch WARNING: {e}")
        return {}


def load_rolling(conn):
    """Load rolling_efficiency into indexed dict keyed by (game_date_str, team)."""
    if not tbl_exists(conn, 'rolling_efficiency'): return {}
    try:
        df = pd.read_sql("SELECT * FROM rolling_efficiency", conn)
        df['team'] = df['team'].apply(norm)
        index = {}
        for _, row in df.iterrows():
            index[(row['game_date'], row['team'])] = row.to_dict()
        print(f"  Rolling efficiency: {len(index):,} (date,team) entries")
        return index
    except Exception as e:
        print(f"  Rolling efficiency WARNING: {e}")
        return {}
