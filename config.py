"""
Central configuration for the weather trading bot.
All credentials are loaded from .env — never hardcoded here.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Target stations (ICAO → GHCND mapping for NOAA CDO cross-validation)
# ---------------------------------------------------------------------------
STATIONS = ["KJFK", "KORD", "KMIA", "KDFW", "KLAX", "KATL", "KDEN", "KHOU"]

GHCND_IDS = {
    "KJFK": "USW00094789",
    "KORD": "USW00094846",
    "KMIA": "USW00012839",
    "KDFW": "USW00003927",
    "KLAX": "USW00023174",
    "KATL": "USW00013874",
    "KDEN": "USW00023062",
    "KHOU": "USW00012918",
}

# Station coordinates (lat, lon) for Open-Meteo and other coordinate-based APIs
STATION_COORDS = {
    "KJFK": (40.6413, -73.7781),
    "KORD": (41.9742, -87.9073),
    "KMIA": (25.7959, -80.2870),
    "KDFW": (32.8998, -97.0403),
    "KLAX": (33.9425, -118.4081),
    "KATL": (33.6407, -84.4277),
    "KDEN": (39.8561, -104.6737),
    "KHOU": (29.6454, -95.2789),
}

# Station timezones (for display conversion from UTC)
STATION_TIMEZONES = {
    "KJFK": "America/New_York",
    "KORD": "America/Chicago",
    "KMIA": "America/New_York",
    "KDFW": "America/Chicago",
    "KLAX": "America/Los_Angeles",
    "KATL": "America/New_York",
    "KDEN": "America/Denver",
    "KHOU": "America/Chicago",
}

# WFO mapping for IEM AFM archive (station → WFO code)
WFO_MAP = {
    "KJFK": "OKX",
    "KORD": "LOT",
    "KMIA": "MFL",
    "KDFW": "FWD",
    "KLAX": "LOX",
    "KATL": "FFC",
    "KDEN": "BOU",
    "KHOU": "HGX",
}

# ---------------------------------------------------------------------------
# Historical data range (15 years)
# ---------------------------------------------------------------------------
START_DATE = "2010-01-01"
END_DATE   = "2024-12-31"

# ---------------------------------------------------------------------------
# Data paths
# ---------------------------------------------------------------------------
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
DATA_DIR   = os.path.join(BASE_DIR, "data")
RAW_DIR    = os.path.join(DATA_DIR, "raw")
LOGS_DIR   = os.path.join(BASE_DIR, "logs")

OBS_PARQUET      = os.path.join(DATA_DIR, "obs_daily.parquet")
Z500_PARQUET     = os.path.join(DATA_DIR, "z500_anomaly.parquet")
PATTERNS_PARQUET = os.path.join(DATA_DIR, "pattern_labels.parquet")
CENTROIDS_PKL    = os.path.join(DATA_DIR, "cluster_centroids.pkl")
FCST_PARQUET     = os.path.join(DATA_DIR, "model_fcst.parquet")
BIAS_PARQUET     = os.path.join(DATA_DIR, "bias_table.parquet")

# ---------------------------------------------------------------------------
# External API credentials (loaded from .env)
# ---------------------------------------------------------------------------
NOAA_CDO_TOKEN = os.getenv("NOAA_CDO_TOKEN", "")
KALSHI_API_KEY = os.getenv("KALSHI_API_KEY", "")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")  # path to service account JSON

# ---------------------------------------------------------------------------
# NCEP/NCAR Reanalysis OPeNDAP
# ---------------------------------------------------------------------------
NCEP_OPENDAP_TEMPLATE = (
    "https://psl.noaa.gov/thredds/dodsC/Datasets/"
    "ncep.reanalysis.dailyavgs/pressure/hgt.{year}.nc"
)
NCEP_HTTP_TEMPLATE = (
    "https://downloads.psl.noaa.gov/Datasets/"
    "ncep.reanalysis.dailyavgs/pressure/hgt.{year}.nc"
)

# 500mb domain: 20–55°N, 130–60°W (NCEP 0-360 lon: 230–300°E)
LAT_BOUNDS = (20.0, 55.0)
LON_BOUNDS = (230.0, 300.0)

# ---------------------------------------------------------------------------
# Pattern clustering
# ---------------------------------------------------------------------------
SEASONS = {
    "DJF": [12, 1, 2],
    "MAM": [3, 4, 5],
    "JJA": [6, 7, 8],
    "SON": [9, 10, 11],
}
K_RANGE        = range(8, 13)   # 8 to 12 inclusive
CLUSTER_RANDOM_STATE = 42
CLUSTER_N_INIT = 20

# ---------------------------------------------------------------------------
# Bias table
# ---------------------------------------------------------------------------
MODEL_BIN_SIZE = 2    # degrees F
MIN_N_OBS      = 10   # minimum observations to trust a bias cell

# ---------------------------------------------------------------------------
# Kalshi market structure
# ---------------------------------------------------------------------------
# Markets open the day before at 10:00 AM EDT
# Last trading time: 11:59 PM ET on the event day
# Settlement: first 7:00 or 8:00 AM ET after LCD data releases

# Bucket structure: 2°F wide, odd-start (matching confirmed Kalshi format)
# e.g. "68 or below", "69 to 70", "71 to 72", "73 to 74", "75 to 76", "77 or above"
# Bucket center naming: B68, B69.5, B71.5, B73.5, B75.5, B77
KALSHI_BUCKET_LOWER_TAIL = 68        # "68 or below"
KALSHI_BUCKET_UPPER_TAIL = 77        # "77 or above"
KALSHI_BUCKET_STARTS     = [69, 71, 73, 75]   # lower bounds of interior buckets
KALSHI_BUCKET_CENTERS    = {         # bucket_lower → Kalshi center label
    68: "68",
    69: "69.5",
    71: "71.5",
    73: "73.5",
    75: "75.5",
    77: "77",
}

# Market series format: KXHIGH{4-char-station} e.g. KXHIGHLAX
# Event format:  KXHIGHLAX-26APR08
# Market format: KXHIGHLAX-26APR08-B71.5
KALSHI_SERIES_PREFIX = "KXHIGH"

# Kalshi API base URLs
KALSHI_DEMO_URL = "https://demo-api.kalshi.co/trade-api/v2"
KALSHI_LIVE_URL = "https://api.elections.kalshi.com/trade-api/v2"

# Coastal stations — eligible for marine fog penalty
COASTAL_STATIONS = {"KLAX", "KJFK", "KMIA"}

# Early exit: if position bid reaches this level and high is locked in bucket
EARLY_EXIT_BID_THRESHOLD = 0.85   # exit at 85¢ rather than wait for LCD

# ---------------------------------------------------------------------------
# Live data sources
# ---------------------------------------------------------------------------
# NOMADS GFS OPeNDAP for live 500mb analysis (00Z cycle)
NOMADS_GFS_URL = (
    "https://nomads.ncep.noaa.gov/dods/gfs_0p25/gfs{date}/gfs_0p25_00z"
)
# Open-Meteo forecast API (current day, not archive)
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

# METAR/TAF source
AVWX_TAF_URL  = "https://aviationweather.gov/api/data/taf"
AVWX_METAR_URL = "https://aviationweather.gov/api/data/metar"

# ---------------------------------------------------------------------------
# Risk parameters
# ---------------------------------------------------------------------------
STARTING_BANKROLL        = 100.0   # paper trading bankroll in USD
DAILY_LOSS_LIMIT_PCT     = 0.10    # halt bot if daily loss exceeds 10%
MAX_STAKE_PCT            = 0.02    # max 2% of bankroll per trade (fractional Kelly)
MAX_EXPOSURE_PCT         = 0.50    # max 50% of bankroll in open positions
STOP_LOSS_PCT            = 0.40    # close position if value falls to 40% of entry
REVERSAL_EDGE_THRESHOLD  = -0.15   # signal reversal stop: exit and do not re-enter
PROFIT_REVERSAL_THRESHOLD = 0.10   # early profit exit threshold
MIN_KELLY_STAKE          = 1.00    # minimum stake in USD to enter a trade

# Confidence-scaled Kelly: scale stake down when pattern match is uncertain
CONFIDENCE_KELLY_SCALE = {
    "high":   1.00,   # close to cluster center — full Kelly sizing
    "medium": 0.75,   # reasonable match — 75% of Kelly
    "low":    0.50,   # unusual synoptic territory — half Kelly, proceed with caution
}

# Intraday temperature exit guards (uses IEM 1-min running max)
OVERSHOOT_EXIT_BUFFER_F       = 0.5  # exit if running_max >= bucket_upper − 0.5°F (before peak hour)
UNDERSHOOT_EXIT_BUFFER_F      = 2.0  # exit if running_max < bucket_lower − 2°F (after peak hour)
UNDERSHOOT_WARNING_LEAD_HOURS = 1    # warn this many hours before peak hour if tracking low

# Adjacent bucket expansion (auto-entry when forecast shifts one bucket)
EXPANSION_EDGE_MIN          = 0.18   # new bucket must clear this edge to trigger expansion
EXPANSION_CURRENT_EDGE_MAX  = 0.05   # expand only when current bucket edge has degraded to this
MAX_STATION_POSITIONS       = 2      # max simultaneous positions per station

# Significant reposition (2-step and 3+ step bucket shifts — close old, open new)
SIGNIFICANT_REPOSITION_EDGE_MIN = 0.22   # 2-step shift: higher bar than adjacent expansion
MAJOR_REPOSITION_EDGE_MIN       = 0.25   # 3+ step shift: highest bar, large forecast revision

# Liquidity guards — applied before every order
MIN_MARKET_VOLUME  = 50    # minimum contracts traded in this market before we enter
MAX_BID_ASK_SPREAD = 0.20  # max acceptable bid-ask spread (20¢); wider = illiquid, skip

# Signal freshness — re-run calculation if last signal is older than this
STALE_SIGNAL_HOURS = 4

# Market schedule (UTC)
MARKET_OPEN_UTC_HOUR   = 14   # Kalshi opens Day-1 markets at 14:00 UTC (10 AM EDT)
MARKET_OPEN_UTC_MINUTE = 5    # fire 5 min after open to let liquidity settle
SETTLEMENT_SWEEP_UTC_HOUR = 9 # check for overnight settlements at 09:00 UTC

# Tier 3 model data retry — handles delayed NWS/Open-Meteo updates
TIER3_RETRY_INTERVAL_MIN = 15   # wait this long between retries
TIER3_MAX_RETRIES        = 3    # give up after 3 attempts (45 min total window)

# ---------------------------------------------------------------------------
# Error alerting (optional — configure via .env)
# ---------------------------------------------------------------------------
ALERT_EMAIL_TO      = os.getenv("ALERT_EMAIL_TO", "")
ALERT_EMAIL_FROM    = os.getenv("ALERT_EMAIL_FROM", "")
ALERT_SMTP_HOST     = os.getenv("ALERT_SMTP_HOST", "smtp.gmail.com")
ALERT_SMTP_PORT     = int(os.getenv("ALERT_SMTP_PORT", "587"))
ALERT_SMTP_PASSWORD = os.getenv("ALERT_SMTP_PASSWORD", "")

# ---------------------------------------------------------------------------
# Station peak heating hours — 90th percentile by station and month
# Source: run python scripts/build_peak_hours.py and paste output here
# Default: 15 (3 PM local) — safe placeholder until real data is available
# ---------------------------------------------------------------------------
STATION_PEAK_HOURS: dict[str, dict[int, int]] = {
    "KJFK": {1: 15, 2: 15, 3: 15, 4: 15, 5: 15, 6: 15, 7: 15, 8: 15, 9: 15, 10: 15, 11: 15, 12: 15},
    "KORD": {1: 15, 2: 15, 3: 15, 4: 15, 5: 15, 6: 15, 7: 15, 8: 15, 9: 15, 10: 15, 11: 15, 12: 15},
    "KMIA": {1: 15, 2: 15, 3: 15, 4: 15, 5: 15, 6: 15, 7: 15, 8: 15, 9: 15, 10: 15, 11: 15, 12: 15},
    "KDFW": {1: 15, 2: 15, 3: 15, 4: 15, 5: 15, 6: 15, 7: 15, 8: 15, 9: 15, 10: 15, 11: 15, 12: 15},
    "KLAX": {1: 15, 2: 15, 3: 15, 4: 15, 5: 15, 6: 15, 7: 15, 8: 15, 9: 15, 10: 15, 11: 15, 12: 15},
    "KATL": {1: 15, 2: 15, 3: 15, 4: 15, 5: 15, 6: 15, 7: 15, 8: 15, 9: 15, 10: 15, 11: 15, 12: 15},
    "KDEN": {1: 15, 2: 15, 3: 15, 4: 15, 5: 15, 6: 15, 7: 15, 8: 15, 9: 15, 10: 15, 11: 15, 12: 15},
    "KHOU": {1: 15, 2: 15, 3: 15, 4: 15, 5: 15, 6: 15, 7: 15, 8: 15, 9: 15, 10: 15, 11: 15, 12: 15},
}

# ---------------------------------------------------------------------------
# Trading mode
# ---------------------------------------------------------------------------
USE_DEMO = True   # True = paper trading, False = live trading

# ---------------------------------------------------------------------------
# Edge threshold — hybrid weather penalty system
# ---------------------------------------------------------------------------
EDGE_THRESHOLD_BASE = 0.12   # base edge required with no weather penalty

# TAF weather penalty multipliers (worst condition in 12Z-00Z window)
WEATHER_PENALTY = {
    "clear":       1.0,   # SKC / CLR / FEW
    "scattered":   1.2,   # SCT only, no precip
    "broken":      1.5,   # BKN / OVC, no precip
    "marine_fog":  1.8,   # BR / FG at coastal stations
    "convective":  2.5,   # VCTS / TS in TAF
    "precip":      3.0,   # RA / SN / FZRA active
    "hard_skip":   None,  # FZRA+OVC / heavy SN / ICE — never trade
}

# bias_std gate — independent secondary check
STD_GATE_VALUE = 4.5    # if bias_std exceeds this, apply floor
STD_GATE_FLOOR = 0.30   # minimum effective threshold when gate fires

# ---------------------------------------------------------------------------
# Scheduler tier intervals
# ---------------------------------------------------------------------------
TIER1_INTERVAL_SECONDS = 300    # 5 min — TAF AMD / SPECI
TIER2_INTERVAL_SECONDS = 1800   # 30 min — METAR running high
# Tier 3 runs on GFS cycle alignment (every 6hrs + 30min offset)

# ---------------------------------------------------------------------------
# Google Sheets tab names
# ---------------------------------------------------------------------------
SHEET_TABS = {
    "dashboard":      "Dashboard",
    "trade_log":      "Trade Log",
    "skipped":        "Skipped Signals",
    "model_accuracy": "Model Accuracy",
    "eod_summary":    "EOD Summary",
}

# ---------------------------------------------------------------------------
# Aliases for backward compatibility
# ---------------------------------------------------------------------------
CLUSTER_PKL = CENTROIDS_PKL   # pattern_classifier.py uses this name
