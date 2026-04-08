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
# Risk parameters
# ---------------------------------------------------------------------------
STARTING_BANKROLL        = 100.0   # paper trading bankroll in USD
DAILY_LOSS_LIMIT_PCT     = 0.10    # halt bot if daily loss exceeds 10%
MAX_STAKE_PCT            = 0.02    # max 2% of bankroll per trade (fractional Kelly)
MAX_EXPOSURE_PCT         = 0.50    # max 50% of bankroll in open positions
STOP_LOSS_PCT            = 0.40    # close position if value falls to 40% of entry
REVERSAL_EDGE_THRESHOLD  = -0.15   # signal reversal stop threshold
PROFIT_REVERSAL_THRESHOLD = 0.10   # early profit exit threshold
REPOSITION_CONFIDENCE_THRESHOLD = 2.0  # multiplier on normal entry threshold

# ---------------------------------------------------------------------------
# Trading mode
# ---------------------------------------------------------------------------
USE_DEMO = True   # True = paper trading, False = live trading

# Minimum edge required to enter a trade
MIN_ENTRY_EDGE = 0.12

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
