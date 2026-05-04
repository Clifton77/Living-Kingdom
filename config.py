"""
Central configuration for the weather trading bot.
All credentials are loaded from .env — never hardcoded here.
"""
import os
from datetime import date as _date, timedelta as _timedelta
try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*args, **kwargs):
        return False

load_dotenv()

# ---------------------------------------------------------------------------
# Target stations
# These are the KALSHI-SIDE labels used as keys throughout the bot.
# They do NOT always match the actual NWS settlement station (see below).
# ---------------------------------------------------------------------------
STATIONS = [
    # Original 8
    "KJFK", "KORD", "KMIA", "KDFW", "KLAX", "KATL", "KDEN", "KHOU",
    # Discovered Apr 2026 via discover_markets.py
    "KAUS", "KPHL", "KBOS", "KDCA", "KLAS", "KMSP", "KMSY",
    "KOKC", "KPHX", "KSAT", "KSEA", "KSFO",
]

# ---------------------------------------------------------------------------
# Kalshi settlement stations (confirmed from rules_primary, Apr 2026)
# CRITICAL: The bias model and all pipeline scripts must use these stations
# for observation and forecast data — NOT the ICAO codes in STATIONS above.
#
# Confirmed mismatches (VERIFY new stations from Kalshi rules page before live trading):
#   KJFK → KXHIGHNY   settles on Central Park, NY (KNYC), NOT JFK Airport
#   KORD → KXHIGHCHI  settles on Chicago Midway (KMDW),  NOT O'Hare
#   KHOU → KXHIGHTHOU settles on Houston-Hobby (KHOU) — confirmed from market rules
#
# New stations (Apr 2026) — settlement inferred from rules text, verify before trading:
#   KAUS: "Austin Bergstrom" in rules  → KAUS (direct match, low mismatch risk)
#   KPHL: "Philadelphia International" → KPHL (direct match)
#   KBOS: "Boston" → KBOS (Logan, standard NWS CLI station)
#   KDCA: "Washington DC" → KDCA (Reagan National, standard NWS CLIDCA)
#   KLAS: "Las Vegas" → KLAS (direct match)
#   KMSP: "Minneapolis" → KMSP (direct match)
#   KMSY: "New Orleans" → KMSY (Louis Armstrong, standard NWS CLIMSY)
#   KOKC: "Oklahoma City" → KOKC (Will Rogers, direct match)
#   KPHX: "Phoenix" → KPHX (Sky Harbor, direct match)
#   KSAT: "San Antonio" → KSAT (direct match)
#   KSEA: "Seattle" → KSEA (Sea-Tac, direct match)
#   KSFO: "San Francisco" → KSFO (direct match)
# ---------------------------------------------------------------------------
KALSHI_SETTLEMENT_STATION = {
    # Original 8 — confirmed
    "KJFK": "KNYC",   # Central Park, NY            — NWS WBAN: 94728
    "KORD": "KMDW",   # Chicago Midway              — NWS WBAN: 14819
    "KMIA": "KMIA",   # Miami International         — confirmed match
    "KDFW": "KDFW",   # Dallas/Fort Worth Intl      — inferred match
    "KLAX": "KLAX",   # Los Angeles Airport         — confirmed match
    "KATL": "KATL",   # Atlanta Hartsfield          — inferred match
    "KDEN": "KDEN",   # Denver International        — inferred match
    "KHOU": "KHOU",   # Houston Hobby               — confirmed from market rules
    # New 12 — inferred from rules text, verify from Kalshi rules page
    "KAUS": "KAUS",   # Austin Bergstrom            — "Austin Bergstrom" in rules
    "KPHL": "KPHL",   # Philadelphia Intl           — "Philadelphia International" in rules
    "KBOS": "KBOS",   # Boston Logan                — "Boston" in rules
    "KDCA": "KDCA",   # DC Reagan National          — "Washington DC", NWS CLIDCA
    "KLAS": "KLAS",   # Las Vegas                   — "Las Vegas" in rules
    "KMSP": "KMSP",   # Minneapolis-St. Paul        — "Minneapolis" in rules
    "KMSY": "KMSY",   # New Orleans                 — "New Orleans", NWS CLIMSY
    "KOKC": "KOKC",   # Oklahoma City               — "Oklahoma City" in rules
    "KPHX": "KPHX",   # Phoenix Sky Harbor          — "Phoenix" in rules
    "KSAT": "KSAT",   # San Antonio Intl            — "San Antonio" in rules
    "KSEA": "KSEA",   # Seattle-Tacoma              — "Seattle" in rules
    "KSFO": "KSFO",   # San Francisco Intl          — "San Francisco" in rules
}

# GHCND IDs for the SETTLEMENT stations (used by build_obs_database.py)
# Key = settlement station ICAO (from KALSHI_SETTLEMENT_STATION values)
GHCND_IDS = {
    # Original 8
    "KNYC": "USW00094728",   # Central Park, New York
    "KMDW": "USW00014819",   # Chicago Midway
    "KMIA": "USW00012839",   # Miami International
    "KDFW": "USW00003927",   # Dallas/Fort Worth International
    "KLAX": "USW00023174",   # Los Angeles International
    "KATL": "USW00013874",   # Atlanta Hartsfield-Jackson
    "KDEN": "USW00023062",   # Denver International
    "KHOU": "USW00012918",   # Houston Hobby
    # New 12
    "KAUS": "USW00013904",   # Austin Bergstrom
    "KPHL": "USW00013739",   # Philadelphia International
    "KBOS": "USW00014739",   # Boston Logan
    "KDCA": "USW00013743",   # DC Reagan National
    "KLAS": "USW00023169",   # Las Vegas
    "KMSP": "USW00014922",   # Minneapolis-St. Paul
    "KMSY": "USW00012916",   # New Orleans
    "KOKC": "USW00013967",   # Oklahoma City Will Rogers
    "KPHX": "USW00023183",   # Phoenix Sky Harbor
    "KSAT": "USW00012921",   # San Antonio International
    "KSEA": "USW00024233",   # Seattle-Tacoma
    "KSFO": "USW00023234",   # San Francisco International
}

# Station coordinates (lat, lon) — settlement stations for Open-Meteo forecasts
STATION_COORDS = {
    # Original 8
    "KNYC": (40.7789,  -73.9692),   # Central Park, New York
    "KMDW": (41.7862,  -87.7525),   # Chicago Midway
    "KMIA": (25.7959,  -80.2870),   # Miami International
    "KDFW": (32.8998,  -97.0403),   # Dallas/Fort Worth International
    "KLAX": (33.9425, -118.4081),   # Los Angeles International
    "KATL": (33.6407,  -84.4277),   # Atlanta Hartsfield-Jackson
    "KDEN": (39.8561, -104.6737),   # Denver International
    "KHOU": (29.6454,  -95.2789),   # Houston Hobby
    # New 12
    "KAUS": (30.1945,  -97.6699),   # Austin Bergstrom
    "KPHL": (39.8721,  -75.2411),   # Philadelphia International
    "KBOS": (42.3643,  -71.0052),   # Boston Logan
    "KDCA": (38.8521,  -77.0377),   # DC Reagan National
    "KLAS": (36.0840, -115.1537),   # Las Vegas
    "KMSP": (44.8848,  -93.2223),   # Minneapolis-St. Paul
    "KMSY": (29.9934,  -90.2580),   # New Orleans
    "KOKC": (35.3931,  -97.6007),   # Oklahoma City Will Rogers
    "KPHX": (33.4373, -112.0078),   # Phoenix Sky Harbor
    "KSAT": (29.5337,  -98.4698),   # San Antonio International
    "KSEA": (47.4502, -122.3088),   # Seattle-Tacoma
    "KSFO": (37.6213, -122.3790),   # San Francisco International
}

# Human-readable city names for display (Kalshi station label → city)
STATION_CITY_NAMES = {
    "KJFK": "New York",
    "KORD": "Chicago",
    "KMIA": "Miami",
    "KDFW": "Dallas",
    "KLAX": "Los Angeles",
    "KATL": "Atlanta",
    "KDEN": "Denver",
    "KHOU": "Houston",
    "KAUS": "Austin",
    "KPHL": "Philadelphia",
    "KBOS": "Boston",
    "KDCA": "Washington DC",
    "KLAS": "Las Vegas",
    "KMSP": "Minneapolis",
    "KMSY": "New Orleans",
    "KOKC": "Oklahoma City",
    "KPHX": "Phoenix",
    "KSAT": "San Antonio",
    "KSEA": "Seattle",
    "KSFO": "San Francisco",
}

# Station timezones (for display conversion from UTC)
STATION_TIMEZONES = {
    # Original 8
    "KJFK": "America/New_York",
    "KORD": "America/Chicago",
    "KMIA": "America/New_York",
    "KDFW": "America/Chicago",
    "KLAX": "America/Los_Angeles",
    "KATL": "America/New_York",
    "KDEN": "America/Denver",
    "KHOU": "America/Chicago",
    # New 12
    "KAUS": "America/Chicago",
    "KPHL": "America/New_York",
    "KBOS": "America/New_York",
    "KDCA": "America/New_York",
    "KLAS": "America/Los_Angeles",
    "KMSP": "America/Chicago",
    "KMSY": "America/Chicago",
    "KOKC": "America/Chicago",
    "KPHX": "America/Phoenix",      # Arizona — no DST
    "KSAT": "America/Chicago",
    "KSEA": "America/Los_Angeles",
    "KSFO": "America/Los_Angeles",
}

# WFO mapping for IEM AFM archive (Kalshi station label → WFO code)
# KNYC and KMDW share the same WFO as KJFK and KORD respectively.
WFO_MAP = {
    # Original 8
    "KJFK": "OKX",   # OKX covers both JFK and Central Park
    "KORD": "LOT",   # LOT covers both O'Hare and Midway
    "KMIA": "MFL",
    "KDFW": "FWD",
    "KLAX": "LOX",
    "KATL": "FFC",
    "KDEN": "BOU",
    "KHOU": "HGX",
    # New 12
    "KAUS": "EWX",   # Austin/San Antonio
    "KPHL": "PHI",   # Philadelphia/Wilmington
    "KBOS": "BOX",   # Boston/Taunton
    "KDCA": "LWX",   # Baltimore/Washington
    "KLAS": "VEF",   # Las Vegas
    "KMSP": "MPX",   # Minneapolis/St. Paul
    "KMSY": "LIX",   # New Orleans
    "KOKC": "OUN",   # Oklahoma City
    "KPHX": "PSR",   # Phoenix
    "KSAT": "EWX",   # Austin/San Antonio (same WFO as KAUS)
    "KSEA": "SEW",   # Seattle/Tacoma
    "KSFO": "MTR",   # San Francisco Bay Area
}

# ---------------------------------------------------------------------------
# Historical data range (15 years)
# ---------------------------------------------------------------------------
START_DATE = "2010-01-01"
END_DATE   = (_date.today() - _timedelta(days=1)).isoformat()  # yesterday, updated at runtime

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
# Peak heating hours — built by scripts/build_peak_hours.py
HOURLY_OBS_PARQUET = os.path.join(DATA_DIR, "hourly_obs.parquet")    # raw IEM hourly obs cache
PEAK_HOURS_PARQUET = os.path.join(DATA_DIR, "peak_hours.parquet")    # DOY-smoothed p90 curve

# ---------------------------------------------------------------------------
# Trading mode
# ---------------------------------------------------------------------------
USE_DEMO = os.getenv("USE_DEMO", "true").lower() != "false"   # True = paper trading on demo.kalshi.co
DRY_RUN  = os.getenv("DRY_RUN",  "false").lower() == "true"  # True = live API, no order placement

# ---------------------------------------------------------------------------
# External API credentials (loaded from .env)
#
# Kalshi demo and live are SEPARATE accounts with separate credentials.
# Set USE_DEMO above, then provide the matching keys below.
#
# Demo credentials  → created at https://demo.kalshi.co → Settings → API Keys
# Live credentials  → created at https://kalshi.com     → Settings → API Keys
#
# Preferred: use mode-specific keys so flipping USE_DEMO switches everything:
#   KALSHI_DEMO_API_KEY, KALSHI_DEMO_PRIVATE_KEY_PATH
#   KALSHI_LIVE_API_KEY, KALSHI_LIVE_PRIVATE_KEY_PATH
#
# Fallback: if mode-specific keys are absent, KALSHI_API_KEY /
#   KALSHI_PRIVATE_KEY_PATH are used (single-credential setup).
# ---------------------------------------------------------------------------
NOAA_CDO_TOKEN = os.getenv("NOAA_CDO_TOKEN", "")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")  # path to service account JSON

if USE_DEMO:
    KALSHI_API_KEY = (
        os.getenv("KALSHI_DEMO_API_KEY")
        or os.getenv("KALSHI_API_KEY", "")
    )
    KALSHI_PRIVATE_KEY_PATH = (
        os.getenv("KALSHI_DEMO_PRIVATE_KEY_PATH")
        or os.getenv("KALSHI_PRIVATE_KEY_PATH", "")
    )
else:
    KALSHI_API_KEY = (
        os.getenv("KALSHI_LIVE_API_KEY")
        or os.getenv("KALSHI_API_KEY", "")
    )
    KALSHI_PRIVATE_KEY_PATH = (
        os.getenv("KALSHI_LIVE_PRIVATE_KEY_PATH")
        or os.getenv("KALSHI_PRIVATE_KEY_PATH", "")
    )

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

# Per-station Kalshi series tickers (confirmed from live API series list Apr 2026).
# Each city has a unique series ticker — there is NO universal prefix.
# Event tickers are: {series_ticker}-{YYMONDD}  e.g. KXHIGHLAX-26APR14
# Market tickers:    {series_ticker}-{YYMONDD}-B{center}  e.g. KXHIGHLAX-26APR14-B80.5
# Bucket centers use center of 2°F range (e.g. 80-81° → B80.5).
# Tail buckets (floor/ceiling) use their bound value (e.g. B77, B86).
# Bucket ranges shift by station and season — always discover dynamically via API.
KALSHI_STATION_SERIES = {
    # Original 8
    "KJFK": "KXHIGHNY",     # NYC (settles on Central Park KNYC)
    "KORD": "KXHIGHCHI",    # Chicago (settles on Midway KMDW)
    "KMIA": "KXHIGHMIA",    # Miami International
    "KDFW": "KXHIGHTDAL",   # Dallas/Fort Worth
    "KLAX": "KXHIGHLAX",    # Los Angeles
    "KATL": "KXHIGHTATL",   # Atlanta
    "KDEN": "KXHIGHDEN",    # Denver
    "KHOU": "KXHIGHTHOU",   # Houston Hobby
    # New 12 — discovered Apr 2026
    "KAUS": "KXHIGHAUS",    # Austin Bergstrom
    "KPHL": "KXHIGHPHIL",   # Philadelphia International
    "KBOS": "KXHIGHTBOS",   # Boston Logan
    "KDCA": "KXHIGHTDC",    # Washington DC Reagan National
    "KLAS": "KXHIGHTLV",    # Las Vegas
    "KMSP": "KXHIGHTMIN",   # Minneapolis-St. Paul
    "KMSY": "KXHIGHTNOLA",  # New Orleans
    "KOKC": "KXHIGHTOKC",   # Oklahoma City
    "KPHX": "KXHIGHTPHX",   # Phoenix Sky Harbor
    "KSAT": "KXHIGHTSATX",  # San Antonio
    "KSEA": "KXHIGHTSEA",   # Seattle-Tacoma
    "KSFO": "KXHIGHTSFO",   # San Francisco
}

# Kept for legacy reference only — actual bucket ranges are station/date dependent.
# Bot always uses dynamic discovery (get_markets_for_station_date) not these constants.
KALSHI_SERIES_PREFIX = "KXHIGH"   # deprecated — use KALSHI_STATION_SERIES

# Kalshi API base URLs
KALSHI_DEMO_URL = "https://demo-api.kalshi.co/trade-api/v2"
KALSHI_LIVE_URL = "https://api.elections.kalshi.com/trade-api/v2"

# Coastal stations — eligible for marine fog penalty
# KJFK is the Kalshi label but the settlement station is Central Park (KNYC),
# which is inland. Keep KJFK here so the signal engine can still apply the
# NYC marine fog modifier (fog from the ocean still affects Central Park temps).
COASTAL_STATIONS = {"KLAX", "KJFK", "KMIA", "KBOS", "KSFO", "KSEA"}

# Early exit: if position bid reaches this level and high is locked in bucket
EARLY_EXIT_BID_THRESHOLD = 0.85   # exit at 85¢ rather than wait for LCD

# ---------------------------------------------------------------------------
# Live data sources
# ---------------------------------------------------------------------------
# NOMADS GFS OPeNDAP for live 500mb analysis (00Z cycle)
NOMADS_GFS_URL = (
    "https://nomads.ncep.noaa.gov/dods/gfs_0p25/gfs{date}/gfs_0p25_00z"
)
# Open-Meteo forecast API — replaced by Herbie (NOAA-direct, no throttling)
# OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

# Forecast blending — NWS AFM + NBM
NWS_BLEND_WEIGHT    = 0.60   # weight for NWS human-adjusted AFM
NBM_BLEND_WEIGHT    = 0.40   # weight for NBM (HRRR/GFS/ECMWF calibrated blend)
NBM_DIVERGENCE_GATE = 3.0    # °F — skip if NWS vs NBM spread exceeds this

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
STOP_LOSS_PCT            = 0.65    # close position if value falls to 65% of entry
PROFIT_REVERSAL_THRESHOLD = 0.10   # early profit exit threshold
MIN_KELLY_STAKE          = 1.00    # minimum stake in USD to enter a trade
MIN_PROB_RATIO           = 0.75   # used for logging only — buckets below this shown as low-prob
MIN_BUCKET_PROB          = 0.05   # minimum model probability for a bucket to be eligible for selection
MOS_DIVERGENCE_THRESHOLD = 4.0    # skip if NWS and GFS-MOS disagree by more than this (°F)

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
MAX_BID_ASK_SPREAD = 0.20  # max acceptable bid-ask spread (20¢); wider = illiquid, retry

# Liquidity retry — when spread/volume gates fail, retry before giving up
LIQUIDITY_RETRY_INTERVAL_MIN = 15   # minutes between liquidity retries
LIQUIDITY_MAX_RETRIES        = 4    # give up after 4 attempts (~1 hour total window)

# Signal freshness — re-run calculation if last signal is older than this
STALE_SIGNAL_HOURS = 4

# Price scan is merged into Tier 1 (runs at TIER1_INTERVAL_SECONDS cadence).
# Kept for config compatibility; not used as a separate scheduler job.
PRICE_SCAN_INTERVAL_MIN = 5    # retained for reference; Tier 1 handles this inline

# Market schedule (UTC)
SETTLEMENT_SWEEP_UTC_HOUR = 9 # check for overnight settlements at 09:00 UTC

# Same-day entry gate: no entries before the 12Z Tier 3 model run (12:30 UTC = 7:30 AM CT).
# Entries are only allowed for the settlement date's own calendar day — no next-day pre-entry.
SAME_DAY_ENTRY_OPEN_UTC_HOUR   = 12
SAME_DAY_ENTRY_OPEN_UTC_MINUTE = 30

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
    "KAUS": {1: 15, 2: 15, 3: 15, 4: 15, 5: 15, 6: 15, 7: 15, 8: 15, 9: 15, 10: 15, 11: 15, 12: 15},
    "KPHL": {1: 15, 2: 15, 3: 15, 4: 15, 5: 15, 6: 15, 7: 15, 8: 15, 9: 15, 10: 15, 11: 15, 12: 15},
    "KBOS": {1: 15, 2: 15, 3: 15, 4: 15, 5: 15, 6: 15, 7: 15, 8: 15, 9: 15, 10: 15, 11: 15, 12: 15},
    "KDCA": {1: 15, 2: 15, 3: 15, 4: 15, 5: 15, 6: 15, 7: 15, 8: 15, 9: 15, 10: 15, 11: 15, 12: 15},
    "KLAS": {1: 15, 2: 15, 3: 15, 4: 15, 5: 15, 6: 15, 7: 15, 8: 15, 9: 15, 10: 15, 11: 15, 12: 15},
    "KMSP": {1: 15, 2: 15, 3: 15, 4: 15, 5: 15, 6: 15, 7: 15, 8: 15, 9: 15, 10: 15, 11: 15, 12: 15},
    "KMSY": {1: 15, 2: 15, 3: 15, 4: 15, 5: 15, 6: 15, 7: 15, 8: 15, 9: 15, 10: 15, 11: 15, 12: 15},
    "KOKC": {1: 15, 2: 15, 3: 15, 4: 15, 5: 15, 6: 15, 7: 15, 8: 15, 9: 15, 10: 15, 11: 15, 12: 15},
    "KPHX": {1: 15, 2: 15, 3: 15, 4: 15, 5: 15, 6: 15, 7: 15, 8: 15, 9: 15, 10: 15, 11: 15, 12: 15},
    "KSAT": {1: 15, 2: 15, 3: 15, 4: 15, 5: 15, 6: 15, 7: 15, 8: 15, 9: 15, 10: 15, 11: 15, 12: 15},
    "KSEA": {1: 15, 2: 15, 3: 15, 4: 15, 5: 15, 6: 15, 7: 15, 8: 15, 9: 15, 10: 15, 11: 15, 12: 15},
    "KSFO": {1: 15, 2: 15, 3: 15, 4: 15, 5: 15, 6: 15, 7: 15, 8: 15, 9: 15, 10: 15, 11: 15, 12: 15},
}

# ---------------------------------------------------------------------------
# Edge threshold — binary weather gate system
# ---------------------------------------------------------------------------
MIN_EDGE             = 0.06   # minimum edge (normalized) required to enter a trade
MIN_YES_ASK          = 0.04   # floor on Kalshi yes_ask — never enter below 4¢ (market pricing near-impossible)
MAX_YES_ASK          = 0.55   # ceiling on Kalshi yes_ask — never enter above this price (reduces stop-loss dollar risk)
MIN_MODEL_PROB_FOR_ENTRY = 0.45   # model Gaussian must assign ≥45% to the entry bucket (conviction floor)
MIN_YES_ASK_FOR_ENTRY    = 0.25   # market must not be deeply skeptical; below 25¢ = BUY_NO zone
MAX_DAILY_ENTRIES_PER_STATION = 2   # max new position opens per station per event date

# Late-entry Kelly scaling — reduce stake as peak hour approaches to reflect
# the narrowing profit window.  Applies to YES entries only.
LATE_ENTRY_KELLY_MIN_SCALE   = 0.25  # floor: never below 25% Kelly in the final hour
LATE_ENTRY_KELLY_WINDOW_HOURS = 4    # hours before peak where scaling begins

# Peak-window TAF scoring — how far around the expected high-temp hour to check
TAF_PRE_PEAK_WINDOW_HOURS  = 3   # hours before peak to include (approaching storm matters)
TAF_POST_PEAK_WINDOW_HOURS = 1   # hours after peak to include

# bias_std gate — skip when forecast uncertainty is too high (Kelly can't compensate for wrong bucket)
BIAS_STD_GATE = 5.0   # skip if bias_std exceeds this (°F)

# Sigma inflation — widen bias_std proportionally to NWS/NBM/MOS spread on the NWS path.
# Inactive on Phase4 path (NBM ensemble spread already encodes model disagreement there).
# 0.10 = a 5°F model spread inflates sigma by 50%. Calibratable.
SIGMA_SPREAD_SCALE = 0.10

# NBM ensemble spread calibration — scales NBM ens std dev to match the magnitude of
# historical forecast error (bias table ERA5 sigma). Mean NBM ens std dev ~1.68 degF;
# mean historical sigma ~3.5 degF → calibration ~2.0. Using 1.5 to avoid over-gating
# variable stations (KSEA, KORD). effective_sigma = max(bias_std, nbm_spread * scale).
NBM_SPREAD_CALIBRATION = 1.5

# Same-day entry cutoff: stop entering same-day markets this many hours before peak
ENTRY_CUTOFF_PRE_PEAK_HOURS = 2

# ---------------------------------------------------------------------------
# Scheduler tier intervals
# ---------------------------------------------------------------------------
TIER1_INTERVAL_SECONDS        = 300   # 5 min — METAR + exits + entries (sleep-based, post-ASOS-aligned)
TIER1_NEARHR_INTERVAL_SECONDS = 60    # fast-poll interval near top of hour to catch fresh hourly obs
TIER1_NEARHR_START_MINUTE     = 47    # begin fast polling at :47 past the hour
TIER1_NEARHR_END_MINUTE       = 5     # end fast polling at :05 of the next hour
TIER2_INTERVAL_SECONDS        = 600   # 10 min — TAF amendments + auto-close on signal flip
# Tier 3 runs on GFS cycle alignment (every 6hrs + 30min offset); no order execution

SNAPSHOT_INTERVAL_MIN = 15      # how often to log an intraday position snapshot while a trade is open

# ---------------------------------------------------------------------------
# Startup config validation — fail fast if a station is missing from any dict
# ---------------------------------------------------------------------------
def _validate_config():
    # Dicts keyed by Kalshi-side station label
    kalshi_keyed = {
        "KALSHI_SETTLEMENT_STATION": KALSHI_SETTLEMENT_STATION,
        "STATION_CITY_NAMES":        STATION_CITY_NAMES,
        "STATION_TIMEZONES":         STATION_TIMEZONES,
        "WFO_MAP":                   WFO_MAP,
        "KALSHI_STATION_SERIES":     KALSHI_STATION_SERIES,
        "STATION_PEAK_HOURS":        STATION_PEAK_HOURS,
    }
    for dict_name, d in kalshi_keyed.items():
        missing = [s for s in STATIONS if s not in d]
        if missing:
            raise RuntimeError(
                f"config.py: {dict_name} is missing entries for: {missing}"
            )
    # Dicts keyed by settlement station (KALSHI_SETTLEMENT_STATION values)
    settlement_stations = set(KALSHI_SETTLEMENT_STATION.values())
    settlement_keyed = {
        "GHCND_IDS":      GHCND_IDS,
        "STATION_COORDS": STATION_COORDS,
    }
    for dict_name, d in settlement_keyed.items():
        missing = [s for s in settlement_stations if s not in d]
        if missing:
            raise RuntimeError(
                f"config.py: {dict_name} is missing entries for settlement stations: {missing}"
            )

_validate_config()

# ---------------------------------------------------------------------------
# Google Sheets tab names
# ---------------------------------------------------------------------------
SHEET_TABS = {
    "dashboard":      "Dashboard",
    "trade_log":      "Trade Log",
    "skipped":        "Skipped Signals",
    "model_accuracy": "Model Accuracy",
    "eod_summary":    "EOD Summary",
    "snapshots":      "Position Snapshots",
}

# ---------------------------------------------------------------------------
# Web dashboard
# ---------------------------------------------------------------------------
DASHBOARD_HOST     = os.getenv("DASHBOARD_HOST", "0.0.0.0")
DASHBOARD_PORT     = int(os.getenv("DASHBOARD_PORT", "5000"))
DASHBOARD_USER     = os.getenv("DASHBOARD_USER", "admin")
DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "")   # required — set in .env

# ---------------------------------------------------------------------------
# Aliases for backward compatibility
# ---------------------------------------------------------------------------
CLUSTER_PKL = CENTROIDS_PKL   # pattern_classifier.py uses this name


# ---------------------------------------------------------------------------
# Settlement station helper
# ---------------------------------------------------------------------------
def settlement_station(kalshi_label: str) -> str:
    """
    Return the NWS settlement station ICAO for a given Kalshi station label.
    Pipeline scripts (build_obs_database, build_model_forecast_archive, etc.)
    should use this to fetch the correct historical data.

    Example:
        settlement_station("KJFK") → "KNYC"  (Central Park, not JFK Airport)
        settlement_station("KORD") → "KMDW"  (Midway, not O'Hare)
        settlement_station("KMIA") → "KMIA"  (match)
    """
    return KALSHI_SETTLEMENT_STATION.get(kalshi_label, kalshi_label)


# Convenience: list of unique settlement stations (for pipeline iteration)
SETTLEMENT_STATIONS = list(dict.fromkeys(
    KALSHI_SETTLEMENT_STATION[s] for s in STATIONS
))
