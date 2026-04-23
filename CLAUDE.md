# Living-Kingdom — Project Context for Claude

## What This Is
A weather-based Kalshi prediction market trading bot. It forecasts daily high temperatures for 20 US cities, computes the probability distribution across Kalshi's 2°F bucket markets, and places trades based on edge.

## Architecture Overview

```
run_pipeline.py          — one-shot pipeline runner (scripts 1-4 in order)
scheduler.py             — daily cron: builds signals, places trades at 10 AM ET
run.py                   — production entry point
dashboard.py             — Streamlit dashboard for monitoring
signal_engine.py         — main signal: blends forecast + pattern + bias + TAF
```

### Pipeline Scripts (run in order)
1. `scripts/build_obs_database.py`     — fetch NOAA CDO observed tmax → `obs_daily.parquet`
2. `scripts/build_500mb_database.py`   — fetch NCEP reanalysis → `z500_anomaly.parquet`
3. `scripts/build_pattern_clusters.py` — seasonal KMeans on z500 → `cluster_centroids.pkl` + `pattern_labels.parquet`
4. `scripts/build_bias_table.py`       — model forecast error table → `bias_table.parquet`
5. `scripts/build_model_forecast_archive.py` — GFS/NBM archive → `model_fcst.parquet`

### Key Support Scripts
- `scripts/pattern_classifier.py`  — live 500mb pattern classification
- `scripts/taf_interpreter.py`     — TAF fog/cloud penalty scorer
- `scripts/discover_markets.py`    — Kalshi market discovery (KXHIGH* series)
- `kalshi_client.py`               — Kalshi API wrapper (RSA auth)

## Stations (20 total)
Original 8: KJFK, KORD, KMIA, KDFW, KLAX, KATL, KDEN, KHOU
Added Apr 2026: KAUS, KPHL, KBOS, KDCA, KLAS, KMSP, KMSY, KOKC, KPHX, KSAT, KSEA, KSFO

**Critical settlement mismatches** (Kalshi settles on a different NWS station):
- KJFK → KNYC (Central Park, NY) — NOT JFK Airport
- KORD → KMDW (Chicago Midway) — NOT O'Hare

## Data Files (`data/`)
| File | Description |
|------|-------------|
| `obs_daily.parquet` | Historical observed tmax by station |
| `z500_anomaly.parquet` | 500mb height anomalies, 2.5° grid, 20-55°N 130-60°W |
| `pattern_labels.parquet` | Seasonal cluster assignments per day |
| `cluster_centroids.pkl` | Pickle: `{season: {scaler, model, k, feature_cols}}` |
| `model_fcst.parquet` | GFS/NBM forecast archive |
| `bias_table.parquet` | Model bias by station/bucket/season/pattern |

## Live Data Sources
| Source | Used For | Notes |
|--------|----------|-------|
| Open-Meteo forecast API | Live 500hPa z500 for pattern classification | Replaced NOMADS (retired SCN 25-81) |
| Open-Meteo forecast API | Daily high temperature forecast (primary) | `OPEN_METEO_FORECAST_URL` in config |
| GFS-MOS (NBM alt) | Secondary forecast signal | |
| Aviation Weather (aviationweather.gov) | TAF for fog/cloud penalty | |
| ASOS live | Intraday temperature tracking | |
| Kalshi API (live) | Market prices, order placement | RSA key auth |

## NOMADS Retirement (SCN 25-81) — RESOLVED
**Problem**: NOMADS GFS OPeNDAP (`/dods/` path) was permanently retired.  
**Solution (Option A)**: Replaced with Open-Meteo pressure-level API in `pattern_classifier.py`.  
- Queries the same 2.5° NCEP grid points (20-55°N, 130-60°W) used during training
- Open-Meteo returns raw geopotential height in gpm (same units as NCEP reanalysis)
- `_compute_anomaly()` subtracts seasonal climatological mean to match training features
- Fallback: most recent row of `z500_anomaly.parquet` if Open-Meteo fails

## Known Bugs Fixed
- `cluster_centroids.pkl` was missing `feature_cols` key → `KeyError` in `classify_pattern()`. Fixed in `build_pattern_clusters.py` (line 130).
- Old NOMADS fetch used `:.2f` column format vs training data's `:.1f` → grid mismatch. Fixed: Open-Meteo fetch uses `:.1f` throughout.

## Config Keys (config.py)
- `STATIONS` — list of 20 ICAO codes (Kalshi-side labels)
- `KALSHI_SETTLEMENT_STATION` — maps Kalshi ICAO → actual NWS settlement station
- `KALSHI_STATION_SERIES` — maps ICAO → Kalshi series ticker (e.g. KXHIGHLAX)
- `LAT_BOUNDS = (20.0, 55.0)`, `LON_BOUNDS = (230.0, 300.0)` — z500 domain (0-360 lon)
- `OPEN_METEO_FORECAST_URL` — `https://api.open-meteo.com/v1/forecast`
- `NOMADS_GFS_URL` — retired, kept in config for reference only

## Signal Engine Blend (signal_engine.py)
1. Open-Meteo forecast (primary)
2. GFS-MOS (secondary)
3. Pattern cluster bias adjustment
4. TAF fog/cloud penalty (coastal stations)
5. Intraday ASOS tracking

## Kalshi Market Structure
- Buckets: 2°F wide, odd-start (e.g. 69-70, 71-72 ...)
- Tail buckets: "68 or below" (B68), "77 or above" (B77) — vary by station/season
- Series tickers: per-station (no universal prefix) — always discover dynamically
- Event ticker format: `{series}-{YYMONDD}` e.g. `KXHIGHLAX-26APR14`
- Market ticker format: `{series}-{YYMONDD}-B{center}` e.g. `KXHIGHLAX-26APR14-B80.5`

## Branch
Active development: `claude/push-recent-changes-iMtPy`

---

## Pending Redesign — Signal Engine + Same-Day Markets (approved 2026-04-21)

### What Changes and Why
Current threshold system multiplies a 0.12 base edge by weather penalties (up to 3×), causing stations with genuine edge to fail silently. New design: edge only needs to beat $0.01 (1 cent). Weather is a binary gate — skip or hard-skip — not a multiplier. Same-day market trading is also being added (bot currently only trades tomorrow's market).

### 1. `utils/weather_penalty.py`
Replace `compute_effective_threshold()` + `WEATHER_PENALTY` multiplier dict with:
```python
def compute_weather_gate(taf_condition: str) -> str:
    if taf_condition in ("hard_skip", "precip", "convective"):
        return "hard_skip"
    if taf_condition in ("broken", "marine_fog"):
        return "skip"
    return "trade"   # scattered, clear
```

### 2. `scripts/signal_engine.py`
- Wire `compute_weather_gate()` in place of `compute_effective_threshold()`
- `hard_skip` → `_hard_skip_signal()`; `skip` → `_skip_signal()`
- Decision gate: `if top.edge >= MIN_EDGE (0.01): TRADE else: WATCH`
- Remove `STD_GATE_VALUE` / `STD_GATE_FLOOR` bias_std gate (Kelly self-regulates)
- Keep: MOS divergence gate (4°F), `MIN_KELLY_STAKE` ($1.00), bucket selection logic

### 3. `scheduler.py`
**Same-day trading:**
- Add `_get_entry_event_date(station, now_utc) -> date | None`
  - Before cutoff (peak_hour − 2h): return `date.today()`
  - After cutoff + after 14:05 UTC: return `date.today() + 1`
  - In gap: return `None` (no entries)
- Tier 3 loop: compute target date per-station using `_get_entry_event_date()`
- Tier 1 entry: use `sig.event_date` directly (not hardcoded `today+1`)

**Tier 2 AMD:**
- After `_execute_exit()` on AMD, write `HARD_SKIP` signal into `_latest_signals[station]`
- Blocks Tier 1 re-entry until next Tier 3 run

### 4. `config.py`
```python
MIN_EDGE = 0.01                     # 1-cent minimum edge to trade
ENTRY_CUTOFF_PRE_PEAK_HOURS = 2     # stop same-day entries N hours before peak
# Remove: WEATHER_PENALTY, EDGE_THRESHOLD_BASE, STD_GATE_VALUE, STD_GATE_FLOOR
```

### Key Functions to Reuse (unchanged)
- `get_peak_hour(station, event_date)` — `utils/peak_hours.py`
- `get_all_snapshots(station, event_date)` — `kalshi_client.py`
- `build_probability_distribution()` — `signal_engine.py:173`
- `kelly_stake()` — `signal_engine.py:657`
- `interpret_taf()` — `taf_interpreter.py`
- `_execute_exit()` — `scheduler.py:722`
