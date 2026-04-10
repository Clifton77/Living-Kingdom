"""
Dashboard preview server — shows the real UI with mock data.
No scheduler, no data pipeline, no API keys required.
Run: python preview_dashboard.py
"""

from flask import Flask, render_template, jsonify, Response
import json, time, threading, queue, random

app = Flask(__name__)

MOCK_STATE = {
    "mode": "DEMO",
    "is_halted": False,
    "kill_switch": False,
    "summary": {
        "bankroll": 100.00, "available_capital": 88.40,
        "daily_pnl": 3.20, "realized_pnl": 3.20,
        "open_positions": 2, "trade_count_today": 3,
        "wins_today": 2, "losses_today": 1, "win_rate": 66.7,
        "is_halted": False, "kill_switch": False,
        "reversal_blocked": [], "expansion_used": [],
    },
    "positions": {
        "KXHIGHLAX-26APR09-B71.5": {
            "station": "KLAX", "market_id": "KXHIGHLAX-26APR09-B71.5",
            "bucket_lower": 71, "contracts": 10, "entry_price": 0.28,
            "entry_usd": 2.80, "entry_time": "2026-04-08T14:07:00+00:00",
            "event_date": "2026-04-09", "current_bid": 0.41, "current_ask": 0.43,
            "unrealized_pnl": 1.30, "pnl_pct": 46.4, "exit_value": 4.10, "last_edge": 0.22,
            "stop_loss_triggered": False, "reversal_triggered": False,
            "early_exit_triggered": False, "manually_closed": False,
        },
        "KXHIGHJFK-26APR09-B69.5": {
            "station": "KJFK", "market_id": "KXHIGHJFK-26APR09-B69.5",
            "bucket_lower": 69, "contracts": 5, "entry_price": 0.35,
            "entry_usd": 1.75, "entry_time": "2026-04-08T14:09:00+00:00",
            "event_date": "2026-04-09", "current_bid": 0.31, "current_ask": 0.33,
            "unrealized_pnl": -0.20, "pnl_pct": -11.4, "exit_value": 1.55, "last_edge": 0.15,
            "stop_loss_triggered": False, "reversal_triggered": False,
            "early_exit_triggered": False, "manually_closed": False,
        },
    },
    "signals": {
        "KJFK": {
            "station": "KJFK", "event_date": "2026-04-09",
            "local_time": "Tue Apr 8  10:45 EDT",
            "decision": "TRADE",
            "forecast_raw": 70.0, "bias_mean": -0.8, "bias_std": 3.1,
            "forecast_adjusted": 69.2, "cluster_id": 4, "season": "MAM", "n_obs": 52,
            "pattern_confidence": "high",
            "top_bucket": 69, "top_edge": 0.19, "top_model_prob": 0.48,
            "top_kalshi_prob": 0.29, "top_yes_ask": 0.29,
            "kelly_fraction": 0.062, "kelly_stake_usd": 1.75, "kelly_contracts": 5,
            "signal_generated_at": "2026-04-08T14:45:00+00:00",
            "threshold_result": {
                "effective_threshold": 0.12, "weather_condition": "clear",
                "weather_multiplier": 1.0, "std_gate_fired": False,
            },
            "buckets": [
                {"bucket_lower": 68, "bucket_label": "68° or below", "model_prob": 0.05, "kalshi_prob": 0.09, "edge": -0.04, "yes_ask": 0.09, "yes_bid": 0.07},
                {"bucket_lower": 69, "bucket_label": "69° to 70°",   "model_prob": 0.48, "kalshi_prob": 0.29, "edge": 0.19, "yes_ask": 0.29, "yes_bid": 0.27},
                {"bucket_lower": 71, "bucket_label": "71° to 72°",   "model_prob": 0.28, "kalshi_prob": 0.32, "edge": -0.04, "yes_ask": 0.32, "yes_bid": 0.30},
                {"bucket_lower": 73, "bucket_label": "73° to 74°",   "model_prob": 0.12, "kalshi_prob": 0.18, "edge": -0.06, "yes_ask": 0.18, "yes_bid": 0.16},
                {"bucket_lower": 75, "bucket_label": "75° to 76°",   "model_prob": 0.05, "kalshi_prob": 0.08, "edge": -0.03, "yes_ask": 0.08, "yes_bid": 0.06},
                {"bucket_lower": 77, "bucket_label": "77° or above", "model_prob": 0.02, "kalshi_prob": 0.04, "edge": -0.02, "yes_ask": 0.04, "yes_bid": 0.02},
            ],
            "reasoning": {
                "current_conditions": "Clear skies at JFK. Temp 64°F, dewpoint 48°F, winds NW 12kt. Low humidity, good visibility. No precipitation in the area.",
                "synoptic_pattern": "A mid-level ridge is building over the Northeast. 500mb heights running 30–40m above climatology. Southwest flow aloft promoting warm air advection. Pattern matches Cluster 4 — spring ridge, historically associated with above-normal afternoon highs.",
                "forecast_and_bias": "NWS forecasts a high of 70°F. Historical bias for this pattern/month: −0.8°F. Bias-adjusted estimate: 69.2°F ± 3.1°F. In 52 similar past days, the high has verified in the 69–70°F bucket 48% of the time.",
                "market_analysis": "Kalshi is pricing the 69–70°F bucket at 29¢ (29% implied probability). Our model says 48%. That's a 19-cent edge — well above the 12-cent threshold for clear conditions. The market appears to be anchoring on a round 70°F forecast without applying the warm-season ridge bias.",
                "decision_rationale": "TRADE — strong edge (+0.19) in the 69–70°F bucket driven by ridge pattern bias and model underestimate. Kelly stake $1.75.",
                "expansion_note": "",
                "expansion_guardrails": {},
                "bucket_table": [],
                "threshold_checks": [],
                "data_sources": {},
                "generated_at": "2026-04-08T14:45:00+00:00",
            },
            "metar": {
                "temp_f": 64.0, "dewpoint_f": 48.0,
                "wind_dir": 315, "wind_speed_kt": 12,
                "sky_condition": "CLR", "raw": "KJFK 081454Z 31512KT 10SM CLR 18/09 A2992",
            },
            "taf": {
                "ceiling_category": "clear", "weather_condition": "clear",
                "has_amd": False, "hard_skip": False,
                "summary": "VFR conditions expected through 12Z tomorrow. No significant weather.",
            },
        },
        "KLAX": {
            "station": "KLAX", "event_date": "2026-04-09",
            "local_time": "Tue Apr 8  07:45 PDT",
            "decision": "TRADE",
            "forecast_raw": 74.0, "bias_mean": -1.2, "bias_std": 3.4,
            "forecast_adjusted": 72.8, "cluster_id": 3, "season": "MAM", "n_obs": 47,
            "pattern_confidence": "medium",
            "top_bucket": 71, "top_edge": 0.24, "top_model_prob": 0.52,
            "top_kalshi_prob": 0.28, "top_yes_ask": 0.28,
            "kelly_fraction": 0.085, "kelly_stake_usd": 2.80, "kelly_contracts": 10,
            "signal_generated_at": "2026-04-08T14:45:00+00:00",
            "threshold_result": {
                "effective_threshold": 0.216, "weather_condition": "marine_fog",
                "weather_multiplier": 1.8, "std_gate_fired": False,
            },
            "buckets": [
                {"bucket_lower": 68, "bucket_label": "68° or below", "model_prob": 0.03, "kalshi_prob": 0.07, "edge": -0.04, "yes_ask": 0.07, "yes_bid": 0.05},
                {"bucket_lower": 69, "bucket_label": "69° to 70°",   "model_prob": 0.08, "kalshi_prob": 0.12, "edge": -0.04, "yes_ask": 0.12, "yes_bid": 0.10},
                {"bucket_lower": 71, "bucket_label": "71° to 72°",   "model_prob": 0.52, "kalshi_prob": 0.28, "edge": 0.24, "yes_ask": 0.28, "yes_bid": 0.26},
                {"bucket_lower": 73, "bucket_label": "73° to 74°",   "model_prob": 0.24, "kalshi_prob": 0.31, "edge": -0.07, "yes_ask": 0.31, "yes_bid": 0.29},
                {"bucket_lower": 75, "bucket_label": "75° to 76°",   "model_prob": 0.09, "kalshi_prob": 0.14, "edge": -0.05, "yes_ask": 0.14, "yes_bid": 0.12},
                {"bucket_lower": 77, "bucket_label": "77° or above", "model_prob": 0.04, "kalshi_prob": 0.08, "edge": -0.04, "yes_ask": 0.08, "yes_bid": 0.06},
            ],
            "reasoning": {
                "current_conditions": "Marine layer present at LAX this morning. Temp 61°F, dewpoint 56°F, winds SW 8kt. Overcast below 1500ft. Typical June Gloom pattern — marine layer usually burns off by noon local.",
                "synoptic_pattern": "500mb ridge axis sitting just offshore. Onshore flow keeping marine layer in place this morning. Pattern Cluster 3 — coastal ridge with marine influence. Historically the layer clears by 11 AM–1 PM local, and afternoon highs run 2–3°F below the raw model forecast due to residual cooling.",
                "forecast_and_bias": "NWS forecasts a high of 74°F. Marine layer bias for this cluster: −1.2°F. Adjusted estimate: 72.8°F ± 3.4°F. In 47 similar days, the high verified in the 71–72°F bucket 52% of the time — the marine layer consistently shaves the peak.",
                "market_analysis": "Kalshi prices 71–72°F at 28¢ (28%). Our model says 52%. Edge of +0.24 clears the marine fog penalty threshold of 0.216. The market is following the raw NWS 74°F forecast without accounting for the well-documented marine layer cool bias at this coastal station.",
                "decision_rationale": "TRADE — large edge (+0.24) driven by marine layer cool bias. Edge clears the elevated marine fog threshold (0.216). Kelly stake $2.80.",
                "expansion_note": "",
                "expansion_guardrails": {},
                "bucket_table": [],
                "threshold_checks": [],
                "data_sources": {},
                "generated_at": "2026-04-08T14:45:00+00:00",
            },
            "metar": {
                "temp_f": 61.0, "dewpoint_f": 56.0,
                "wind_dir": 220, "wind_speed_kt": 8,
                "sky_condition": "OVC 015", "raw": "KLAX 081454Z 22008KT 10SM OVC015 16/13 A2994",
            },
            "taf": {
                "ceiling_category": "broken", "weather_condition": "marine_fog",
                "has_amd": False, "hard_skip": False,
                "summary": "Marine layer OVC015 through 18Z, lifting to BKN030 by 20Z, becoming VFR by 22Z.",
            },
        },
        "KORD": {
            "station": "KORD", "event_date": "2026-04-09",
            "local_time": "Tue Apr 8  09:45 CDT",
            "decision": "WATCH",
            "forecast_raw": 65.0, "bias_mean": 0.4, "bias_std": 4.8,
            "forecast_adjusted": 65.4, "cluster_id": 7, "season": "MAM", "n_obs": 28,
            "pattern_confidence": "low",
            "top_bucket": 65, "top_edge": 0.09, "top_model_prob": 0.38,
            "top_kalshi_prob": 0.29, "top_yes_ask": 0.29,
            "kelly_fraction": 0.0, "kelly_stake_usd": 0.0, "kelly_contracts": 0,
            "signal_generated_at": "2026-04-08T14:45:00+00:00",
            "threshold_result": {
                "effective_threshold": 0.30, "weather_condition": "convective",
                "weather_multiplier": 2.5, "std_gate_fired": True,
            },
            "buckets": [],
            "reasoning": {
                "current_conditions": "Mostly cloudy at ORD. Temp 58°F, dewpoint 54°F, winds S 15kt gusting 24kt. Elevated instability ahead of approaching shortwave.",
                "synoptic_pattern": "Shortwave trough approaching from the west. Cluster 7 — transitional pattern with high model spread. Low confidence classification; synoptic setup is unusual for this time of year.",
                "forecast_and_bias": "NWS forecasts a high of 65°F. Bias +0.4°F for this cluster/month, but bias_std is 4.8°F — well above the 4.5°F gate. Model uncertainty is too high to trust a precise bucket.",
                "market_analysis": "Edge of +0.09 is present but below the convective threshold of 0.30. The std gate also fired (4.8 > 4.5), requiring a 0.30 floor. Not enough edge to trade through this uncertainty.",
                "decision_rationale": "WATCH — edge (+0.09) below convective threshold (0.30) and std gate fired. Monitoring for TAF improvement.",
                "expansion_note": "", "expansion_guardrails": {}, "bucket_table": [],
                "threshold_checks": [], "data_sources": {}, "generated_at": "2026-04-08T14:45:00+00:00",
            },
            "metar": {"temp_f": 58.0, "dewpoint_f": 54.0, "wind_dir": 180, "wind_speed_kt": 15, "sky_condition": "BKN 025", "raw": ""},
            "taf": {"ceiling_category": "broken", "weather_condition": "convective", "has_amd": False, "hard_skip": False, "summary": "VCTS possible 20Z–00Z. BKN020 through period."},
        },
        "KMIA": {
            "station": "KMIA", "event_date": "2026-04-09",
            "local_time": "Tue Apr 8  10:45 EDT",
            "decision": "SKIP",
            "forecast_raw": 88.0, "bias_mean": 0.2, "bias_std": 2.8,
            "forecast_adjusted": 88.2, "cluster_id": 2, "season": "MAM", "n_obs": 61,
            "pattern_confidence": "high",
            "top_bucket": 88, "top_edge": 0.06, "top_model_prob": 0.34,
            "top_kalshi_prob": 0.28, "top_yes_ask": 0.28,
            "kelly_fraction": 0.0, "kelly_stake_usd": 0.0, "kelly_contracts": 0,
            "signal_generated_at": "2026-04-08T14:45:00+00:00",
            "threshold_result": {
                "effective_threshold": 0.12, "weather_condition": "clear",
                "weather_multiplier": 1.0, "std_gate_fired": False,
            },
            "buckets": [],
            "reasoning": {"current_conditions": "Clear and hot at MIA.", "synoptic_pattern": "Deep subtropical ridge.", "forecast_and_bias": "Model 88°F, bias +0.2°F.", "market_analysis": "Edge only +0.06, below 0.12 threshold.", "decision_rationale": "SKIP — insufficient edge (+0.06 < 0.12).", "expansion_note": "", "expansion_guardrails": {}, "bucket_table": [], "threshold_checks": [], "data_sources": {}, "generated_at": "2026-04-08T14:45:00+00:00"},
            "metar": {"temp_f": 82.0, "dewpoint_f": 68.0, "wind_dir": 135, "wind_speed_kt": 10, "sky_condition": "FEW 025", "raw": ""},
            "taf": {"ceiling_category": "clear", "weather_condition": "clear", "has_amd": False, "hard_skip": False, "summary": "VFR through period."},
        },
        "KDFW": {"station": "KDFW", "event_date": "2026-04-09", "local_time": "Tue Apr 8  09:45 CDT", "decision": "SKIP", "forecast_raw": 82.0, "bias_mean": -0.5, "bias_std": 3.2, "forecast_adjusted": 81.5, "cluster_id": 5, "season": "MAM", "n_obs": 44, "pattern_confidence": "medium", "top_bucket": 81, "top_edge": 0.08, "top_model_prob": 0.31, "top_kalshi_prob": 0.23, "top_yes_ask": 0.23, "kelly_fraction": 0.0, "kelly_stake_usd": 0.0, "kelly_contracts": 0, "signal_generated_at": "2026-04-08T14:45:00+00:00", "threshold_result": {"effective_threshold": 0.12, "weather_condition": "scattered", "weather_multiplier": 1.2, "std_gate_fired": False}, "buckets": [], "reasoning": {"current_conditions": "Partly cloudy at DFW.", "synoptic_pattern": "Weak ridge over Texas.", "forecast_and_bias": "Model 82°F, bias −0.5°F.", "market_analysis": "Edge +0.08 below scattered threshold 0.144.", "decision_rationale": "SKIP — edge +0.08 below threshold 0.144 (scattered clouds).", "expansion_note": "", "expansion_guardrails": {}, "bucket_table": [], "threshold_checks": [], "data_sources": {}, "generated_at": "2026-04-08T14:45:00+00:00"}, "metar": {"temp_f": 74.0, "dewpoint_f": 58.0, "wind_dir": 200, "wind_speed_kt": 12, "sky_condition": "SCT 040", "raw": ""}, "taf": {"ceiling_category": "scattered", "weather_condition": "scattered", "has_amd": False, "hard_skip": False, "summary": "SCT clouds through period, VFR."}},
        "KATL": {"station": "KATL", "event_date": "2026-04-09", "local_time": "Tue Apr 8  10:45 EDT", "decision": "SKIP", "forecast_raw": 76.0, "bias_mean": 0.6, "bias_std": 3.0, "forecast_adjusted": 76.6, "cluster_id": 6, "season": "MAM", "n_obs": 55, "pattern_confidence": "high", "top_bucket": 77, "top_edge": 0.07, "top_model_prob": 0.29, "top_kalshi_prob": 0.22, "top_yes_ask": 0.22, "kelly_fraction": 0.0, "kelly_stake_usd": 0.0, "kelly_contracts": 0, "signal_generated_at": "2026-04-08T14:45:00+00:00", "threshold_result": {"effective_threshold": 0.12, "weather_condition": "clear", "weather_multiplier": 1.0, "std_gate_fired": False}, "buckets": [], "reasoning": {"current_conditions": "Clear at ATL.", "synoptic_pattern": "Ridge building.", "forecast_and_bias": "Model 76°F, bias +0.6°F.", "market_analysis": "Edge +0.07 just below threshold.", "decision_rationale": "SKIP — edge +0.07 below threshold 0.12.", "expansion_note": "", "expansion_guardrails": {}, "bucket_table": [], "threshold_checks": [], "data_sources": {}, "generated_at": "2026-04-08T14:45:00+00:00"}, "metar": {"temp_f": 70.0, "dewpoint_f": 52.0, "wind_dir": 270, "wind_speed_kt": 8, "sky_condition": "CLR", "raw": ""}, "taf": {"ceiling_category": "clear", "weather_condition": "clear", "has_amd": False, "hard_skip": False, "summary": "VFR through period."}},
        "KDEN": {"station": "KDEN", "event_date": "2026-04-09", "local_time": "Tue Apr 8  08:45 MDT", "decision": "SKIP", "forecast_raw": 62.0, "bias_mean": 1.1, "bias_std": 5.2, "forecast_adjusted": 63.1, "cluster_id": 1, "season": "MAM", "n_obs": 19, "pattern_confidence": "low", "top_bucket": 63, "top_edge": 0.05, "top_model_prob": 0.27, "top_kalshi_prob": 0.22, "top_yes_ask": 0.22, "kelly_fraction": 0.0, "kelly_stake_usd": 0.0, "kelly_contracts": 0, "signal_generated_at": "2026-04-08T14:45:00+00:00", "threshold_result": {"effective_threshold": 0.30, "weather_condition": "clear", "weather_multiplier": 1.0, "std_gate_fired": True}, "buckets": [], "reasoning": {"current_conditions": "Partly cloudy at DEN.", "synoptic_pattern": "Low-confidence trough pattern.", "forecast_and_bias": "Model 62°F, bias +1.1°F, but std 5.2°F is high.", "market_analysis": "Std gate fired (5.2 > 4.5). Required threshold 0.30. Edge only +0.05.", "decision_rationale": "SKIP — std gate fired, required edge 0.30, have 0.05.", "expansion_note": "", "expansion_guardrails": {}, "bucket_table": [], "threshold_checks": [], "data_sources": {}, "generated_at": "2026-04-08T14:45:00+00:00"}, "metar": {"temp_f": 55.0, "dewpoint_f": 38.0, "wind_dir": 290, "wind_speed_kt": 14, "sky_condition": "SCT 060", "raw": ""}, "taf": {"ceiling_category": "scattered", "weather_condition": "scattered", "has_amd": False, "hard_skip": False, "summary": "VFR, gusty winds."}},
        "KHOU": {"station": "KHOU", "event_date": "2026-04-09", "local_time": "Tue Apr 8  09:45 CDT", "decision": "SKIP", "forecast_raw": 84.0, "bias_mean": -0.3, "bias_std": 3.6, "forecast_adjusted": 83.7, "cluster_id": 5, "season": "MAM", "n_obs": 38, "pattern_confidence": "medium", "top_bucket": 83, "top_edge": 0.04, "top_model_prob": 0.26, "top_kalshi_prob": 0.22, "top_yes_ask": 0.22, "kelly_fraction": 0.0, "kelly_stake_usd": 0.0, "kelly_contracts": 0, "signal_generated_at": "2026-04-08T14:45:00+00:00", "threshold_result": {"effective_threshold": 0.12, "weather_condition": "clear", "weather_multiplier": 1.0, "std_gate_fired": False}, "buckets": [], "reasoning": {"current_conditions": "Hot and humid at HOU.", "synoptic_pattern": "Gulf moisture surge.", "forecast_and_bias": "Model 84°F, bias −0.3°F.", "market_analysis": "Edge only +0.04, well below threshold.", "decision_rationale": "SKIP — insufficient edge (+0.04 < 0.12).", "expansion_note": "", "expansion_guardrails": {}, "bucket_table": [], "threshold_checks": [], "data_sources": {}, "generated_at": "2026-04-08T14:45:00+00:00"}, "metar": {"temp_f": 78.0, "dewpoint_f": 72.0, "wind_dir": 160, "wind_speed_kt": 10, "sky_condition": "CLR", "raw": ""}, "taf": {"ceiling_category": "clear", "weather_condition": "clear", "has_amd": False, "hard_skip": False, "summary": "VFR through period."}},
    },
    "alerts": [
        {"title": "Bot started", "message": "WeatherBot DEMO mode initialized. Paper trading active.", "level": "INFO", "timestamp": "2026-04-08 14:05 UTC"},
        {"title": "Order filled — KLAX", "message": "KXHIGHLAX-26APR09-B71.5 | 10 contracts @ $0.28 | stake $2.80", "level": "INFO", "timestamp": "2026-04-08 14:07 UTC"},
        {"title": "Order filled — KJFK", "message": "KXHIGHJFK-26APR09-B69.5 | 5 contracts @ $0.35 | stake $1.75", "level": "INFO", "timestamp": "2026-04-08 14:09 UTC"},
    ],
    "tier_status": {"tier1": "14:30 UTC", "tier2": "14:30 UTC", "tier3": "14:05 UTC", "settlement": "09:00 UTC"},
    "adjustable_settings": {
        "STOP_LOSS_PCT":                  {"label": "Stop-loss %",              "min": 0.1, "max": 0.9, "value": 0.40},
        "REVERSAL_EDGE_THRESHOLD":        {"label": "Reversal edge threshold",  "min": -0.5,"max": 0.0,"value": -0.15},
        "PROFIT_REVERSAL_THRESHOLD":      {"label": "Profit reversal threshold","min": 0.0, "max": 0.5,"value": 0.10},
        "EXPANSION_EDGE_MIN":             {"label": "Expansion edge min",        "min": 0.1, "max": 0.5,"value": 0.18},
        "SIGNIFICANT_REPOSITION_EDGE_MIN":{"label": "Significant reposition edge","min":0.1,"max":0.5,"value": 0.22},
        "MAJOR_REPOSITION_EDGE_MIN":      {"label": "Major reposition edge",    "min": 0.1, "max": 0.5,"value": 0.25},
        "EDGE_THRESHOLD_BASE":            {"label": "Base edge threshold",      "min": 0.05,"max": 0.5,"value": 0.12},
        "MIN_KELLY_STAKE":                {"label": "Min Kelly stake ($)",      "min": 0.5, "max":10.0,"value": 1.00},
    },
    "settings": {},
}

@app.route("/")
def index():
    return render_template("index.html", initial_state=MOCK_STATE)

@app.route("/api/state")
def api_state():
    return jsonify(MOCK_STATE)

@app.route("/stream")
def stream():
    def gen():
        yield f"event: full_state\ndata: {json.dumps(MOCK_STATE)}\n\n"
        # Simulate a live P/L tick every few seconds
        bid = 0.41
        while True:
            time.sleep(4)
            bid = round(min(0.95, max(0.10, bid + random.uniform(-0.02, 0.03))), 2)
            pnl = round((bid - 0.28) * 10, 4)
            pct = round((bid - 0.28) / 0.28 * 100, 1)
            update = {"bankroll": 100.00, "available_capital": round(88.40 + pnl, 2),
                      "daily_pnl": round(3.20 + pnl - 1.30, 2), "realized_pnl": 3.20,
                      "open_positions": 2, "trade_count_today": 3,
                      "wins_today": 2, "losses_today": 1, "win_rate": 66.7,
                      "is_halted": False, "kill_switch": False,
                      "reversal_blocked": [], "expansion_used": []}
            yield f"event: state_update\ndata: {json.dumps(update)}\n\n"
            yield "event: heartbeat\ndata: {}\n\n"
    return Response(gen(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.route("/api/kill-switch/activate",   methods=["POST"])
def ks_on():  return jsonify({"ok": True})
@app.route("/api/kill-switch/deactivate", methods=["POST"])
def ks_off(): return jsonify({"ok": True})
@app.route("/api/signal-pass",            methods=["POST"])
def sig():    return jsonify({"ok": True, "message": "Signal pass triggered (preview mode)"})
@app.route("/api/close-all",              methods=["POST"])
def ca():     return jsonify({"ok": True, "results": []})
@app.route("/api/close-position/<path:mid>", methods=["POST"])
def cp(mid):  return jsonify({"ok": True, "realized_pnl": 0.0})
@app.route("/api/settings",              methods=["GET","POST"])
def settings():
    if request.method == "POST": return jsonify({"ok": True, "saved": {}, "errors": []})
    return jsonify(MOCK_STATE["adjustable_settings"])

if __name__ == "__main__":
    print("\n  WeatherBot Dashboard Preview")
    print("  ─────────────────────────────")
    print("  http://localhost:5000\n")
    app.run(host="0.0.0.0", port=5000, debug=False)
