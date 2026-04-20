"""
Generates docs/dashboard_manual.pdf — a reference guide for the WeatherBot dashboard cards.
Run: python scripts/generate_manual.py
"""

from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, KeepTogether,
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER
import os

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "docs", "dashboard_manual.pdf")
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

# ── Colour palette ────────────────────────────────────────────────────────────
C_DARK      = colors.HexColor("#1a1a2e")
C_BLUE      = colors.HexColor("#0d6efd")
C_GREEN     = colors.HexColor("#198754")
C_YELLOW    = colors.HexColor("#ffc107")
C_RED       = colors.HexColor("#dc3545")
C_TEAL      = colors.HexColor("#0dcaf0")
C_GREY      = colors.HexColor("#6c757d")
C_LIGHTGREY = colors.HexColor("#f8f9fa")
C_PANEL     = colors.HexColor("#e9ecef")
C_WHITE     = colors.white

# ── Styles ────────────────────────────────────────────────────────────────────
styles = getSampleStyleSheet()

def S(name, **kw):
    base = styles[name] if name in styles else styles["Normal"]
    return ParagraphStyle(name + str(id(kw)), parent=base, **kw)

title_style    = S("Title",    fontSize=26, textColor=C_DARK, spaceAfter=6, alignment=TA_CENTER)
subtitle_style = S("Normal",   fontSize=12, textColor=C_GREY, spaceAfter=20, alignment=TA_CENTER)
h1_style       = S("Heading1", fontSize=16, textColor=C_BLUE, spaceBefore=18, spaceAfter=6,
                   borderPad=4)
h2_style       = S("Heading2", fontSize=13, textColor=C_DARK, spaceBefore=12, spaceAfter=4,
                   fontName="Helvetica-Bold")
h3_style       = S("Heading3", fontSize=11, textColor=C_GREY, spaceBefore=8, spaceAfter=3,
                   fontName="Helvetica-BoldOblique")
body_style     = S("Normal",   fontSize=10, textColor=C_DARK, spaceAfter=5, leading=14)
bullet_style   = S("Normal",   fontSize=10, textColor=C_DARK, leftIndent=18, spaceAfter=3,
                   leading=13, bulletIndent=6)
note_style     = S("Normal",   fontSize=9,  textColor=C_GREY,  spaceAfter=4, leading=12,
                   leftIndent=12)
mono_style     = S("Normal",   fontSize=9,  fontName="Courier", textColor=C_DARK,
                   backColor=C_PANEL, leftIndent=12, spaceAfter=4, leading=12)

def H1(text): return Paragraph(text, h1_style)
def H2(text): return Paragraph(text, h2_style)
def H3(text): return Paragraph(text, h3_style)
def P(text):  return Paragraph(text, body_style)
def B(text):  return Paragraph(f"• {text}", bullet_style)
def N(text):  return Paragraph(f"<i>{text}</i>", note_style)
def HR():     return HRFlowable(width="100%", thickness=0.5, color=C_PANEL, spaceAfter=6)
def SP(h=6):  return Spacer(1, h)

def badge_table(badges):
    """Render a row of coloured badge cells."""
    data = [[Paragraph(f"<b>{label}</b>", S("Normal", fontSize=9, textColor=C_WHITE,
                                             alignment=TA_CENTER))
             for label, _ in badges]]
    colours_cmd = [
        ("BACKGROUND", (i, 0), (i, 0), col)
        for i, (_, col) in enumerate(badges)
    ]
    t = Table(data, colWidths=[1.1 * inch] * len(badges))
    t.setStyle(TableStyle([
        ("ROWBACKGROUNDS", (0, 0), (-1, -1), [C_WHITE]),
        ("ROUNDEDCORNERS", [4]),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("BOX", (0, 0), (-1, -1), 0, C_WHITE),
    ] + colours_cmd))
    return t

def info_table(rows, col_widths=None):
    """Two-column label / explanation table."""
    data = []
    for label, text in rows:
        data.append([
            Paragraph(f"<b>{label}</b>", S("Normal", fontSize=9, textColor=C_GREY)),
            Paragraph(text, S("Normal", fontSize=9, textColor=C_DARK, leading=12)),
        ])
    cw = col_widths or [1.5 * inch, 4.8 * inch]
    t = Table(data, colWidths=cw)
    t.setStyle(TableStyle([
        ("VALIGN",        (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING",    (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("ROWBACKGROUNDS", (0, 0), (-1, -1), [C_LIGHTGREY, C_WHITE]),
        ("BOX",           (0, 0), (-1, -1), 0.3, C_PANEL),
        ("LINEBELOW",     (0, 0), (-1, -1), 0.3, C_PANEL),
    ]))
    return t


# ── Document ──────────────────────────────────────────────────────────────────
doc = SimpleDocTemplate(
    OUTPUT_PATH,
    pagesize=letter,
    leftMargin=0.85 * inch,
    rightMargin=0.85 * inch,
    topMargin=0.85 * inch,
    bottomMargin=0.85 * inch,
    title="WeatherBot Dashboard — Reference Manual",
    author="WeatherBot",
)

story = []

# ── Cover ─────────────────────────────────────────────────────────────────────
story += [
    SP(40),
    Paragraph("WeatherBot", title_style),
    Paragraph("Dashboard Reference Manual", subtitle_style),
    SP(4),
    Paragraph("Understanding every element on the station signal cards",
              S("Normal", fontSize=11, textColor=C_GREY, alignment=TA_CENTER)),
    SP(60),
    HR(),
    Paragraph(
        "This manual explains every field, badge, number, and colour shown on the "
        "WeatherBot dashboard cards. Each card represents one city whose daily high "
        "temperature is traded on Kalshi prediction markets.",
        S("Normal", fontSize=10, textColor=C_GREY, alignment=TA_CENTER, leading=14)),
    HR(),
]

# ═══════════════════════════════════════════════════════════════════════════════
story += [SP(20), H1("1. Overview — What Is a Signal Card?")]

story += [
    P("The dashboard shows one card per city (station). Each card runs the full "
      "signal pipeline and tells you:"),
    B("Whether the bot wants to trade, watch, or skip tomorrow's market"),
    B("What temperature outcome it is backing and why"),
    B("How much it wants to stake, calculated by the Kelly criterion"),
    B("Real-time weather conditions and intraday temperature tracking"),
    SP(4),
    P("Cards are refreshed automatically every 6 hours (Tier 3 signal pass) and "
      "can be triggered on-demand with the <b>Run Signal</b> button."),
]

# ═══════════════════════════════════════════════════════════════════════════════
story += [SP(10), H1("2. Card Header")]

story += [
    H2("2.1  Decision Badge"),
    P("The coloured badge in the top-right corner of the card is the bot's final "
      "verdict for this station. There are five possible values:"),
    SP(6),
    badge_table([
        ("TRADE",       C_GREEN),
        ("WATCH",       C_YELLOW),
        ("SKIP",        C_GREY),
        ("HARD SKIP",   C_RED),
        ("CONSTRAINED", C_TEAL),
    ]),
    SP(8),
    info_table([
        ("TRADE",
         "All conditions met — edge clears the threshold, Kelly stake is above the "
         "minimum, and capital is available. The bot will place (or has placed) an order."),
        ("WATCH",
         "Edge is positive and the signal is real, but it does not clear today's "
         "required threshold. Worth monitoring — conditions may improve later in the day."),
        ("SKIP",
         "No bucket shows positive edge against Kalshi's current prices. "
         "The bot sees no mispricing worth trading."),
        ("HARD SKIP",
         "The TAF (terminal forecast) contains dangerous weather — freezing rain, "
         "heavy snow, or severe convection. Temperature forecasts become unreliable "
         "in these conditions. No trade is ever placed on a HARD SKIP."),
        ("CONSTRAINED",
         "The signal is valid and edge is present, but the exposure limit has been "
         "reached (another position is already using available capital). "
         "The bot will not open a second trade until the first closes."),
    ]),
]

story += [
    SP(8),
    H2("2.2  Weather Icon Panel"),
    P("The small panel in the top-left of the card header shows the current "
      "live observation (METAR) for the station:"),
    info_table([
        ("Temperature",  "Current surface temperature in °F. Colour-coded: blue = cold (<45°F), "
                         "green = mild, orange = warm (>80°F), red = hot (>95°F)."),
        ("Sky cover",    "Current ceiling / cloud cover from the METAR: SKC (clear), FEW, SCT "
                         "(scattered), BKN (broken), OVC (overcast)."),
        ("Wind",         "Wind speed in knots (kt). Direction is the heading the wind is coming "
                         "FROM, in degrees (e.g. 270° = westerly wind)."),
        ("Dewpoint",     "DP = dewpoint temperature. High dewpoint (close to temperature) means "
                         "humid air — relevant for coastal fog risk stations like KSFO."),
    ]),
    N("If aviationweather.gov is temporarily unavailable, the weather panel is hidden "
      "rather than showing stale or invalid data."),
]

story += [
    SP(8),
    H2("2.3  Market Date"),
    P("Below the header, the label <b>\"High Temperature Market — YYYY-MM-DD\"</b> "
      "shows the date of the Kalshi market being analysed. This is always "
      "<i>tomorrow's</i> date — the bot trades the next-day high temperature market, "
      "which settles after the calendar day ends."),
]

# ═══════════════════════════════════════════════════════════════════════════════
story += [SP(10), H1("3. Forecast Row")]

story += [
    H2("3.1  Bias-Adjusted Forecast"),
    P("The large temperature number labelled <b>\"Bias-adj. forecast\"</b> is the "
      "bot's best estimate of tomorrow's high temperature. It is built in two steps:"),
    B("<b>Primary forecast</b> — fetched live from the NWS api.weather.gov gridded "
      "forecast, updated hourly. This is an official NWS human-forecaster product."),
    B("<b>Bias correction</b> — for NWS forecasts the correction is zero (NWS "
      "forecasters already account for local biases). For ERA5/Open-Meteo fallback "
      "sources, a statistical correction is applied from the historical bias table."),
    SP(4),
    P("The <b>±X.X°F</b> figure beside the forecast is the <i>uncertainty spread</i> "
      "(one standard deviation). It represents the typical error on similar forecast "
      "days. For NWS forecasts this is fixed at ±3.5°F — the documented NWS day-ahead "
      "mean absolute error. About 68% of similar days land within this range."),
]

story += [
    SP(6),
    H2("3.2  NWS vs MOS Divergence Badge"),
    P("When GFS-MOS (Model Output Statistics) data is available, a small badge "
      "appears showing <b>\"NWS +X°F vs MOS\"</b>. This compares the official NWS "
      "human forecast against the raw GFS model guidance:"),
    info_table([
        ("Yellow badge",  "NWS forecaster is running warmer than the GFS model by >2°F. "
                          "The forecaster may know about local warm advection or sea-breeze "
                          "patterns the model misses."),
        ("Blue badge",    "NWS forecaster is running cooler than the GFS model by >2°F. "
                          "The forecaster may be accounting for marine influence, cloud "
                          "cover, or a cold pool the model underestimates."),
        ("Grey badge",    "NWS and GFS agree within ±2°F — forecaster and model are aligned."),
        ("No badge",      "GFS-MOS data was not available from IEM today. Signal uses "
                          "NWS alone."),
    ]),
    N("This badge does NOT change the forecast — it is a confidence indicator only. "
      "Large NWS/MOS disagreements widen uncertainty about the outcome."),
]

story += [
    SP(6),
    H2("3.3  Cluster · Season · n="),
    P("The small text on the right of the forecast row — e.g. "
      "<b>\"Cluster 3 · MAM · n=42\"</b> — describes the synoptic weather regime "
      "used to calibrate the forecast:"),
    info_table([
        ("Cluster N",
         "The 500mb upper-air pattern for today, classified by KMeans into one of "
         "several seasonal regimes. Each cluster captures a distinct large-scale "
         "weather pattern (e.g. ridge, trough, zonal flow). The bot was trained on "
         "historical days with the same cluster."),
        ("Season",
         "The meteorological season: DJF (Dec-Jan-Feb), MAM (Mar-Apr-May), "
         "JJA (Jun-Jul-Aug), SON (Sep-Oct-Nov). Clusters are trained separately "
         "per season because the same upper-air pattern means different things in "
         "summer vs winter."),
        ("n=",
         "The number of historical observations in the bias table that match today's "
         "exact cluster, month, and forecast range. Higher n means the bias "
         "correction is more statistically reliable. Below n=10 the bot applies "
         "a station/month average fallback instead."),
    ]),
]

# ═══════════════════════════════════════════════════════════════════════════════
story += [SP(10), H1("4. Obs Tracking")]

story += [
    P("The <b>Obs tracking</b> line shows the current METAR temperature alongside "
      "a coloured divergence bubble:"),
    info_table([
        ("XX°F",      "The most recent observed temperature at the station (METAR), "
                      "updated roughly every 20-30 minutes."),
        ("+X°F ▲",    "Current obs are X°F ABOVE the adjusted forecast. Shown in orange/red. "
                      "A large positive divergence early in the day suggests the high may "
                      "beat the forecast — good news if you hold a higher bucket."),
        ("−X°F ▼",    "Current obs are X°F BELOW the adjusted forecast. Shown in blue. "
                      "A large negative divergence mid-afternoon is an early warning that "
                      "the daily high may fall short — relevant for exit timing."),
        ("≈ 0°F",     "Tracking in line with the forecast (within ±4°F). No action needed."),
    ]),
    N("Obs tracking disappears when the METAR fetch fails. This is normal and temporary — "
      "aviationweather.gov occasionally times out. It returns automatically on the next "
      "Tier 1 cycle (every ~5 minutes)."),
]

# ═══════════════════════════════════════════════════════════════════════════════
story += [SP(10), H1("5. Top Bucket Row")]

story += [
    P("The highlighted row labelled <b>Top bucket</b> shows the single best market "
      "opportunity the bot has identified for this station. It is selected by blending "
      "model probability and edge, so it favours the <i>likely</i> outcome while "
      "still rewarding genuine mispricing by Kalshi."),
    SP(4),
    H2("5.1  Bucket Label"),
    P("Kalshi's temperature markets are divided into 2°F buckets. The label shows "
      "the range the bot is backing:"),
    info_table([
        ("≤ 72°F",    "The <i>lower tail</i> bucket — settles YES if the daily high is "
                      "72°F or below. This is the lowest bucket available in the market."),
        ("74–75°F",   "An <i>interior</i> bucket — settles YES if the daily high falls "
                      "between 74°F and 75°F (exclusive upper bound)."),
        ("≥ 81°F",    "The <i>upper tail</i> bucket — settles YES if the daily high is "
                      "81°F or above. This is the highest bucket available in the market."),
    ]),
    N("Tail boundaries vary by station and season. They are discovered live from "
      "Kalshi's API each cycle — the dashboard always shows the actual live boundaries, "
      "not hardcoded constants."),
]

story += [
    SP(6),
    H2("5.2  Model %"),
    P("The probability the bot's forecast model assigns to this bucket settling YES. "
      "Calculated by integrating a normal distribution (mean = adjusted forecast, "
      "σ = uncertainty spread) over the bucket's temperature range."),
    P("Example: forecast 76°F ± 3.5°F → the 74–75°F bucket covers roughly 22% of "
      "the probability distribution."),
]

story += [
    SP(6),
    H2("5.3  Kalshi %"),
    P("The probability implied by Kalshi's current <i>ask price</i> for the Yes "
      "contract. If you can buy Yes for 20¢, Kalshi is implying a 20% chance of "
      "settlement."),
    N("Kalshi % = yes_ask price (e.g. $0.20 → 20%). This is the price you pay "
      "per contract, not the mid-market probability."),
]

story += [
    SP(6),
    H2("5.4  Edge"),
    P("Edge = Model % − Kalshi %. This is the core signal:"),
    info_table([
        ("Positive edge",  "Our model thinks this outcome is MORE likely than Kalshi does. "
                           "We have a statistical advantage — the market is underpricing "
                           "the outcome."),
        ("Negative edge",  "Kalshi is pricing this outcome higher than our model. "
                           "No advantage — do not trade."),
        ("✓ checkmark",    "Edge of +0.12 or above — clears the base threshold required "
                           "to trade (before any weather penalty is applied)."),
    ]),
    P("Edge is expressed as a decimal fraction (e.g. +0.15 = 15 percentage points "
      "of advantage). A +0.15 edge means we believe the true probability is 15pp "
      "higher than the Kalshi price implies."),
]

# ═══════════════════════════════════════════════════════════════════════════════
story += [SP(10), H1("6. Kelly Stake Row")]

story += [
    P("The <b>Kelly stake</b> line shows how much the bot wants to risk and "
      "at what price:"),
    info_table([
        ("$X.XX",
         "Total dollar amount to spend on this trade, calculated by the fractional "
         "Kelly criterion. Capped at 2% of the current bankroll per trade to prevent "
         "overexposure."),
        ("N contracts",
         "Number of Yes contracts to buy. Each contract pays $1.00 if it settles YES "
         "and $0.00 if it settles NO."),
        ("@ $X.XX",
         "The ask price per contract in dollars (e.g. $0.18 = 18¢). This is what "
         "you pay upfront. Your maximum profit per contract is $1.00 − ask price."),
    ]),
    SP(4),
    H2("Kelly Criterion Explained"),
    P("The Kelly criterion is a formula that sizes bets to maximise long-run bankroll "
      "growth. Given:"),
    B("p = our model probability of winning"),
    B("b = net profit per dollar staked (= (1 − ask) / ask)"),
    B("q = 1 − p"),
    P("Kelly fraction = (p × b − q) / b"),
    P("The bot uses <i>fractional Kelly</i> — it never bets more than 2% of bankroll "
      "regardless of how large the Kelly fraction computes to. This protects against "
      "model errors and extreme market moves."),
    P("Kelly stake is further scaled down when the synoptic pattern confidence is "
      "<b>MEDIUM</b> (75% of Kelly) or <b>LOW</b> (50% of Kelly) — unusual weather "
      "regimes carry higher forecast uncertainty."),
    N("A Kelly stake below $1.00 triggers a WATCH instead of TRADE — "
      "the position would be too small to be worth the transaction cost."),
]

# ═══════════════════════════════════════════════════════════════════════════════
story += [SP(10), H1("7. Expandable Sections")]

story += [
    P("Three collapsible panels can be opened by clicking the buttons at the "
      "bottom of each card:"),
]

story += [
    SP(4),
    H2("7.1  Full Reasoning"),
    P("A four-section plain-English explanation of the signal:"),
    info_table([
        ("Current Conditions",
         "A summary of the live METAR: temperature, dewpoint, wind, and sky cover "
         "at the station right now."),
        ("Synoptic Pattern",
         "A description of the 500mb upper-air pattern (cluster). Explains what "
         "large-scale weather regime is in place and how confidently it matches "
         "the historical training set."),
        ("Forecast & Bias",
         "Explains the primary forecast source, any NWS/MOS divergence, the "
         "historical bias estimate, and the adjusted forecast with its uncertainty "
         "range. For NWS forecasts, notes that no bias correction is applied "
         "because NWS forecasts are already human-calibrated."),
        ("Market Analysis",
         "States what Kalshi is pricing the top bucket at, what the bot's model "
         "says, and the resulting edge. Also states the current edge threshold "
         "and whether any weather penalty has been applied."),
        ("Decision Rationale",
         "One sentence explaining the final TRADE / WATCH / SKIP decision "
         "in plain terms — stake size, edge vs threshold, or reason for skipping."),
    ]),
]

story += [
    SP(8),
    H2("7.2  Bucket Table"),
    P("A full table of every live Kalshi bucket for this station, showing:"),
    info_table([
        ("★ (star)",    "The bucket selected as the top opportunity."),
        ("Bucket",      "The temperature range label (e.g. 74–75°F, ≤ 72°F, ≥ 81°F)."),
        ("Model %",     "Our model's probability for this bucket."),
        ("Kalshi %",    "Kalshi's implied probability (ask price)."),
        ("Edge",        "Model % − Kalshi %. Green = positive (we have edge). "
                        "Red = negative (Kalshi overpricing this outcome)."),
        ("Bar",         "A visual bar proportional to the magnitude of edge. "
                        "Green bar = positive edge; red bar = negative edge."),
    ]),
    N("Buckets with model probability below 75% of the peak bucket are still shown "
      "in the table for reference, but are not eligible for selection as the top "
      "trading bucket."),
]

story += [
    SP(8),
    H2("7.3  Weather Summary"),
    P("A detailed breakdown of the current meteorological data:"),
    info_table([
        ("METAR",
         "The full current observation: temperature, dewpoint, wind direction "
         "and speed, sky cover, and the raw METAR string for reference."),
        ("TAF Highlights",
         "Key points from the Terminal Aerodrome Forecast for the next 24 hours: "
         "ceiling, weather condition category, and whether an Amendment (AMD) "
         "has been issued. AMD means the forecast was revised after the original "
         "issuance — useful context when conditions are changing rapidly."),
    ]),
]

# ═══════════════════════════════════════════════════════════════════════════════
story += [SP(10), H1("8. Weather Condition Categories & Thresholds")]

story += [
    P("The TAF is parsed into one of six condition categories. Each category "
      "adjusts the edge threshold required to place a trade — bad weather makes "
      "temperature forecasts less reliable, so the bot demands higher edge:"),
    SP(4),
    info_table([
        ("clear",
         "Clear skies, no significant weather. Base threshold applies (typically 12%)."),
        ("scattered",
         "Scattered clouds (3–4 oktas). Mild penalty — threshold increases slightly."),
        ("broken",
         "Mostly cloudy / broken ceiling. Moderate penalty — forecasts less certain "
         "due to reduced solar heating."),
        ("marine_fog",
         "Marine layer or coastal fog risk (relevant for KLAX, KSFO, KSEA). "
         "Significant penalty — fog burns off unpredictably and can cap the daily high."),
        ("convective",
         "Thunderstorm activity in the TAF. High penalty — convective initiation "
         "timing is very difficult to forecast."),
        ("precip",
         "Active precipitation (rain, drizzle). High penalty — evaporative cooling "
         "suppresses the daily high in ways models often underestimate."),
        ("hard_skip",
         "Dangerous conditions: freezing rain, heavy snow, or ice. The bot will "
         "not trade regardless of edge — temperature forecasts under these conditions "
         "are considered unreliable. Decision = HARD SKIP."),
    ]),
]

# ═══════════════════════════════════════════════════════════════════════════════
story += [SP(10), H1("9. Threshold Checklist")]

story += [
    P("Inside the Full Reasoning panel, a checklist shows whether each guardrail "
      "passed or failed:"),
    info_table([
        ("Weather condition",
         "Passes if the TAF does not trigger a HARD SKIP. A failed check here "
         "means no trade is placed regardless of other factors."),
        ("Edge vs threshold",
         "Passes if the top bucket's edge meets or exceeds the weather-adjusted "
         "threshold. The threshold is the base rate (12%) plus any weather penalty."),
        ("Bias uncertainty gate",
         "Passes if the forecast uncertainty (bias_std) is below a level that "
         "would make the probability distribution too flat to trade confidently. "
         "A very wide spread means almost all buckets have similar probability, "
         "making edge hard to find reliably."),
        ("Minimum stake",
         "Passes if the Kelly-calculated stake is at least $1.00. Below this "
         "the trade is not worth opening."),
        ("Historical sample size",
         "Passes if the bias table has at least 10 observations for today's "
         "regime. Below 10 the bias correction is less statistically reliable."),
        ("Pattern confidence",
         "HIGH = today matches the cluster centroid closely. MEDIUM = reasonable "
         "match. LOW = unusual synoptic setup. Low confidence does not prevent "
         "trading but reduces the Kelly stake to 50%."),
    ]),
]

# ═══════════════════════════════════════════════════════════════════════════════
story += [SP(10), H1("10. Operating Modes")]

story += [
    P("The bot runs in one of three modes, shown in the top-right of the "
      "dashboard header:"),
    info_table([
        ("DEMO",
         "Paper trading. All logic runs normally but orders are simulated — "
         "no real money is placed on Kalshi. P&L is tracked against a virtual "
         "starting bankroll. This is the default mode."),
        ("LIVE (dry run)",
         "Connected to the live Kalshi API and reading real market data, but "
         "still not placing real orders. Useful for validating API connectivity "
         "and order logic before going fully live."),
        ("LIVE TRADING",
         "Fully live. Real orders are placed on Kalshi using real funds. "
         "All risk limits (2% max stake, 50% max exposure, stop-loss) apply."),
    ]),
    N("Mode is set by environment variables in the .env file: "
      "USE_DEMO=true/false and DRY_RUN=true/false."),
]

# ═══════════════════════════════════════════════════════════════════════════════
story += [SP(10), H1("11. Signal Refresh Schedule")]

story += [
    info_table([
        ("Tier 1 (~5 min)",
         "METAR fetch, intraday temperature tracking, position exit checks "
         "(stop-loss, profit target, overshoot/undershoot guards)."),
        ("Tier 2 (~10 min)",
         "TAF monitor — checks for amended forecasts that might change the "
         "weather penalty category on open positions."),
        ("Tier 3 (6 hrs)",
         "Full signal recompute for all 20 stations: new NWS forecast, "
         "pattern classification, bias lookup, Kalshi price fetch, "
         "edge calculation, Kelly sizing. This is what populates the cards."),
        ("Settlement (09:00 UTC)",
         "Checks Kalshi settlement results for yesterday's markets. "
         "Closes any positions that settled and records P&L."),
        ("Obs update (10:00 UTC)",
         "Appends yesterday's observed high temperatures from NOAA CDO "
         "to the historical database — keeps the bias table current."),
        ("Pipeline (1st of month)",
         "Full rebuild of the 500mb pattern clusters and bias table "
         "using the most recent month of observations."),
    ]),
    N("Tier 3 can also be triggered on-demand via the Run Signal button on the dashboard."),
]

# ═══════════════════════════════════════════════════════════════════════════════
story += [SP(10), H1("12. Key Terms Glossary")]

story += [
    info_table([
        ("Kalshi bucket",
         "A 2°F temperature range that defines one prediction market. "
         "Buying Yes on a bucket pays $1.00 if the official daily high falls "
         "within that range, $0.00 otherwise."),
        ("Edge",
         "The gap between our model's probability and Kalshi's implied probability. "
         "Positive edge means we think the outcome is more likely than the market does."),
        ("Bias correction",
         "A statistical adjustment to a raw model forecast based on how much "
         "that model historically over- or under-predicts in similar conditions."),
        ("Kelly criterion",
         "A mathematical formula for optimal bet sizing. Maximises long-run "
         "bankroll growth by betting proportionally to edge."),
        ("500mb / z500",
         "The geopotential height of the 500 millibar pressure level in the "
         "atmosphere (~18,000 ft). Used to classify the large-scale weather "
         "pattern (synoptic regime) driving surface temperatures."),
        ("METAR",
         "Aviation weather observation issued every 20-60 minutes. Contains "
         "current temperature, dewpoint, wind, visibility, and sky cover."),
        ("TAF",
         "Terminal Aerodrome Forecast — a 24-30 hour aviation weather forecast "
         "issued by the NWS. Used to assess weather penalty category."),
        ("NWS AFM",
         "Area Forecast Matrix — a gridded product issued by NWS human forecasters "
         "containing their official max/min temperature guidance."),
        ("GFS-MOS",
         "GFS Model Output Statistics — a statistically post-processed version "
         "of the GFS global model. Used as a cross-check against the NWS forecast."),
        ("Settlement station",
         "The specific NWS weather station whose official daily maximum temperature "
         "Kalshi uses to settle the market. Not always the same as the airport ICAO "
         "code (e.g. KORD markets settle on KMDW — Chicago Midway, not O'Hare)."),
    ], col_widths=[1.6 * inch, 4.7 * inch]),
]

# ── Build ─────────────────────────────────────────────────────────────────────
doc.build(story)
print(f"PDF written to: {os.path.abspath(OUTPUT_PATH)}")
