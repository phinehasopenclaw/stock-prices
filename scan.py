"""Gap scanner: market-wide sweep -> 3-point filter -> 5m setup read -> scan.json.

Runs as a long-lived GitHub Actions job. Each loop:
  1. Sweeps Yahoo's US equity screener (premarket-aware) for the movers universe.
  2. Keeps names gapping up >= GAP_MIN with price/market-cap/volume floors.
  3. Pulls 5-minute bars (incl. premarket) for the top candidates and computes
     VWAP, EMA9, session/premarket high-low, volume surge, extension.
  4. Classifies the setup (A-TRIGGER, A-WATCH, B-BREAKOUT, B-WATCH, EXTENDED,
     FADING, HOLDING) and proposes entry / stop / targets.
  5. Checks Finnhub news for a real catalyst and dilution red flags.
  6. Writes out/scan.json and force-pushes it to the scan-data branch.
"""
import datetime as dt
import json
import math
import os
import re
import base64
import gzip
import io
import subprocess
import sys
import threading
import time
import traceback
from zoneinfo import ZoneInfo

import requests
import yfinance as yf
from yfinance import EquityQuery as Q

ET = ZoneInfo("America/New_York")
CT = ZoneInfo("America/Chicago")

# ---- Tunables -------------------------------------------------------------
GAP_MIN = float(os.environ.get("GAP_MIN", 5.0))        # % up vs prior close
PRICE_MIN, PRICE_MAX = 1.0, 100.0
MCAP_MIN = 50e6
FLOAT_MIN = 5e6            # shares; below this, spreads gap and stops slip badly
DOLLAR_VOL_MIN = 3e6       # $ traded today before a name counts as liquid
PM_DOLLAR_MIN = 250_000    # pre-market: $ traded so far before a name shows on the watch list
AVGVOL_MIN = 150_000
SURGE_MIN = 1.5            # completed-bar volume vs baseline for A-TRIGGER / B-BREAKOUT
IGNITION_PACE = 3.0        # forming-bar volume pace vs baseline for IGNITION
ORB_START_CT, ORB_END_CT = dt.time(8, 35), dt.time(9, 30)
NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "").strip()
MAX_BAR_NAMES = 40          # names that get 5m bars each loop
MAX_NEWS_NAMES = 40         # names that get a Finnhub news check (cached 20 min)
NEWS_TTL = 20 * 60          # seconds to cache a news lookup
LOOP_SECONDS = int(os.environ.get("LOOP_SECONDS", 60))
PUSH_MIN_RV = 1.5   # phone push only when relative volume is 1.5x or more (Nov 13 scoreboard)
# Armed levels: names whose entry is known before the break get watched on 1-minute bars every
# WATCH_SECONDS, so the signal fires at the break instead of after a finished 5-minute bar
# (Nov 11-13: opening-range triggers printed ~6 min late, 0.3R past entry).
ARM_SETUPS = ("ORB-WATCH", "A-WATCH", "B-WATCH")
ARM_MIN_RV = 1.5
ARM_MAX = 8
WATCH_SECONDS = 15
CHASE_R = 0.3          # a break already more than this far past entry is logged, not pushed
BUY_LIMIT_R = 0.25     # buy stop-limit: limit this far above the stop price, so a runaway never fills
ARCHIVE_KEEP_DAYS = 7
RUN_MINUTES = int(os.environ.get("RUN_MINUTES", 350))
END_CT_HHMM = (16, 15)      # stop at 16:15 CT year-round (DST-aware)
ONE_SHOT = os.environ.get("ONE_SHOT") == "1"

FINNHUB_KEY = os.environ.get("FINNHUB_KEY", "")
REPO = os.environ.get("GITHUB_REPOSITORY", "")
TOKEN = os.environ.get("GITHUB_TOKEN", "")
OUT_DIR = os.path.abspath("out")

POS_WORDS = [
    "earnings", "results", "revenue", "guidance", "raises", "raised", "beat", "beats",
    "record", "fda", "approval", "approved", "clearance", "phase", "trial", "topline",
    "data", "acquisition", "acquire", "merger", "buyout", "takeover", "to be acquired",
    "contract", "award", "partnership", "agreement", "collaboration", "upgrade",
    "price target", "launch", "patent", "license", "deal", "order",
]
WARN_WORDS = {
    "offering": "Offering", "priced": "Offering", "registered direct": "Offering",
    "at-the-market": "ATM offering", "private placement": "Placement",
    "warrant": "Warrants", "reverse split": "Reverse split",
    "ads ratio": "Reverse split (ADS ratio)", "ratio change": "Reverse split (ADS ratio)",
    "going concern": "Going concern", "delist": "Delisting risk",
    "minimum bid": "Delisting risk", "nasdaq notice": "Delisting risk",
    "downgrade": "Downgrade",
}
OTC_EXCH = re.compile(r"otc|pink|pnk|oqb|oqx|other", re.I)
# Recaps of a move that already happened ("Why X stock rocketed today") are not new news.
RECAP_WHY = re.compile(r"\bwhy\b.*\b(stock|shares)\b|here'?s why", re.I)
RECAP_MOVE = re.compile(
    r"\b(stock|shares)\b[^.]{0,30}\b(soar|rocket|jump|surg|skyrocket|pop|rall|climb|spik|is up|are up|higher)\w*", re.I)


def is_recap(n):
    """A 'why it moved' story, or a move story from before today: describes a move, isn't the catalyst."""
    head = n.get("headline", "")
    if RECAP_WHY.search(head):
        return True
    start = dt.datetime.combine(dt.datetime.now(ET).date(), dt.time(0, 0), ET).timestamp()
    return bool(RECAP_MOVE.search(head)) and n.get("datetime", 0) < start
ROUNDUP = re.compile(
    r"movers|stocks moving|gainers|losers|most active|unusual volume|top stocks|"
    r"premarket|pre-market|mid-day|midday|after-hours|session top|shares are trading|"
    r"\bdow\b|s&p 500|nasdaq composite|stock market|wall street|stocks to watch|"
    r"stocks making|points;|market update|market wrap|\d+ stocks|top \d+|here are",
    re.I,
)
NEG = re.compile(
    r"\b(lower(s|ed)?|cut(s)?|slash(es|ed)?|downbeat|miss(es|ed)?|disappoint\w*|weak(er)?|"
    r"fell|falls|plunge\w*|tumble\w*|sink(s)?|decline[sd]?|downgrade[sd]?|suspend\w*|"
    r"halt(s|ed)? (trial|study|enrollment)|reject\w*|delay(s|ed)?|withdraw\w*|warns?)\b",
    re.I,
)
CAT_TYPES = [
    ("M&A", r"acqui|merger|buyout|takeover|to be acquired|tender offer"),
    ("FDA/Clinical", r"\bfda\b|approv|clearance|phase \d|phase [iv]+|\btrial\b|topline|data readout"),
    ("Earnings", r"earnings|results|revenue|guidance|quarter|\bq[1-4]\b|record sales|eps"),
    ("Contract", r"contract|award|purchase order|partnership|agreement|collaboration|license"),
    ("Analyst", r"upgrade|initiat|price target|reiterat|maintains|buy rating|outperform|overweight"),
]
CAT_BONUS = {"M&A": 2.5, "FDA/Clinical": 2.5, "Earnings": 2.0, "Contract": 1.5, "Analyst": 0.8, "Other": 0.3}


def catalyst_type(text):
    for name, pat in CAT_TYPES:
        if re.search(pat, text, re.I):
            return name
    return "Other"


GENERIC_NAME_WORDS = {"the", "inc", "corp", "corporation", "holdings", "group", "company",
                      "limited", "ltd", "technologies", "international", "global", "systems"}


def about_company(n, row):
    """True when a headline is about this company, not a market roundup that tags it."""
    text = (n.get("headline", "") + " " + n.get("summary", "")[:400])
    if re.search(r"\b" + re.escape(row["sym"]) + r"\b", text):
        return True
    words = [w for w in re.findall(r"[A-Za-z]{4,}", row.get("name") or "")
             if w.lower() not in GENERIC_NAME_WORDS]
    return bool(words) and words[0].lower() in text.lower()

_news_cache = {}
_state = {"loop": 0, "errors": []}


def log(msg):
    print(f"[{dt.datetime.now(CT):%H:%M:%S}] {msg}", flush=True)


def fnum(x, nd=2):
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return None
        return round(float(x), nd)
    except Exception:
        return None


# ---- 1. Universe ----------------------------------------------------------
def screener_pages(sort_field, pages, session_state):
    base = [
        Q("gt", ["intradayprice", PRICE_MIN]),
        Q("lt", ["intradayprice", PRICE_MAX]),
        Q("gt", ["avgdailyvol3m", AVGVOL_MIN]),
        Q("gt", ["intradaymarketcap", 20e6]),
        Q("eq", ["region", "us"]),
    ]
    query = Q("and", base)
    quotes = []
    for p in range(pages):
        for attempt in range(3):
            try:
                r = yf.screen(query, offset=p * 250, size=250, sortField=sort_field, sortAsc=False)
                batch = r.get("quotes", [])
                quotes.extend(batch)
                total = r.get("total", 0)
                break
            except Exception as e:
                log(f"screen {sort_field} p{p} attempt {attempt}: {e!r}")
                time.sleep(4 * (attempt + 1))
        else:
            _state["errors"].append(f"screener page {p} ({sort_field}) failed")
            break
        if (p + 1) * 250 >= (total or 0):
            break
        time.sleep(0.6)
    return quotes


def build_universe():
    """Returns (session, rows). session is PRE / REGULAR / POST / CLOSED."""
    # Probe market state with one small page.
    first = screener_pages("dayvolume", 1, None)
    states = [q.get("marketState") for q in first if q.get("marketState")]
    state = max(set(states), key=states.count) if states else "CLOSED"
    session = {"PRE": "PRE", "PREPRE": "PRE", "REGULAR": "REGULAR",
               "POST": "POST", "POSTPOST": "CLOSED", "CLOSED": "CLOSED"}.get(state, state)

    if session == "REGULAR":
        quotes = first + screener_pages("percentchange", 2, session)
    else:
        # Before the open, percent-change sorting is yesterday's; sweep wide.
        quotes = first + screener_pages("dayvolume", 11, session)[250:]

    if session == "PRE" and quotes:
        have_pm = sum(1 for q in quotes if q.get("preMarketPrice") is not None)
        _state["pm_field_coverage"] = round(have_pm / len(quotes), 3)
        if have_pm < 0.2 * len(quotes):
            return session, premarket_from_bars(quotes)

    seen, rows = set(), []
    for q in quotes:
        s = q.get("symbol")
        if not s or s in seen or q.get("quoteType") not in (None, "EQUITY"):
            continue
        if OTC_EXCH.search(str(q.get("fullExchangeName") or q.get("exchange") or "")):
            continue  # OTC ADRs barely trade and crowd real movers out of the bar budget
        seen.add(s)
        prev_close = q.get("regularMarketPreviousClose")
        reg = q.get("regularMarketPrice")
        if session == "PRE":
            price = q.get("preMarketPrice")
            base = reg  # regularMarketPrice is still yesterday's close
            chg = q.get("preMarketChangePercent")
            if price is None:
                continue
            if chg is None and base:
                chg = (price / base - 1) * 100
            prev_close = base
        elif session == "POST" or session == "CLOSED":
            price = reg
            chg = q.get("regularMarketChangePercent")
        else:
            price = reg
            chg = q.get("regularMarketChangePercent")
        if price is None or chg is None:
            continue
        rows.append({
            "sym": s,
            "name": q.get("shortName") or q.get("longName") or s,
            "price": price,
            "prev_close": prev_close,
            "chg_pct": chg,
            "mcap": q.get("marketCap"),
            "float": q.get("floatShares"),
            "avg_vol": q.get("averageDailyVolume3Month") or q.get("averageDailyVolume10Day"),
            "day_vol": q.get("regularMarketVolume"),
            "pm_vol": q.get("preMarketVolume"),
            "exchange": q.get("fullExchangeName") or q.get("exchange"),
        })
    _state["universe_size"] = len(rows)
    _state["pre_price_coverage"] = sum(1 for r in rows if r["price"]) if session == "PRE" else None
    return session, rows


def premarket_from_bars(quotes):
    """Fallback when the screener omits premarket prices: price the most likely
    movers (yesterday's biggest gainers + most active) from premarket 5m bars."""
    _state["errors"].append("Screener lacks premarket prices; using bar fallback (narrower universe).")
    eq = [q for q in quotes if q.get("symbol") and q.get("regularMarketPrice")]
    by_chg = sorted(eq, key=lambda q: -(q.get("regularMarketChangePercent") or 0))[:250]
    by_vol = sorted(eq, key=lambda q: -(q.get("regularMarketVolume") or 0))[:250]
    pick = {q["symbol"]: q for q in by_chg + by_vol}
    syms = list(pick)
    rows = []
    for i in range(0, len(syms), 100):
        chunk = syms[i:i + 100]
        try:
            data = yf.download(chunk, period="1d", interval="5m", prepost=True, group_by="ticker",
                               progress=False, threads=True, auto_adjust=False)
        except Exception as e:
            _state["errors"].append(f"fallback bars chunk {i}: {e!r}"[:200])
            continue
        today = dt.datetime.now(ET).date()
        for s in chunk:
            try:
                df = data[s].dropna(subset=["Close"])
                if df.index.tz is None:
                    df.index = df.index.tz_localize("UTC")
                df = df.tz_convert(ET)
                df = df[df.index.date == today]
                if df.empty:
                    continue
                q = pick[s]
                base = q["regularMarketPrice"]
                price = float(df["Close"].iloc[-1])
                rows.append({
                    "sym": s, "name": q.get("shortName") or s, "price": price, "prev_close": base,
                    "chg_pct": (price / base - 1) * 100, "mcap": q.get("marketCap"),
                    "float": q.get("floatShares"),
                    "avg_vol": q.get("averageDailyVolume3Month") or q.get("averageDailyVolume10Day"),
                    "day_vol": None, "pm_vol": int(df["Volume"].fillna(0).sum()),
                    "exchange": q.get("fullExchangeName") or q.get("exchange"),
                })
            except Exception:
                continue
        time.sleep(1)
    _state["universe_size"] = len(rows)
    return rows


# ---- 3. Bars & setup ------------------------------------------------------
def ema(vals, n):
    k, out, e = 2 / (n + 1), [], None
    for v in vals:
        e = v if e is None else v * k + e * (1 - k)
        out.append(e)
    return out


def analyze_bars(df, row, session):
    """df: one ticker's 5m bars for today (ET index). Mutates row with setup info."""
    df = df.dropna(subset=["Close"])
    if df.empty:
        row["setup"] = "NO DATA"
        return
    today = dt.datetime.now(ET).date()
    df = df[df.index.date == today]
    if df.empty:
        row["setup"] = "NO DATA"
        return
    t = df.index.time
    open_t = dt.time(9, 30)
    pre = df[[x < open_t for x in t]]
    reg = df[[x >= open_t for x in t]]
    sess = reg if len(reg) >= 3 else (df if len(reg) else pre)
    if sess.empty:
        sess = df

    tp = (sess["High"] + sess["Low"] + sess["Close"]) / 3
    v = sess["Volume"].fillna(0)
    if v.sum() <= 0:  # Yahoo pre-market bars carry no volume: use a time-weighted average
        v = v * 0 + 1
    vwap_series = (tp * v).cumsum() / v.cumsum().replace(0, float("nan"))
    vwap = fnum(vwap_series.iloc[-1], 4)
    closes = list(df["Close"])
    ema9 = fnum(ema(closes, 9)[-1], 4)

    price = float(row["price"])
    sess_high = float(sess["High"].max())
    sess_low = float(sess["Low"].min())
    row.update({
        "vwap": vwap,
        "ema9": ema9,
        "pm_high": fnum(pre["High"].max()) if len(pre) else None,
        "pm_low": fnum(pre["Low"].min()) if len(pre) else None,
        "day_high": fnum(reg["High"].max()) if len(reg) else None,
        "day_low": fnum(reg["Low"].min()) if len(reg) else None,
        "sess_high": fnum(sess_high),
        "sess_low": fnum(sess_low),
        "cum_vol": int(df["Volume"].fillna(0).sum()),
    })
    if session == "PRE" and len(reg) == 0 and len(pre) < 3:
        # Sparse pre-market prints are normal; show it for awareness, never as an entry.
        row["setup"] = "PM-WATCH"
        row["setup_note"] = (f"Pre-market only ({len(pre)} print{'s' if len(pre) != 1 else ''}). "
                             "Watch it into the open; no entry before the first 5-min candle (Rule 8).")
        return
    if vwap is None or len(sess) < 3:
        row["setup"] = "THIN"
        row["setup_note"] = "Too few bars to read yet."
        return

    # Completed bars vs. the bar still forming. The forming bar is judged by its volume
    # PACE (volume so far scaled to 5 minutes) so a surge is seen while it happens,
    # not 5-7 minutes later when the bar closes.
    now = dt.datetime.now(ET)
    now_ct = now.astimezone(CT)
    bars, forming, f_elapsed = sess, None, 0.0
    age = (now - bars.index[-1].to_pydatetime()).total_seconds()
    if age < 300 and len(bars) > 3:
        forming, f_elapsed = bars.iloc[-1], max(age, 1.0)
        bars = bars.iloc[:-1]
    o, h, l, c, vol = (list(bars[k]) for k in ("Open", "High", "Low", "Close", "Volume"))
    vol = [x if x == x else 0 for x in vol]
    last = len(c) - 1
    reg_mode = len(reg) >= 3
    # Baseline = prior 6 completed bars. Once the session has some history, the first two
    # regular-session bars (the opening rush) are left out so they don't bury later surges.
    base_idx = list(range(max(0, last - 6), last))
    if reg_mode and last >= 5:
        base_idx = [i for i in base_idx if i >= 2]
    base = [vol[i] for i in base_idx]
    avg_prior = (sum(base) / len(base)) if base else 0
    surge = (vol[last] / avg_prior) if avg_prior else None
    pace = None
    if forming is not None and avg_prior and f_elapsed >= 60:
        fv = float(forming["Volume"]) if forming["Volume"] == forming["Volume"] else 0.0
        pace = fv * (300.0 / f_elapsed) / avg_prior
    green = c[last] > o[last]
    ext = (price - vwap) / vwap * 100
    support = max(vwap, ema9 or vwap)
    recent_low = min(l[max(0, last - 5):last + 1])
    pulled_back = sess_high > price * 1.025 and recent_low <= support * 1.015
    trigger = green and surge is not None and surge >= SURGE_MIN and last >= 1 and c[last] > h[last - 1]
    near_high = price >= sess_high * 0.985
    late = now_ct.time() >= PRIME_END_CT
    row["late"] = late

    # Red-volume dry-up during the pullback vs. the run.
    reds = [vol[i] for i in range(max(0, last - 6), last) if c[i] < o[i]]
    greens = [vol[i] for i in range(0, last) if c[i] > o[i]]
    drying = bool(reds and greens and (sum(reds) / len(reds)) < 0.6 * (max(greens)))

    row["vol_surge"] = fnum(surge, 2)
    row["vol_pace"] = fnum(pace, 2)
    row["ext_pct"] = fnum(ext, 2)
    row["red_vol_drying"] = drying
    row["spark"] = {
        "c": [fnum(x, 4) for x in closes[-40:]],
        "v": [int(x) if x == x else 0 for x in list(df["Volume"])[-40:]],
        "vwap": [fnum(x, 4) for x in list(vwap_series)[-40:]],
    }

    def lv(entry, stop, t1=None, t2=None):
        """Levels with a hard floor: T1 is never closer than 2R, T2 never closer than 3R."""
        entry, stop = round(entry, 2), round(stop, 2)
        risk = entry - stop
        row["entry"], row["stop"] = entry, stop
        row["t1"] = round(max(t1 or 0, entry + 2 * risk), 2)
        row["t2"] = round(max(t2 or 0, entry + 3 * risk, row["t1"]), 2)
        row["risk_pct"] = fnum(risk / entry * 100, 1) if entry else None
        row["room_r"] = fnum((sess_high - entry) / risk, 2) if risk > 0 else None

    def late_note():
        return " Late session (after 10:30 CT): half size or skip." if late else ""

    # ---- Opening-range breakout (Rule 8), 8:35-9:30 CT ----------------------------
    first = reg.iloc[0] if len(reg) else None
    first_done = first is not None and (now - reg.index[0].to_pydatetime()).total_seconds() >= 300
    if first_done:
        row["rule8"] = bool(first["Close"] > first["Open"])
    if (first_done and row["rule8"] and ORB_START_CT <= now_ct.time() < ORB_END_CT
            and price >= float(first["Low"])):
        o_hi, o_lo = float(first["High"]), float(first["Low"])
        entry, stop = o_hi + 0.01, o_lo - 0.01
        risk = entry - stop
        if risk > 0 and risk / entry <= 0.08:
            if price < entry:
                row["setup"] = "ORB-WATCH"
                row["setup_note"] = (f"First 5-min candle closed green (Rule 8 passed). "
                                     f"Buy the break of its high {o_hi:.2f}; stop under its low {o_lo:.2f}.")
                lv(entry, stop)
                return
            if price <= entry + risk and price > vwap:
                row["setup"] = "ORB-TRIGGER"
                row["setup_note"] = (f"Broke the opening-candle high {o_hi:.2f}"
                                     + (f" on {pace:.1f}x volume pace" if pace else "")
                                     + f". Stop under the candle low {o_lo:.2f}.")
                lv(entry, stop)
                return
            # Already more than 1R past the break: fall through (usually EXTENDED).

    if price < vwap:
        row["setup"] = "FADING"
        row["setup_note"] = f"Below VWAP {vwap:.2f}. Only a VWAP reclaim on volume re-opens it."
        row["reclaim"] = round(vwap, 2)
        return

    # ---- Ignition: the bar forming right now is a volume burst breaking out ---------
    if forming is not None and pace is not None and pace >= IGNITION_PACE and ext <= 8:
        f_o, f_h, f_l = float(forming["Open"]), float(forming["High"]), float(forming["Low"])
        f_rng = f_h - f_l
        prior_hi = max(h[max(0, last - 5):last + 1])
        if (price > f_o and f_rng > 0 and (price - f_l) / f_rng >= 0.6 and price >= prior_hi):
            stop = min(f_l, l[last]) - 0.01
            if (price + 0.01 - stop) / price <= 0.08:
                row["setup"] = "IGNITION"
                row["setup_note"] = (f"Volume burst NOW: this bar is trading at {pace:.1f}x normal pace, "
                                     f"breaking {prior_hi:.2f} above VWAP {vwap:.2f}. Limit at the ask; stop under the bar low."
                                     + late_note())
                lv(price + 0.01, stop)
                return

    if ext > 8:
        row["setup"] = "EXTENDED"
        row["setup_note"] = f"{ext:.1f}% above VWAP. No entry; wait for a pullback toward {support:.2f}."
        row["pullback_zone"] = [round(support, 2), round(support * 1.015, 2)]
    elif pulled_back and trigger:
        lv(h[last] + 0.01, recent_low * 0.995, sess_high)
        if (row.get("room_r") or 0) >= 2:
            row["setup"] = "A-TRIGGER"
            row["setup_note"] = (f"Pulled back to VWAP/EMA9 and printed a green bar on {surge:.1f}x volume."
                                 + (" Red volume dried up." if drying else "") + late_note())
        else:
            row["setup"] = "A-WATCH"
            row["setup_note"] = (f"Green bar on {surge:.1f}x volume, but the high {sess_high:.2f} is under 2R away. "
                                 "Skip unless it clears the high.")
    elif pulled_back:
        row["setup"] = "A-WATCH"
        row["setup_note"] = (f"Holding support near {support:.2f}. Wait for a green bar on volume."
                             + (" Red volume drying up." if drying else " Red volume still heavy."))
        lv(h[last] + 0.01, recent_low * 0.995, sess_high)
    elif near_high and max(surge or 0, (pace or 0) / 1.5) >= SURGE_MIN:
        row["setup"] = "B-BREAKOUT"
        vtxt = f"{surge:.1f}x volume" if (surge or 0) >= SURGE_MIN else f"{pace:.1f}x volume pace"
        row["setup_note"] = f"Pressing the high {sess_high:.2f} on {vtxt}. Half size." + late_note()
        lv(sess_high + 0.01, min(l[max(0, last - 2):last + 1]) * 0.995)
    elif near_high:
        row["setup"] = "B-WATCH"
        row["setup_note"] = f"At the high {sess_high:.2f} without a volume surge yet."
        lv(sess_high + 0.01, min(l[max(0, last - 2):last + 1]) * 0.995)
    else:
        row["setup"] = "HOLDING"
        row["setup_note"] = "Above VWAP, between support and the high. No trigger."


def fetch_bars(rows, session):
    syms = [r["sym"] for r in rows]
    if not syms:
        return
    try:
        data = yf.download(syms, period="1d", interval="5m", prepost=True, group_by="ticker",
                           progress=False, threads=True, auto_adjust=False)
    except Exception as e:
        _state["errors"].append(f"bars download failed: {e!r}"[:300])
        return
    for r in rows:
        try:
            df = data[r["sym"]] if len(syms) > 1 else data
            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC")
            df = df.tz_convert(ET)
            analyze_bars(df, r, session)
        except Exception as e:
            r["setup"] = "NO DATA"
            r["setup_note"] = f"Bars unavailable ({type(e).__name__})."


# ---- 5. Catalyst ----------------------------------------------------------
def check_news(row):
    sym = row["sym"]
    hit = _news_cache.get(sym)
    if hit and time.time() - hit[0] < NEWS_TTL:
        items = hit[1]
    else:
        items = []
        if FINNHUB_KEY:
            today = dt.datetime.now(ET).date()
            frm = (today - dt.timedelta(days=4)).isoformat()
            try:
                r = requests.get("https://finnhub.io/api/v1/company-news",
                                 params={"symbol": sym, "from": frm, "to": today.isoformat(), "token": FINNHUB_KEY},
                                 timeout=15)
                if r.status_code == 200 and isinstance(r.json(), list):
                    items = r.json()[:30]
                elif r.status_code == 429:
                    time.sleep(2)
            except Exception:
                pass
            time.sleep(1.1)  # stay under 60/min
        _news_cache[sym] = (time.time(), items)

    cutoff = time.time() - 3 * 86400
    specific = [n for n in items if n.get("datetime", 0) >= cutoff
                and not ROUNDUP.search(n.get("headline", "")) and about_company(n, row)]
    # Headlines that name the company outrank ones that only mention it in the summary.
    specific.sort(key=lambda n: (not about_company({"headline": n.get("headline", "")}, row),
                                 -n.get("datetime", 0)))
    cat, bad, recap, warns = None, None, None, set()
    for n in specific:
        head = n.get("headline", "")
        if is_recap(n):
            recap = recap or n
            continue
        text = (head + " " + n.get("summary", "")[:300]).lower()
        for w, label in WARN_WORDS.items():
            if w in text:
                warns.add(label)
        if not any(w in text for w in POS_WORDS):
            continue
        if NEG.search(head):
            bad = bad or n          # news exists, but the headline reads bad
            warns.add("Bad-news headline")
            row["bad_headline"] = row.get("bad_headline") or head[:160]
        elif cat is None:
            cat = n
    if cat is not None:
        ctype = catalyst_type(cat.get("headline", "") + " " + cat.get("summary", "")[:200])
        row["catalyst_type"] = ctype
        row["catalyst_strength"] = "analyst" if ctype == "Analyst" else "strong"
    elif bad is not None:
        cat = bad
        row["catalyst_type"] = catalyst_type(bad.get("headline", ""))
        row["catalyst_strength"] = "negative"
        warns.add("Bad-news headline")
    elif recap is not None:
        cat = recap
        row["catalyst_type"] = "Recap"
        row["catalyst_strength"] = "weak"
        warns.add("Recap of a past move, not new news")
    elif specific:
        cat = specific[0]
        row["catalyst_type"] = "Other"
        row["catalyst_strength"] = "weak"
    else:
        row["catalyst_strength"] = "none"
    if cat:
        row["catalyst"] = {
            "headline": cat.get("headline", "")[:220],
            "source": cat.get("source"),
            "url": cat.get("url"),
            "time": dt.datetime.fromtimestamp(cat.get("datetime", 0), CT).strftime("%b %d %H:%M CT"),
        }
    row["warnings"] = sorted(warns)
    row["news_count"] = len(specific)


# ---- Filter & rank --------------------------------------------------------
SETUP_RANK = {"IGNITION": 0, "ORB-TRIGGER": 0, "A-TRIGGER": 0, "ORB-WATCH": 1, "A-WATCH": 1, "B-BREAKOUT": 2, "B-WATCH": 3,
              "HOLDING": 4, "PM-WATCH": 4, "EXTENDED": 5, "THIN": 6, "FADING": 7, "NO DATA": 8}


def apply_filter(r, session=None):
    rel = None
    if r.get("avg_vol") and r.get("cum_vol"):
        rel = r["cum_vol"] / r["avg_vol"]
    r["rel_vol"] = fnum(rel, 2)
    low_float = (r.get("float") or 1e12) < 20e6
    catalyst_ok = r.get("catalyst_strength") in ("strong", "analyst") or (
        low_float and (rel or 0) >= 1.0 and r.get("catalyst_strength") == "weak")
    squeeze = low_float and (rel or 0) >= 2.0
    holding_ok = r.get("setup") not in ("FADING", "NO DATA", "THIN")
    flt = r.get("float")
    dollar_vol = (r.get("cum_vol") or 0) * (r.get("price") or 0)
    r["dollar_vol"] = int(dollar_vol)
    tiny_float = flt is not None and flt < FLOAT_MIN
    dv_min = PM_DOLLAR_MIN if session == "PRE" else DOLLAR_VOL_MIN
    if session == "PRE" and not r.get("cum_vol"):
        # Yahoo often omits pre-market volume. Judge by the name's normal day instead:
        # a stock that usually trades $3M+ will be exitable at the open.
        dollar_vol = (r.get("avg_vol") or 0) * (r.get("price") or 0)
        dv_min = DOLLAR_VOL_MIN
        r["liquidity_basis"] = "avg daily $ (pre-market volume not reported)"
    liquid_ok = (not tiny_float) and dollar_vol >= dv_min and (
        (r.get("mcap") or 0) >= MCAP_MIN or (rel or 0) >= 1.0)
    r["filter"] = {"catalyst": bool(catalyst_ok or squeeze), "holding": holding_ok, "liquidity": liquid_ok}
    if squeeze and not catalyst_ok:
        r["catalyst_strength"] = "squeeze"
    reasons = []
    if not r["filter"]["catalyst"]:
        reasons.append({"unchecked": "News not checked yet",
                        "negative": "Bad-news headline"}.get(r.get("catalyst_strength"), "No catalyst"))
    if not holding_ok:
        reasons.append({"FADING": "Fading below VWAP", "THIN": "Too few bars"}.get(r.get("setup"), "No bar data"))
    if not liquid_ok:
        reasons.append("Tiny float (<5M sh)" if tiny_float else
                       (f"Under ${dv_min/1e6:g}M traded" if dv_min >= 1e6 else f"Under ${dv_min/1e3:g}K traded")
                       if dollar_vol < dv_min else "Thin / small")
    r["reject"] = reasons
    r["pass"] = not reasons


def quality(r):
    """Within a setup tier: participation first, then catalyst weight, then the latest bar."""
    q = min(r.get("rel_vol") or 0, 5.0)
    q += CAT_BONUS.get(r.get("catalyst_type"), 0) if r.get("catalyst_strength") in ("strong", "analyst") else 0
    q += 1.5 if r.get("catalyst_strength") == "squeeze" else 0
    q -= 1.5 if r.get("bad_headline") else 0
    q += min(r.get("vol_surge") or 0, 3.0) * 0.5
    return round(q, 2)


def score(r):
    r["quality"] = quality(r)
    return (SETUP_RANK.get(r.get("setup"), 9), -r["quality"], -(r.get("chg_pct") or 0))


# ---- Float lookup (Finnhub profile, cached for the run) --------------------
_profile_cache = {}


def fill_profile(row):
    """Yahoo's screener rarely carries float; Finnhub's profile does (in millions)."""
    sym = row["sym"]
    if sym not in _profile_cache:
        prof = {}
        if FINNHUB_KEY:
            try:
                r = requests.get("https://finnhub.io/api/v1/stock/profile2",
                                 params={"symbol": sym, "token": FINNHUB_KEY}, timeout=15)
                if r.status_code == 200 and isinstance(r.json(), dict):
                    prof = r.json()
            except Exception:
                pass
            time.sleep(1.1)
        _profile_cache[sym] = prof
    prof = _profile_cache[sym]
    if not row.get("float") and prof.get("floatingShare"):
        row["float"] = float(prof["floatingShare"]) * 1e6
    if not row.get("mcap") and prof.get("marketCapitalization"):
        row["mcap"] = float(prof["marketCapitalization"]) * 1e6


# ---- Signal scoreboard ------------------------------------------------------
# Every actionable signal (A-TRIGGER, B-BREAKOUT) is logged with its levels and then
# scored mechanically from 5-minute bars: did price reach the entry, then the target or
# the stop first? Two exit styles are scored side by side:
#   full  = all shares out at T1 or the stop
#   split = half out at T1, stop on the rest moves to breakeven, rest out at T2,
#           breakeven, or the 14:45 CT time stop
SIGNAL_SETUPS = ("IGNITION", "ORB-TRIGGER", "A-TRIGGER", "B-BREAKOUT")   # BREAK signals come from watch_armed()
RESIGNAL_MIN = 20             # same name + setup can signal again after this many minutes
FILL_WINDOW_MIN = 30           # a signal that never trades through its entry expires
TIME_STOP_ET = dt.time(15, 45)  # 14:45 CT
PRIME_END_CT = dt.time(10, 30)
_signals = None


def load_signals():
    global _signals
    if _signals is not None:
        return _signals
    _signals = []
    if REPO:
        try:
            h = {"Accept": "application/vnd.github.raw+json"}
            if TOKEN:
                h["Authorization"] = f"Bearer {TOKEN}"
            r = requests.get(f"https://api.github.com/repos/{REPO}/contents/signals.json?ref=scan-data",
                             headers=h, timeout=20)
            if r.status_code == 200:
                data = r.json()
                _signals = data if isinstance(data, list) else data.get("signals", [])
        except Exception as e:
            _state["errors"].append(f"signal history load failed: {e!r}"[:200])
    log(f"loaded {len(_signals)} past signals")
    return _signals


def log_new_signals(survivors):
    sigs = load_signals()
    now_ct = dt.datetime.now(CT)
    today = now_ct.date().isoformat()
    cutoff = time.time() - RESIGNAL_MIN * 60
    recent = {(x["sym"], x["setup"]) for x in sigs if x["date"] == today and x.get("ts", 0) >= cutoff}
    orb_today = {(x["sym"], x["setup"]) for x in sigs if x["date"] == today and x["setup"] == "ORB-TRIGGER"}
    signalled = {x["sym"] for x in sigs if x["date"] == today}
    for r in survivors:
        if r.get("setup") not in SIGNAL_SETUPS or not r.get("entry") or not r.get("stop"):
            continue
        if (r["sym"], r["setup"]) in recent:
            continue
        if r["setup"] == "ORB-TRIGGER" and (r["sym"], "ORB-TRIGGER") in orb_today:
            continue  # one opening-range break per name per day
        if r["entry"] <= r["stop"]:
            continue
        sig = {
            "id": f"{today}-{r['sym']}-{r['setup']}-{now_ct:%H%M}",
            "date": today, "time_ct": now_ct.strftime("%H:%M"), "ts": int(time.time()),
            "sym": r["sym"], "setup": r["setup"],
            "entry": r["entry"], "stop": r["stop"], "t1": r.get("t1"), "t2": r.get("t2"),
            "price_at_signal": r.get("price"),
            "catalyst_type": r.get("catalyst_type"), "catalyst_strength": r.get("catalyst_strength"),
            "bad_headline": bool(r.get("bad_headline")), "rel_vol": r.get("rel_vol"),
            "vol_surge": r.get("vol_surge"), "float": r.get("float"),
            "prime": now_ct.time() < PRIME_END_CT,
            "vol_pace": r.get("vol_pace"), "room_r": r.get("room_r"),
            "status": "pending",
        }
        sig["repeat"] = r["sym"] in signalled   # later signals on a name already flagged today: logged, not pushed
        sigs.append(sig)
        recent.add((r["sym"], r["setup"]))
        signalled.add(r["sym"])
        if r["setup"] == "ORB-TRIGGER":
            orb_today.add((r["sym"], "ORB-TRIGGER"))
        if not sig["repeat"] and (r.get("rel_vol") or 0) >= PUSH_MIN_RV:
            push_signal(sig, r)   # low-volume triggers are still logged and scored, just not pushed


def push_signal(sig, r):
    """Phone push through ntfy.sh. The topic name lives in the NTFY_TOPIC repo secret."""
    if not NTFY_TOPIC or not sig.get("prime"):
        return  # after 10:30 CT signals are logged for the record but not pushed (replay: -0.27R avg)
    risk = sig["entry"] - sig["stop"]
    late = not sig.get("prime")
    title = f"{'LATE ' if late else ''}{sig['sym']} {sig['setup']} {sig['time_ct']} CT"
    cat = f"{r.get('catalyst_type') or 'no type'} ({r.get('catalyst_strength')})"
    body = (f"Entry >= {sig['entry']:.2f} | stop {sig['stop']:.2f} | T1 {sig['t1']:.2f} (2R) | "
            f"risk {risk / sig['entry'] * 100:.1f}%\n{r.get('setup_note') or ''}\nCatalyst: {cat}"
            + ("\nBAD-NEWS HEADLINE" if r.get("bad_headline") else "")
            + ("".join(f"\nWarning: {w}" for w in (r.get("warnings") or []))))
    try:
        requests.post(f"https://ntfy.sh/{NTFY_TOPIC}", data=body.encode("utf-8"), timeout=10, headers={
            "Title": title.encode("ascii", "ignore").decode(),
            "Priority": "3" if late else "5",
            "Tags": "warning" if late or r.get("bad_headline") else "rocket",
            "Click": f"https://{REPO.split('/')[0]}.github.io/{REPO.split('/')[-1]}/scanner.html" if REPO else "",
        })
    except Exception as e:
        _state["errors"].append(f"push failed: {e!r}"[:150])


def _r_mult(sig, price):
    risk = sig["entry"] - sig["stop"]
    return round((price - sig["entry"]) / risk, 2) if risk > 0 else 0.0


def shadow_1r(sig, bars):
    """Comparison exit plan, recorded but not traded: sell half at +1R, stop the rest at
    breakeven, rest out at T2 (3R) or the 14:45 CT time stop. Also records MFE in R."""
    entry, stop = sig["entry"], sig["stop"]
    R = entry - stop
    if R <= 0:
        return
    t2 = sig.get("t2") or entry + 3 * R
    filled, half, mfe = False, False, None
    for ts, b in bars.iterrows():
        hi, lo, cl = float(b["High"]), float(b["Low"]), float(b["Close"])
        if not filled:
            if hi < entry:
                continue
            filled = True
            if lo <= stop:
                sig["r_1r"], sig["mfe_r"] = -1.0, round(max(0.0, (hi - entry) / R), 2)
                return
        mfe = max(mfe or 0.0, (hi - entry) / R)
        if ts.time() >= TIME_STOP_ET:
            rest = (cl - entry) / R
            sig["r_1r"] = round(0.5 + 0.5 * rest, 2) if half else round(rest, 2)
            break
        if not half:
            if lo <= stop:
                sig["r_1r"] = -1.0
                break
            if hi >= entry + R:
                half = True
                if hi >= t2:
                    sig["r_1r"] = round(0.5 + 0.5 * (t2 - entry) / R, 2)
                    break
            continue
        if lo <= entry:
            sig["r_1r"] = 0.5
            break
        if hi >= t2:
            sig["r_1r"] = round(0.5 + 0.5 * (t2 - entry) / R, 2)
            break
    if mfe is not None:
        sig["mfe_r"] = round(mfe, 2)


def resolve_signal(sig, df):
    """Walk 5m bars after the signal. Conservative: if one bar touches both stop and
    target, the stop is assumed to have hit first."""
    sig_ts = dt.datetime.fromtimestamp(sig["ts"], ET)
    bars = df[df.index >= sig_ts - dt.timedelta(minutes=5)]
    if bars.empty:
        return
    try:
        shadow_1r(sig, bars)
    except Exception:
        pass
    entry, stop = sig["entry"], sig["stop"]
    t1 = sig.get("t1") or entry + 2 * (entry - stop)
    t2 = sig.get("t2") or entry + 3 * (entry - stop)
    filled_at = None
    half_done, rest_stop = False, stop
    for ts, b in bars.iterrows():
        hi, lo, cl = float(b["High"]), float(b["Low"]), float(b["Close"])
        if filled_at is None:
            if hi >= entry:
                filled_at = ts
                sig["status"] = "open"
                sig["fill_time_ct"] = ts.astimezone(CT).strftime("%H:%M")
                # The fill bar itself can stop out (entry and stop both inside its range).
                if lo <= stop:
                    _close(sig, "loss", stop, stop, ts, half_done=False)
                    return
                continue
            if (ts - sig_ts).total_seconds() > FILL_WINDOW_MIN * 60:
                sig["status"] = "no_fill"
                return
            continue
        if ts.time() >= TIME_STOP_ET:
            _close(sig, "time", cl, cl, ts, half_done)
            return
        if not half_done:
            if lo <= stop:
                _close(sig, "loss", stop, stop, ts, False)
                return
            if hi >= t1:
                half_done = True
                sig["t1_time_ct"] = ts.astimezone(CT).strftime("%H:%M")
                rest_stop = entry
                if hi >= t2:
                    _close(sig, "win", t1, t2, ts, True)
                    return
            continue
        if lo <= rest_stop:
            _close(sig, "win", t1, rest_stop, ts, True)
            return
        if hi >= t2:
            _close(sig, "win", t1, t2, ts, True)
            return
    # Still running: record a live mark so the page can show it.
    if filled_at is not None:
        sig["mark_r"] = _r_mult(sig, float(bars["Close"].iloc[-1]))


def _close(sig, status, full_exit, rest_exit, ts, half_done):
    sig["exit_time_ct"] = ts.astimezone(CT).strftime("%H:%M")
    sig["r_full"] = _r_mult(sig, full_exit)
    if half_done:
        sig["r_split"] = round(0.5 * _r_mult(sig, full_exit) + 0.5 * _r_mult(sig, rest_exit), 2)
    else:
        sig["r_split"] = sig["r_full"]
    if status != "time":
        # Label by the actual result, never by which exit fired (a T1 set below the entry
        # once logged a "win" at negative R).
        status = "win" if sig["r_split"] > 0 else "loss" if sig["r_split"] < 0 else "scratch"
    sig["status"] = status
    sig.pop("mark_r", None)


def update_signals():
    sigs = load_signals()
    today = dt.datetime.now(CT).date().isoformat()
    live = [x for x in sigs if x["date"] == today and x["status"] in ("pending", "open")]
    if not live:
        return
    syms = sorted({x["sym"] for x in live})
    try:
        data = yf.download(syms, period="1d", interval="5m", prepost=False, group_by="ticker",
                           progress=False, threads=True, auto_adjust=False)
    except Exception as e:
        _state["errors"].append(f"signal bars failed: {e!r}"[:200])
        return
    for sig in live:
        try:
            df = data[sig["sym"]] if len(syms) > 1 else data
            df = df.dropna(subset=["Close"])
            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC")
            df = df.tz_convert(ET)
            resolve_signal(sig, df)
        except Exception as e:
            sig.setdefault("errors", 0)
            sig["errors"] += 1


def close_stale_signals():
    """Signals left open from a prior day (job stopped early) are marked unresolved."""
    today = dt.datetime.now(CT).date().isoformat()
    for x in load_signals():
        if x["date"] < today and x["status"] in ("pending", "open"):
            x["status"] = "unresolved"


def signal_stats():
    done = [x for x in load_signals() if x["status"] in ("win", "loss", "time", "scratch")]

    def agg(rows):
        if not rows:
            return {"n": 0}
        rf = [x["r_full"] for x in rows]
        rs = [x["r_split"] for x in rows]
        r1 = [x["r_1r"] for x in rows if x.get("r_1r") is not None]
        mf = [x["mfe_r"] for x in rows if x.get("mfe_r") is not None]
        return {"n": len(rows), "win_rate": round(sum(1 for r in rf if r > 0) / len(rows), 3),
                "avg_r_full": round(sum(rf) / len(rows), 2), "avg_r_split": round(sum(rs) / len(rows), 2),
                "total_r_full": round(sum(rf), 2), "total_r_split": round(sum(rs), 2),
                "avg_r_1r": round(sum(r1) / len(r1), 2) if r1 else None, "n_1r": len(r1),
                "avg_mfe_r": round(sum(mf) / len(mf), 2) if mf else None}

    clean = lambda x: x.get("catalyst_strength") in ("strong", "squeeze") and not x.get("bad_headline")
    groups = {
        "all": done,
        "ORB-TRIGGER": [x for x in done if x["setup"] == "ORB-TRIGGER"],
        "IGNITION": [x for x in done if x["setup"] == "IGNITION"],
        "BREAK (armed level)": [x for x in done if x["setup"] == "BREAK"],
        "A-TRIGGER": [x for x in done if x["setup"] == "A-TRIGGER"],
        "B-BREAKOUT": [x for x in done if x["setup"] == "B-BREAKOUT"],
        "prime (before 10:30 CT)": [x for x in done if x.get("prime")],
        "after 10:30 CT": [x for x in done if not x.get("prime")],
        "clean catalyst": [x for x in done if clean(x)],
        "A-TRIGGER + prime + clean": [x for x in done if x["setup"] == "A-TRIGGER" and x.get("prime") and clean(x)],
        "rel vol >= 1.5 at signal": [x for x in done if (x.get("rel_vol") or 0) >= 1.5],
        "rel vol < 1.5 at signal": [x for x in done if (x.get("rel_vol") or 0) < 1.5],
        "first signal on the name": [x for x in done if not x.get("repeat")],
        "repeat signal on the name": [x for x in done if x.get("repeat")],
    }
    all_sigs = load_signals()
    return {
        "groups": {k: agg(v) for k, v in groups.items()},
        "days": len({x["date"] for x in all_sigs}),
        "no_fill": sum(1 for x in all_sigs if x["status"] == "no_fill"),
        "since": min((x["date"] for x in all_sigs), default=None),
    }


def write_signals():
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(os.path.join(OUT_DIR, "signals.json"), "w") as f:
        json.dump(load_signals(), f, separators=(",", ":"), default=str)


# ---- Output ---------------------------------------------------------------
def publish(payload):
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(os.path.join(OUT_DIR, "scan.json"), "w") as f:
        json.dump(payload, f, separators=(",", ":"), default=str)
    if not (REPO and TOKEN):
        log("No repo/token; wrote out/scan.json locally only.")
        return
    url = f"https://x-access-token:{TOKEN}@github.com/{REPO}.git"
    cmds = [
        "rm -rf .git", "git init -q -b scan-data",
        "git config user.name 'Scanner Bot'", "git config user.email bot@stockprices.local",
        "git add -A", f"git commit -qm 'scan {dt.datetime.now(CT):%H:%M CT}'",
        f"git push -qf {url} scan-data",
    ]
    for c in cmds:
        p = subprocess.run(c, shell=True, cwd=OUT_DIR, capture_output=True, text=True)
        if p.returncode != 0:
            log(f"git step failed: {c.split(' x-access')[0][:40]} :: {p.stderr[:200]}")
            return


_quote_cache = {}


def fetch_quotes(syms):
    """Finnhub quotes for names signalled today, so the page's position cards never freeze
    at an old scan price when a browser has no key. Cached 45s; ~25 names max."""
    out = {}
    if not FINNHUB_KEY:
        return out
    for s in list(syms)[:25]:
        hit = _quote_cache.get(s)
        if hit and time.time() - hit["t"] < 45:
            out[s] = hit
            continue
        try:
            r = requests.get("https://finnhub.io/api/v1/quote", params={"symbol": s, "token": FINNHUB_KEY}, timeout=8)
            if r.status_code == 200:
                q = r.json()
                if q.get("c"):
                    _quote_cache[s] = out[s] = {"p": q["c"], "h": q.get("h"), "t": time.time()}
            elif r.status_code == 429:
                break
        except Exception:
            pass
        time.sleep(0.3)
    return out


_lock = threading.RLock()
_armed = {}            # sym -> armed level dict, refreshed every scan loop
_wake = threading.Event()


def today_ct():
    return dt.datetime.now(CT).date().isoformat()


def archive_dir(day=None):
    d = os.path.join(OUT_DIR, "archive", day or today_ct())
    os.makedirs(d, exist_ok=True)
    return d


def archive_append(name, obj):
    """Append one JSON line to today's archive file; published with the scan, restored on restart."""
    try:
        with open(os.path.join(archive_dir(), name), "a") as f:
            f.write(json.dumps(obj, separators=(",", ":"), default=str) + "\n")
    except Exception as e:
        _state["errors"].append(f"archive: {e!r}"[:150])


def archive_restore():
    """A new run starts with an empty out/ dir: pull today's archive files back from scan-data."""
    if not REPO:
        return
    h = {"Accept": "application/vnd.github.raw+json"}
    if TOKEN:
        h["Authorization"] = f"Bearer {TOKEN}"
    day = today_ct()
    need = {"board.jsonl": "session", "breaks.jsonl": "result"}   # a real line always has this key
    for name, key in need.items():
        try:
            r = requests.get(f"https://api.github.com/repos/{REPO}/contents/archive/{day}/{name}?ref=scan-data",
                             headers=h, timeout=30)
            if r.status_code == 200:
                keep = []
                for line in r.content.decode("utf-8", "ignore").splitlines():
                    try:
                        if key in json.loads(line):
                            keep.append(line)
                    except Exception:
                        pass
                with open(os.path.join(archive_dir(day), name), "w") as f:
                    f.write("".join(x + "\n" for x in keep))
                log(f"restored archive/{day}/{name} ({len(keep)} lines)")
        except Exception as e:
            log(f"archive restore {name} failed: {e!r}")


def archive_prune():
    root = os.path.join(OUT_DIR, "archive")
    if not os.path.isdir(root):
        return
    cutoff = (dt.datetime.now(CT).date() - dt.timedelta(days=ARCHIVE_KEEP_DAYS)).isoformat()
    for d in os.listdir(root):
        if d < cutoff:
            subprocess.run(["rm", "-rf", os.path.join(root, d)])


def board_line(session, survivors):
    keys = ("sym", "price", "chg_pct", "setup", "entry", "stop", "t1", "t2", "vwap", "sess_high",
            "rel_vol", "vol_surge", "vol_pace", "room_r", "ext_pct", "catalyst_type", "catalyst_strength", "late")
    return {"t": dt.datetime.now(CT).strftime("%H:%M:%S"), "session": session,
            "armed": sorted(_armed), "rows": [{k: r.get(k) for k in keys} for r in survivors[:25]]}


def put_main(path, raw_bytes, msg):
    """Write one file to main through the contents API (archive/ is outside the scanner's push trigger)."""
    h = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/vnd.github+json"}
    url = f"https://api.github.com/repos/{REPO}/contents/{path}"
    sha = None
    r = requests.get(url, headers=h, params={"ref": "main"}, timeout=20)
    if r.status_code == 200:
        sha = r.json().get("sha")
    body = {"message": msg, "content": base64.b64encode(raw_bytes).decode(), "branch": "main"}
    if sha:
        body["sha"] = sha
    r = requests.put(url, headers=h, json=body, timeout=60)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"PUT {path} -> {r.status_code} {r.text[:120]}")


def archive_day_close():
    """Once after 15:05 CT: save the day's signals, board history and 1-minute bars for every name
    that appeared, and copy them to main under archive/<date>/ so they're kept for good."""
    day = today_ct()
    d = archive_dir(day)
    if os.path.exists(os.path.join(d, "done")) or not (REPO and TOKEN):
        return
    syms = set()
    try:
        with open(os.path.join(d, "board.jsonl")) as f:
            for line in f:
                try:
                    syms.update(r["sym"] for r in json.loads(line)["rows"])
                except Exception:
                    pass
    except FileNotFoundError:
        pass
    sigs = [x for x in load_signals() if x["date"] == day]
    syms.update(x["sym"] for x in sigs)
    syms = sorted(syms)
    files = {"signals.json": json.dumps(sigs, separators=(",", ":"), default=str).encode()}
    if syms:
        try:
            frames = []
            for i in range(0, len(syms), 40):
                chunk = syms[i:i + 40]
                data = yf.download(chunk, period="1d", interval="1m", prepost=True, group_by="ticker",
                                   progress=False, threads=True, auto_adjust=False)
                for s_ in chunk:
                    try:
                        df = (data[s_] if len(chunk) > 1 else data).dropna(subset=["Close"]).copy()
                        df["sym"] = s_
                        frames.append(df)
                    except Exception:
                        pass
            if frames:
                import pandas as pd
                allb = pd.concat(frames)
                allb.index = allb.index.tz_convert(CT)
                buf = io.BytesIO()
                with gzip.GzipFile(fileobj=buf, mode="wb") as g:
                    g.write(allb.to_csv().encode())
                files["bars_1m.csv.gz"] = buf.getvalue()
        except Exception as e:
            log(f"close archive bars failed: {e!r}")
    for name in ("board.jsonl", "breaks.jsonl"):
        fp = os.path.join(d, name)
        if os.path.exists(fp):
            with open(fp, "rb") as f:
                files[name + ".gz"] = gzip.compress(f.read())
    try:
        for name, raw in files.items():
            put_main(f"archive/{day}/{name}", raw, f"archive {day} {name}")
        open(os.path.join(d, "done"), "w").write(dt.datetime.now(CT).isoformat())
        log(f"archived {day}: {', '.join(files)} ({len(syms)} symbols)")
    except Exception as e:
        _state["errors"].append(f"close archive: {e!r}"[:200])


def make_signal(r, setup, now_ct, price):
    return {
        "id": f"{now_ct.date().isoformat()}-{r['sym']}-{setup}-{now_ct:%H%M%S}",
        "date": now_ct.date().isoformat(), "time_ct": now_ct.strftime("%H:%M"), "ts": int(time.time()),
        "sym": r["sym"], "setup": setup,
        "entry": r["entry"], "stop": r["stop"], "t1": r.get("t1"), "t2": r.get("t2"),
        "price_at_signal": price,
        "catalyst_type": r.get("catalyst_type"), "catalyst_strength": r.get("catalyst_strength"),
        "bad_headline": bool(r.get("bad_headline")), "rel_vol": r.get("rel_vol"),
        "vol_surge": r.get("vol_surge"), "float": r.get("float"),
        "prime": now_ct.time() < PRIME_END_CT,
        "vol_pace": r.get("vol_pace"), "room_r": r.get("room_r"),
        "status": "pending",
    }


def arm_levels(session, survivors):
    """Pick the names worth watching tick by tick: a known entry, real volume, not late."""
    armed = {}
    if session == "REGULAR" and dt.datetime.now(CT).time() < dt.time(14, 30):
        for r in survivors:
            if (r.get("setup") in ARM_SETUPS and r.get("entry") and r.get("stop")
                    and r["entry"] > r["stop"] and (r.get("rel_vol") or 0) >= ARM_MIN_RV
                    and not r.get("bad_headline")):
                R = r["entry"] - r["stop"]
                armed[r["sym"]] = {
                    "sym": r["sym"], "setup": r["setup"], "entry": r["entry"], "stop": r["stop"],
                    "t1": r.get("t1"), "t2": r.get("t2"), "rel_vol": r.get("rel_vol"),
                    "buy_limit": round(r["entry"] + BUY_LIMIT_R * R, 2),
                    "row": {k: r.get(k) for k in ("sym", "entry", "stop", "t1", "t2", "catalyst_type",
                                                  "catalyst_strength", "bad_headline", "rel_vol", "vol_surge",
                                                  "float", "vol_pace", "room_r", "setup_note", "warnings")},
                }
            if len(armed) >= ARM_MAX:
                break
    with _lock:
        _armed.clear()
        _armed.update(armed)
    return [{k: v for k, v in a.items() if k != "row"} for a in armed.values()]


def watch_armed():
    """Background thread: every WATCH_SECONDS, read 1-minute bars for armed names and fire the
    moment price trades through the armed entry on volume."""
    fired = set()
    while True:
        time.sleep(WATCH_SECONDS)
        try:
            with _lock:
                armed = {k: dict(v) for k, v in _armed.items()}
            if not armed:
                continue
            syms = sorted(armed)
            data = yf.download(syms, period="1d", interval="1m", prepost=False, group_by="ticker",
                               progress=False, threads=True, auto_adjust=False)
            now_ct = dt.datetime.now(CT)
            for s_ in syms:
                a = armed[s_]
                key = (now_ct.date().isoformat(), s_, a["entry"])
                if key in fired:
                    continue
                try:
                    df = (data[s_] if len(syms) > 1 else data).dropna(subset=["Close"])
                except Exception:
                    continue
                if len(df) < 5:
                    continue
                last = df.iloc[-1]
                price, hi = float(last["Close"]), float(last["High"])
                if hi < a["entry"]:
                    continue
                vols = df["Volume"].iloc[-21:-1]
                base = float(vols.median()) if len(vols) else 0.0
                vol_x = float(last["Volume"]) / base if base > 0 else None
                R = a["entry"] - a["stop"]
                past_r = (price - a["entry"]) / R if R > 0 else 0
                fired.add(key)
                ev = {"t": now_ct.strftime("%H:%M:%S"), "sym": s_, "armed_from": a["setup"],
                      "entry": a["entry"], "stop": a["stop"], "price": price, "past_r": round(past_r, 2),
                      "bar_vol_x": round(vol_x, 2) if vol_x else None}
                if price < a["entry"]:
                    ev["result"] = "touched and rejected"   # wicked through, closed back under
                elif past_r > CHASE_R:
                    ev["result"] = "already past chase limit"
                else:
                    ev["result"] = "BREAK"
                archive_append("breaks.jsonl", ev)
                log(f"armed {s_} {ev['result']} @ {price} (entry {a['entry']}, {past_r:+.2f}R, bar vol {vol_x})")
                if ev["result"] != "BREAK":
                    continue
                with _lock:
                    sigs = load_signals()
                    already = any(x["date"] == now_ct.date().isoformat() and x["sym"] == s_ for x in sigs)
                    sig = make_signal(a["row"], "BREAK", now_ct, price)
                    sig["armed_from"] = a["setup"]
                    sig["bar_vol_x"] = ev["bar_vol_x"]
                    sig["buy_limit"] = a["buy_limit"]
                    sig["repeat"] = already
                    sigs.append(sig)
                if not already and (vol_x or 0) >= 1.5:   # light-volume breaks are logged and scored, not pushed
                    push_signal(sig, dict(a["row"], setup_note=(
                        f"Broke armed entry {a['entry']:.2f} ({a['setup']}), now {price:.2f}. "
                        f"Buy limit no higher than {a['buy_limit']:.2f}; stop {a['stop']:.2f}.")))
                _wake.set()
        except Exception as e:
            log(f"watcher error: {e!r}")


def one_loop():
    _state["loop"] += 1
    _state["errors"] = []
    t0 = time.time()
    session, rows = build_universe()
    movers = [r for r in rows
              if (r["chg_pct"] or 0) >= GAP_MIN and PRICE_MIN <= r["price"] <= PRICE_MAX]
    movers.sort(key=lambda r: -r["chg_pct"])
    top = movers[:MAX_BAR_NAMES]
    fetch_bars(top, session)
    for r in top:
        if session == "PRE":
            # Yahoo pre-market bars carry no volume and day_vol is still yesterday's;
            # the screener's preMarketVolume is the only honest number before the open.
            r["cum_vol"] = r.get("pm_vol") or r.get("cum_vol") or 0
        else:
            r["cum_vol"] = r.get("cum_vol") or r.get("day_vol") or r.get("pm_vol")
    # News for the strongest candidates (by preliminary setup + gap).
    prelim = sorted(top, key=lambda r: (SETUP_RANK.get(r.get("setup"), 9) >= 7, -r["chg_pct"]))
    for r in prelim[:MAX_NEWS_NAMES]:
        check_news(r)
    # Names below VWAP still get their news read when budget allows (cached 20 min).
    for r in [x for x in top if "catalyst_strength" not in x][:10]:
        check_news(r)
    for r in prelim[:MAX_NEWS_NAMES]:
        fill_profile(r)
    for r in top:
        r.setdefault("catalyst_strength", "unchecked")
        apply_filter(r, session)
    survivors = sorted([r for r in top if r["pass"]], key=score)
    rejected = sorted([r for r in top if not r["pass"]], key=lambda r: -r["chg_pct"])
    for r in survivors + rejected:
        for k in ("price", "prev_close", "chg_pct"):
            r[k] = fnum(r.get(k), 4 if k != "chg_pct" else 2)
    armed_out = arm_levels(session, survivors)
    try:
        _lock.acquire()
        close_stale_signals()
        if session == "REGULAR":
            log_new_signals(survivors)
        update_signals()
        write_signals()
        today = dt.datetime.now(CT).date().isoformat()
        signals_today = [x for x in load_signals() if x["date"] == today]
        stats = signal_stats()
    except Exception as e:
        _state["errors"].append(f"scoreboard: {e!r}"[:200])
        signals_today, stats = [], {}
    finally:
        _lock.release()
    quotes = {}
    if session == "REGULAR":
        try:
            quotes = fetch_quotes(sorted({x["sym"] for x in signals_today}))
        except Exception as e:
            _state["errors"].append(f"quotes: {e!r}"[:150])
    now = dt.datetime.now(dt.timezone.utc)
    payload = {
        "armed": armed_out,
        "quotes": quotes,
        "updated_utc": now.isoformat(timespec="seconds"),
        "updated_ct": now.astimezone(CT).strftime("%Y-%m-%d %H:%M:%S CT"),
        "session": session,
        "loop": _state["loop"],
        "loop_seconds": LOOP_SECONDS,
        "scan_seconds": round(time.time() - t0, 1),
        "params": {"gap_min": GAP_MIN, "price": [PRICE_MIN, PRICE_MAX], "mcap_min": MCAP_MIN},
        "universe_size": _state.get("universe_size"),
        "pm_field_coverage": _state.get("pm_field_coverage"),
        "movers_count": len(movers),
        "survivors": survivors,
        "rejected": [{k: r.get(k) for k in ("sym", "name", "price", "chg_pct", "setup", "reject",
                                             "catalyst_strength", "catalyst_type", "rel_vol", "mcap", "ext_pct", "warnings")}
                     for r in rejected],
        "signals_today": signals_today,
        "signal_stats": stats,
        "errors": _state["errors"],
    }
    archive_append("board.jsonl", board_line(session, survivors))
    if session != "PRE" and dt.datetime.now(CT).time() >= dt.time(15, 5):
        archive_day_close()
    with _lock:
        publish(payload)
    log(f"loop {_state['loop']} {session}: universe={len(rows)} movers={len(movers)} "
        f"survivors={len(survivors)} in {time.time() - t0:.0f}s")
    return session


def relay():
    """GitHub jobs stop at 6h and its cron can run hours late, so a run that hits its
    time limit before the close starts its own successor."""
    if not (REPO and TOKEN):
        return
    try:
        r = requests.post(f"https://api.github.com/repos/{REPO}/actions/workflows/scanner.yml/dispatches",
                          headers={"Authorization": f"Bearer {TOKEN}",
                                   "Accept": "application/vnd.github+json"},
                          json={"ref": "main"}, timeout=20)
        log(f"relay dispatch -> HTTP {r.status_code}")
    except Exception as e:
        log(f"relay dispatch failed: {e!r}")


def main():
    start = time.time()
    subprocess.run(["rm", "-rf", os.path.join(OUT_DIR, "archive")])   # start clean; today's files come back via restore
    archive_restore()
    archive_prune()
    threading.Thread(target=watch_armed, daemon=True).start()
    end_today = dt.datetime.now(CT).replace(hour=END_CT_HHMM[0], minute=END_CT_HHMM[1],
                                            second=0, microsecond=0)
    deadline = min(start + RUN_MINUTES * 60, end_today.timestamp())
    while True:
        t = time.time()
        try:
            session = one_loop()
        except Exception:
            log("loop crashed:\n" + traceback.format_exc())
            session = "ERROR"
        if ONE_SHOT:
            break
        if time.time() >= deadline:
            if time.time() < end_today.timestamp() - 300:
                relay()
            break
        if session == "CLOSED" and dt.datetime.now(ET).hour >= 16:
            log("Market closed for the day; exiting.")
            break
        _wake.wait(max(10, LOOP_SECONDS - (time.time() - t)))
        _wake.clear()


if __name__ == "__main__":
    main()
