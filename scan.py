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
import subprocess
import sys
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
AVGVOL_MIN = 150_000
MAX_BAR_NAMES = 40          # names that get 5m bars each loop
MAX_NEWS_NAMES = 40         # names that get a Finnhub news check (cached 20 min)
NEWS_TTL = 20 * 60          # seconds to cache a news lookup
LOOP_SECONDS = int(os.environ.get("LOOP_SECONDS", 120))
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
    if vwap is None or len(sess) < 3:
        row["setup"] = "THIN"
        row["setup_note"] = "Too few bars to read yet."
        return

    # Completed bars: drop the forming bar if it started < 5 minutes ago.
    now = dt.datetime.now(ET)
    bars = sess
    if (now - bars.index[-1].to_pydatetime()).total_seconds() < 300 and len(bars) > 3:
        bars = bars.iloc[:-1]
    o, h, l, c, vol = (list(bars[k]) for k in ("Open", "High", "Low", "Close", "Volume"))
    vol = [x if x == x else 0 for x in vol]
    last = len(c) - 1
    prior = vol[max(0, last - 6):last]
    avg_prior = (sum(prior) / len(prior)) if prior else 0
    surge = (vol[last] / avg_prior) if avg_prior else None
    green = c[last] > o[last]
    ext = (price - vwap) / vwap * 100
    support = max(vwap, ema9 or vwap)
    recent_low = min(l[max(0, last - 5):last + 1])
    pulled_back = sess_high > price * 1.025 and recent_low <= support * 1.015
    trigger = green and surge is not None and surge >= 1.5 and last >= 1 and c[last] > h[last - 1]
    near_high = price >= sess_high * 0.985

    # Red-volume dry-up during the pullback vs. the run.
    reds = [vol[i] for i in range(max(0, last - 6), last) if c[i] < o[i]]
    greens = [vol[i] for i in range(0, last) if c[i] > o[i]]
    drying = bool(reds and greens and (sum(reds) / len(reds)) < 0.6 * (max(greens)))

    row["vol_surge"] = fnum(surge, 2)
    row["ext_pct"] = fnum(ext, 2)
    row["red_vol_drying"] = drying
    row["spark"] = {
        "c": [fnum(x, 4) for x in closes[-40:]],
        "v": [int(x) if x == x else 0 for x in list(df["Volume"])[-40:]],
        "vwap": [fnum(x, 4) for x in list(vwap_series)[-40:]],
    }

    def lv(entry, stop, t1, t2=None):
        entry, stop = round(entry, 2), round(stop, 2)
        risk = entry - stop
        row["entry"], row["stop"] = entry, stop
        row["t1"] = round(t1, 2)
        row["t2"] = round(t2, 2) if t2 else round(entry + 2 * risk, 2)
        row["risk_pct"] = fnum(risk / entry * 100, 1) if entry else None

    if price < vwap:
        row["setup"] = "FADING"
        row["setup_note"] = f"Below VWAP {vwap:.2f}. Only a VWAP reclaim on volume re-opens it."
        row["reclaim"] = round(vwap, 2)
    elif ext > 8:
        row["setup"] = "EXTENDED"
        row["setup_note"] = f"{ext:.1f}% above VWAP. No entry; wait for a pullback toward {support:.2f}."
        row["pullback_zone"] = [round(support, 2), round(support * 1.015, 2)]
    elif pulled_back and trigger:
        row["setup"] = "A-TRIGGER"
        row["setup_note"] = (f"Pulled back to VWAP/EMA9 and printed a green bar on {surge:.1f}x volume."
                             + (" Red volume dried up." if drying else ""))
        lv(h[last] + 0.01, recent_low * 0.995, sess_high, sess_high + (sess_high - recent_low) * 0.5)
    elif pulled_back:
        row["setup"] = "A-WATCH"
        row["setup_note"] = (f"Holding support near {support:.2f}. Wait for a green bar on volume."
                             + (" Red volume drying up." if drying else " Red volume still heavy."))
        lv(h[last] + 0.01, recent_low * 0.995, sess_high)
    elif near_high and (surge or 0) >= 2:
        row["setup"] = "B-BREAKOUT"
        row["setup_note"] = f"Pressing the high {sess_high:.2f} on {surge:.1f}x volume. Half size."
        lv(sess_high + 0.01, min(l[max(0, last - 2):last + 1]) * 0.995, sess_high * 1.05)
    elif near_high:
        row["setup"] = "B-WATCH"
        row["setup_note"] = f"At the high {sess_high:.2f} without a volume surge yet."
        lv(sess_high + 0.01, min(l[max(0, last - 2):last + 1]) * 0.995, sess_high * 1.05)
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
    cat, bad, warns = None, None, set()
    for n in specific:
        head = n.get("headline", "")
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
SETUP_RANK = {"A-TRIGGER": 0, "A-WATCH": 1, "B-BREAKOUT": 2, "B-WATCH": 3,
              "HOLDING": 4, "EXTENDED": 5, "THIN": 6, "FADING": 7, "NO DATA": 8}


def apply_filter(r):
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
    liquid_ok = (not tiny_float) and dollar_vol >= DOLLAR_VOL_MIN and (
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
                       "Under $3M traded" if dollar_vol < DOLLAR_VOL_MIN else "Thin / small")
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
SIGNAL_SETUPS = ("A-TRIGGER", "B-BREAKOUT")
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
    open_today = {(x["sym"], x["setup"]) for x in sigs if x["date"] == today and x["status"] in ("pending", "open")}
    for r in survivors:
        if r.get("setup") not in SIGNAL_SETUPS or not r.get("entry") or not r.get("stop"):
            continue
        if (r["sym"], r["setup"]) in open_today:
            continue
        if r["entry"] <= r["stop"]:
            continue
        sigs.append({
            "id": f"{today}-{r['sym']}-{r['setup']}-{now_ct:%H%M}",
            "date": today, "time_ct": now_ct.strftime("%H:%M"), "ts": int(time.time()),
            "sym": r["sym"], "setup": r["setup"],
            "entry": r["entry"], "stop": r["stop"], "t1": r.get("t1"), "t2": r.get("t2"),
            "price_at_signal": r.get("price"),
            "catalyst_type": r.get("catalyst_type"), "catalyst_strength": r.get("catalyst_strength"),
            "bad_headline": bool(r.get("bad_headline")), "rel_vol": r.get("rel_vol"),
            "vol_surge": r.get("vol_surge"), "float": r.get("float"),
            "prime": now_ct.time() < PRIME_END_CT,
            "status": "pending",
        })
        open_today.add((r["sym"], r["setup"]))


def _r_mult(sig, price):
    risk = sig["entry"] - sig["stop"]
    return round((price - sig["entry"]) / risk, 2) if risk > 0 else 0.0


def resolve_signal(sig, df):
    """Walk 5m bars after the signal. Conservative: if one bar touches both stop and
    target, the stop is assumed to have hit first."""
    sig_ts = dt.datetime.fromtimestamp(sig["ts"], ET)
    bars = df[df.index >= sig_ts - dt.timedelta(minutes=5)]
    if bars.empty:
        return
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
    sig["status"] = status
    sig["exit_time_ct"] = ts.astimezone(CT).strftime("%H:%M")
    sig["r_full"] = _r_mult(sig, full_exit)
    if half_done:
        sig["r_split"] = round(0.5 * _r_mult(sig, full_exit) + 0.5 * _r_mult(sig, rest_exit), 2)
    else:
        sig["r_split"] = sig["r_full"]
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
    done = [x for x in load_signals() if x["status"] in ("win", "loss", "time")]

    def agg(rows):
        if not rows:
            return {"n": 0}
        rf = [x["r_full"] for x in rows]
        rs = [x["r_split"] for x in rows]
        return {"n": len(rows), "win_rate": round(sum(1 for r in rf if r > 0) / len(rows), 3),
                "avg_r_full": round(sum(rf) / len(rows), 2), "avg_r_split": round(sum(rs) / len(rows), 2),
                "total_r_full": round(sum(rf), 2), "total_r_split": round(sum(rs), 2)}

    clean = lambda x: x.get("catalyst_strength") in ("strong", "squeeze") and not x.get("bad_headline")
    groups = {
        "all": done,
        "A-TRIGGER": [x for x in done if x["setup"] == "A-TRIGGER"],
        "B-BREAKOUT": [x for x in done if x["setup"] == "B-BREAKOUT"],
        "prime (before 10:30 CT)": [x for x in done if x.get("prime")],
        "after 10:30 CT": [x for x in done if not x.get("prime")],
        "clean catalyst": [x for x in done if clean(x)],
        "A-TRIGGER + prime + clean": [x for x in done if x["setup"] == "A-TRIGGER" and x.get("prime") and clean(x)],
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
        apply_filter(r)
    survivors = sorted([r for r in top if r["pass"]], key=score)
    rejected = sorted([r for r in top if not r["pass"]], key=lambda r: -r["chg_pct"])
    for r in survivors + rejected:
        for k in ("price", "prev_close", "chg_pct"):
            r[k] = fnum(r.get(k), 4 if k != "chg_pct" else 2)
    try:
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
    now = dt.datetime.now(dt.timezone.utc)
    payload = {
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
    publish(payload)
    log(f"loop {_state['loop']} {session}: universe={len(rows)} movers={len(movers)} "
        f"survivors={len(survivors)} in {time.time() - t0:.0f}s")
    return session


def main():
    start = time.time()
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
        if ONE_SHOT or time.time() >= deadline:
            break
        if session == "CLOSED" and dt.datetime.now(ET).hour >= 16:
            log("Market closed for the day; exiting.")
            break
        time.sleep(max(10, LOOP_SECONDS - (time.time() - t)))


if __name__ == "__main__":
    main()
