"""Real-time quotes from the Public individual API, for the scanner's trigger checks.

READ-ONLY BY DESIGN. The key inherits Nathan's account permissions (it could trade), so this
module calls exactly three endpoints: create token, list accounts, get quotes. The workflow
fails the run if scan.py or this file ever references an order endpoint.
Never log the secret, the token or account IDs."""
import os, time, threading, datetime as dt
import requests

BASE = "https://api.public.com"
_SECRET = os.environ.get("PUBLIC_API_KEY", "").strip()
_TOKEN_MIN = 60
_lock = threading.Lock()
_tok = {"v": None, "exp": 0.0}
_acct = {"id": None}
_fail = {"n": 0, "until": 0.0, "last": ""}


def enabled():
    return bool(_SECRET) and time.time() >= _fail["until"]


def status():
    return {"enabled": bool(_SECRET), "backoff_until": _fail["until"], "last_error": _fail["last"], **_qstat}


def _err(msg):
    _fail["n"] += 1
    _fail["last"] = msg[:160]
    if _fail["n"] >= 3:                      # three failures in a row: back off 5 minutes, Yahoo covers
        _fail["until"] = time.time() + 300
        _fail["n"] = 0


def _token():
    with _lock:
        if _tok["v"] and time.time() < _tok["exp"] - 120:
            return _tok["v"]
        r = requests.post(BASE + "/userapiauthservice/personal/access-tokens",
                          json={"secret": _SECRET, "validityInMinutes": _TOKEN_MIN}, timeout=10)
        if r.status_code != 200:
            raise RuntimeError(f"token HTTP {r.status_code}")
        _tok["v"], _tok["exp"] = r.json()["accessToken"], time.time() + _TOKEN_MIN * 60
        return _tok["v"]


def _account(tok):
    if _acct["id"]:
        return _acct["id"]
    r = requests.get(BASE + "/userapigateway/trading/account", headers={"Authorization": "Bearer " + tok}, timeout=10)
    if r.status_code != 200:
        raise RuntimeError(f"accounts HTTP {r.status_code}")
    accts = r.json().get("accounts", [])
    pick = next((a for a in accts if a.get("accountType") == "BROKERAGE"), accts[0] if accts else None)
    if not pick:
        raise RuntimeError("no accounts")
    _acct["id"] = pick["accountId"]
    return _acct["id"]


_qstat = {"nofill": 0, "stale": 0}


def quotes(symbols, max_age_s=600):
    """{sym: {"p": last, "bid", "ask", "vol": day volume, "age": seconds}}.
    Nov 19: a 30 s cutoff dropped 23 of 52 thin names to Yahoo; their last trade is still the
    current price, so keep it up to 10 min old and report the age.
    Returns {} on any failure; callers fall back to Yahoo."""
    syms = sorted({s for s in symbols if s})
    if not syms or not enabled():
        return {}
    out = {}
    _qstat.update(nofill=0, stale=0)
    try:
        tok = _token()
        acct = _account(tok)
        now = dt.datetime.now(dt.timezone.utc)
        for i in range(0, len(syms), 100):
            body = {"instruments": [{"symbol": s, "type": "EQUITY"} for s in syms[i:i + 100]]}
            r = requests.post(f"{BASE}/userapigateway/marketdata/{acct}/quotes", json=body,
                              headers={"Authorization": "Bearer " + tok}, timeout=10)
            if r.status_code == 401:
                _tok["v"] = None
                raise RuntimeError("quotes HTTP 401")
            if r.status_code != 200:
                raise RuntimeError(f"quotes HTTP {r.status_code}")
            for q in r.json().get("quotes", []):
                if q.get("outcome") != "SUCCESS" or q.get("last") in (None, ""):
                    _qstat["nofill"] += 1
                    continue
                age = None
                ts = q.get("lastTimestamp")
                if ts:
                    try:
                        age = (now - dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))).total_seconds()
                    except Exception:
                        age = None
                if age is not None and age > max_age_s:
                    _qstat["stale"] += 1
                    continue
                f = lambda k: float(q[k]) if q.get(k) not in (None, "") else None
                out[q["instrument"]["symbol"]] = {"p": f("last"), "bid": f("bid"), "ask": f("ask"),
                                                  "vol": f("volume"), "age": age}
        _fail["n"] = 0
    except Exception as e:
        _err(f"{type(e).__name__}: {e}")
        return {}
    return out
