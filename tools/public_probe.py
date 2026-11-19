"""One-off probe of the Public individual API for the scanner (run in Actions only).
Prints timings and data shape. Never prints the secret, the token or account IDs.
Read-only: calls the token, account-list, quotes and historic-bars endpoints only."""
import os, time, json, statistics, urllib.request, urllib.error, datetime as dt

BASE = "https://api.public.com"
SECRET = os.environ.get("PUBLIC_API_KEY", "").strip()


def req(method, path, body=None, token=None, timeout=20):
    h = {"Content-Type": "application/json", "User-Agent": "stock-prices-probe"}
    if token:
        h["Authorization"] = "Bearer " + token
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(BASE + path, data=data, headers=h, method=method)
    t = time.time()
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read() or b"{}"), time.time() - t
    except urllib.error.HTTPError as e:
        return e.code, {"error": e.read()[:300].decode("utf-8", "replace")}, time.time() - t


def main():
    if not SECRET:
        print("PUBLIC_API_KEY secret is empty or missing"); return
    st, js, lat = req("POST", "/userapiauthservice/personal/access-tokens", {"secret": SECRET, "validityInMinutes": 15})
    print(f"token: HTTP {st} in {lat*1000:.0f} ms")
    if st != 200:
        print("token error:", str(js)[:200].replace(SECRET, "***")); return
    tok = js["accessToken"]

    st, js, lat = req("GET", "/userapigateway/trading/account", token=tok)
    accts = js.get("accounts", []) if st == 200 else []
    print(f"accounts: HTTP {st}, {len(accts)} account(s), types {[a.get('accountType') for a in accts]}")
    if not accts:
        print(str(js)[:200]); return
    acct = next((a["accountId"] for a in accts if a.get("accountType") in ("BROKERAGE", "MARGIN")), accts[0]["accountId"])

    syms = ["SPY", "AAPL", "NVDA", "TSLA", "AMD", "PLTR", "SOFI", "RIOT", "ASTS", "MAGN"]
    body = {"instruments": [{"symbol": s, "type": "EQUITY"} for s in syms]}
    lats = []
    for i in range(10):
        st, js, lat = req("POST", f"/userapigateway/marketdata/{acct}/quotes", body, tok)
        lats.append(lat)
        if st != 200:
            print(f"quotes call {i}: HTTP {st} {str(js)[:150]}"); break
    print(f"quotes x10 (10 symbols each): median {statistics.median(lats)*1000:.0f} ms, max {max(lats)*1000:.0f} ms")
    now = dt.datetime.now(dt.timezone.utc)
    if st == 200:
        for q in js.get("quotes", [])[:10]:
            ts = q.get("lastTimestamp")
            age = None
            if ts:
                try:
                    age = (now - dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))).total_seconds()
                except Exception:
                    age = ts
            print(f"  {q['instrument']['symbol']:5s} last {q.get('last')} bid {q.get('bid')} ask {q.get('ask')} vol {q.get('volume')} age_s {age if not isinstance(age, float) else round(age, 1)} outcome {q.get('outcome')}")

    big = {"instruments": [{"symbol": s, "type": "EQUITY"} for s in (syms * 10)[:100]]}
    st, js, lat = req("POST", f"/userapigateway/marketdata/{acct}/quotes", big, tok)
    print(f"quotes 100 instruments: HTTP {st} in {lat*1000:.0f} ms, returned {len(js.get('quotes', [])) if st == 200 else str(js)[:120]}")

    burst = []
    t0 = time.time()
    for i in range(30):
        st, js, lat = req("POST", f"/userapigateway/marketdata/{acct}/quotes", body, tok)
        burst.append(st)
    print(f"burst 30 calls in {time.time()-t0:.1f}s: statuses {sorted(set(burst))} (429 = rate limited: {burst.count(429)})")

    for sym in ("AAPL", "ASTS"):
        st, js, lat = req("GET", f"/userapigateway/historicdata/EQUITY/{sym}/DAY?tradingSessionToggle=REGULAR_AND_EXTENDED_HOURS", token=tok)
        print(f"bars {sym} DAY: HTTP {st} in {lat*1000:.0f} ms")
        if st != 200:
            print("  ", str(js)[:200]); continue
        keys = [k for k in js.keys()]
        print("   top-level keys:", keys[:12])
        def walk(o):
            if isinstance(o, dict):
                for k, v in o.items():
                    if isinstance(v, dict) and "bars" in v:
                        yield k, v
                    else:
                        yield from walk(v)
            elif isinstance(o, list):
                for v in o:
                    yield from walk(v)
        for name, sess in walk(js):
            bars = sess.get("bars") or []
            if not bars:
                print(f"   {name}: 0 bars (expected {sess.get('expectedBars')})"); continue
            ts = [b.get("timestamp") for b in bars]
            try:
                tt = [dt.datetime.fromisoformat(str(x).replace("Z", "+00:00")) for x in ts]
                step = statistics.median([(b - a).total_seconds() for a, b in zip(tt, tt[1:])]) if len(tt) > 1 else None
                age = (now - tt[-1]).total_seconds()
            except Exception:
                step, age = None, None
            vols = [float(b.get("volume") or 0) for b in bars]
            print(f"   {name}: {len(bars)} bars, step {step}s, last bar age {age and round(age)}s, nonzero-volume bars {sum(v > 0 for v in vols)}/{len(vols)}, sample {json.dumps(bars[-1])[:200]}")
    print("done")


if __name__ == "__main__":
    main()
