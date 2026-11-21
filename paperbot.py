"""Paper trading bot: trades the scanner's signals on paper with the rules Nathan approved Nov 20.

PAPER ONLY. It never places, previews or cancels an order and never calls any broker endpoint;
it reads prices handed to it by the scanner (Public quotes, Yahoo 1-minute bars) and simulates
fills. The workflow's guard step fails the run if this file ever references an order endpoint.

Rules (Nov 13-20 replay, "auto1_oneloss": +$397 over 6 sessions):
  takes   NOW signals, plus armed BREAKs with strong/analyst news, 08:30-10:30 CT
  entry   one scanner loop after the signal, at the Public ask + 0.05%; skip if > entry + 0.3R or <= stop
  size    $200 risk on a stop of at least 0.4%, never more than $15k
  exits   card stop; tp_pct target; at the target, hold as a runner if price is at the high of day
          above VWAP (lock +half the target, trail the 15-minute low, sell on a 5-min close under
          VWAP); otherwise sell at the target; 20-minute clock; anything open at 14:55 CT is sold
  limits  3 open at a time, one loss per name per day, done for the day at +$1,200 or -$400
"""
import datetime as dt
import json
import os

from zoneinfo import ZoneInfo

CT = ZoneInfo("America/Chicago")
RISK, CAP, MIN_STOP, SLIP = 200.0, 15000.0, 0.004, 0.0005
WINDOW = (dt.time(8, 30), dt.time(10, 30))
HOLD_MIN, MAX_OPEN, WIN_DONE, LOSS_DONE = 20, 3, 1200.0, -400.0
FLAT_BY = dt.time(14, 55)
CHASE_R = 0.3
TP_DEFAULT = 1.5


def _t(day, hhmm):
    return dt.datetime.fromisoformat(f"{day}T{hhmm}").replace(tzinfo=CT)


def takes(x):
    t = x.get("time_ct") or ""
    if not (WINDOW[0].strftime("%H:%M") <= t < WINDOW[1].strftime("%H:%M")) or x.get("past_window"):
        return False
    if x.get("pinned"):
        return False
    return bool(x.get("now_ok")) or (x.get("setup") == "BREAK" and x.get("catalyst_strength") in ("strong", "analyst"))


class PaperBot:
    def __init__(self, state=None):
        self.s = state or {}

    # ---- state ---------------------------------------------------------------
    def _reset(self, day):
        hist = self.s.get("history", [])
        if self.s.get("date") and self.s.get("closed"):
            c = self.s["closed"]
            hist = ([{"date": self.s["date"], "pnl": round(sum(x["pnl"] for x in c), 2), "trades": len(c),
                      "wins": sum(1 for x in c if x["pnl"] > 0)}] + hist)[:60]
        self.s = {"date": day, "open": [], "closed": [], "pending": [], "skipped": [], "seen": [],
                  "history": hist, "lost": []}

    def realized(self):
        return sum(x["pnl"] for x in self.s.get("closed", []))

    # ---- one step --------------------------------------------------------------
    def step(self, now, signals_today, quotes, bars):
        """now: aware CT datetime. quotes: {sym: {"p", "bid", "ask"}} (may be empty).
        bars: {sym: DataFrame of 1-minute bars, CT index, columns Open High Low Close Volume}."""
        day = now.date().isoformat()
        if self.s.get("date") != day:
            self._reset(day)
        s = self.s
        # 1. manage open positions bar by bar, then on the live quote
        for p in list(s["open"]):
            self._manage(p, now, quotes.get(p["sym"]), bars.get(p["sym"]))
        # 2. fill what was queued last step
        for q in list(s["pending"]):
            s["pending"].remove(q)
            self._fill(q, now, quotes.get(q["sym"]), bars.get(q["sym"]))
        # 3. queue new signals for the next step (one loop of delay, like a bot that has to see it first)
        seen = set(s["seen"])
        for x in sorted(signals_today, key=lambda x: x.get("ts") or 0):
            if x.get("id") in seen or x.get("date") != day:
                continue
            seen.add(x.get("id"))
            if not takes(x):
                continue
            why = self._gate(x)
            if why:
                s["skipped"].append({"t": x["time_ct"], "sym": x["sym"], "setup": x["setup"], "why": why})
                continue
            s["pending"].append({k: x.get(k) for k in ("id", "sym", "setup", "entry", "stop", "tp_pct", "time_ct", "ts")})
        s["seen"] = sorted(seen)
        # model slippage: live fill vs the 1-minute bar open the replay would have used
        for x in s["open"] + s["closed"]:
            if x.get("model_in") is None:
                df = bars.get(x["sym"])
                m = self._bar_at(df, x["t_in"])
                if m is not None:
                    x["model_in"] = round(m, 4)
                    x["slip_bps"] = round((x["fill"] / m - 1) * 1e4, 1)
        return self.summary(now)

    def _gate(self, x):
        s = self.s
        r = self.realized()
        if r >= WIN_DONE or r <= LOSS_DONE:
            return "Rule 9: done for the day"
        if x["sym"] in s["lost"]:
            return "one loss per name"
        if any(p["sym"] == x["sym"] for p in s["open"]) or any(q["sym"] == x["sym"] for q in s["pending"]):
            return "already in it"
        if len(s["open"]) + len(s["pending"]) >= MAX_OPEN:
            return "3 positions open"
        return None

    @staticmethod
    def _bar_at(df, iso):
        if df is None or not len(df):
            return None
        t = dt.datetime.fromisoformat(iso).replace(second=0, microsecond=0)
        b = df[df.index >= t]
        return float(b["Open"].iloc[0]) if len(b) else None

    def _fill(self, q, now, quote, df):
        s = self.s
        e, st = float(q["entry"]), float(q["stop"])
        R = e - st
        px = None
        if quote:
            px = quote.get("ask") or quote.get("p")
        src = "public"
        if not px and df is not None and len(df):
            px, src = float(df["Close"].iloc[-1]), "yahoo"
        if not px or R <= 0:
            s["skipped"].append({"t": q["time_ct"], "sym": q["sym"], "setup": q["setup"], "why": "no price to fill"})
            return
        fill = float(px) * (1 + SLIP)
        if fill > e + CHASE_R * R:
            s["skipped"].append({"t": q["time_ct"], "sym": q["sym"], "setup": q["setup"],
                                 "why": f"ran {(fill - e) / R:.1f}R before the fill"})
            return
        if fill <= st:
            s["skipped"].append({"t": q["time_ct"], "sym": q["sym"], "setup": q["setup"], "why": "under the stop at fill"})
            return
        if now.time() >= FLAT_BY:
            return
        sh = int(min(RISK / max(fill - st, fill * MIN_STOP), CAP / fill))
        if sh <= 0:
            return
        tp = float(q.get("tp_pct") or TP_DEFAULT)
        s["open"].append({"sym": q["sym"], "setup": q["setup"], "t_sig": q["time_ct"], "t_in": now.isoformat(timespec="seconds"),
                          "fill": round(fill, 4), "src": src, "sh": sh, "entry": e, "stop": st, "tp_pct": tp,
                          "target": round(fill * (1 + tp / 100), 4), "run": False, "lock": None,
                          "last_bar": now.replace(second=0, microsecond=0).isoformat(), "model_in": None})

    def _close(self, p, now, px, why, quote=None):
        s = self.s
        out = float(px) * (1 - SLIP)
        pnl = (out - p["fill"]) * p["sh"]
        c = dict(p, t_out=now.isoformat(timespec="seconds"), out=round(out, 4), why=why, pnl=round(pnl, 2))
        if quote and quote.get("bid"):
            c["bid_at_exit"] = quote["bid"]
        s["open"].remove(p)
        s["closed"].append(c)
        if pnl < 0 and p["sym"] not in s["lost"]:
            s["lost"].append(p["sym"])

    def _manage(self, p, now, quote, df):
        if now.time() >= FLAT_BY:
            px = (quote or {}).get("bid") or (quote or {}).get("p") or (float(df["Close"].iloc[-1]) if df is not None and len(df) else p["fill"])
            return self._close(p, now, px, "flat by 14:55", quote)
        t_in = dt.datetime.fromisoformat(p["t_in"])
        if df is not None and len(df):
            vw = ((df["High"] + df["Low"] + df["Close"]) / 3 * df["Volume"]).cumsum() / df["Volume"].cumsum().replace(0, float("nan"))
            hod = df["High"].cummax()
            last = dt.datetime.fromisoformat(p["last_bar"])
            # completed 1-minute bars since the last check (the forming minute is left for next time)
            done = df[(df.index >= last) & (df.index < now.replace(second=0, microsecond=0))]
            for ts, r in done.iterrows():
                p["last_bar"] = (ts + dt.timedelta(minutes=1)).isoformat()
                if self._bar(p, ts, r, float(vw.loc[ts]) if vw.loc[ts] == vw.loc[ts] else None, float(hod.loc[ts]), df, t_in, quote):
                    return
        # live quote between bars: stop and lock checks only (targets need the bar for the HOD/VWAP call)
        if quote and quote.get("p"):
            px = float(quote["p"])
            if not p["run"] and px <= p["stop"]:
                return self._close(p, now, quote.get("bid") or px, "stop (quote)", quote)
            if p["run"] and p["lock"] and px <= p["lock"]:
                return self._close(p, now, quote.get("bid") or px, "runner lock (quote)", quote)

    def _bar(self, p, ts, r, vwap, hod, df, t_in, quote):
        """Apply one completed 1-minute bar. Returns True if the position closed."""
        now = ts + dt.timedelta(minutes=1)
        if not p["run"]:
            if r["Low"] <= p["stop"]:
                self._close(p, now, p["stop"] - 0.01, "stop", quote); return True
            if r["High"] >= p["target"]:
                if vwap and r["High"] >= hod - 1e-9 and r["Close"] > vwap:
                    p["run"] = True
                    p["lock"] = round(p["fill"] * (1 + p["tp_pct"] / 200), 4)
                    return False
                self._close(p, now, p["target"], "target", quote); return True
            if (now - t_in).total_seconds() >= HOLD_MIN * 60:
                self._close(p, now, float(r["Close"]), "20-min clock", quote); return True
            return False
        if r["Low"] <= p["lock"]:
            self._close(p, now, p["lock"], "runner lock", quote); return True
        p["lock"] = round(max(p["lock"], float(df["Low"].loc[:ts].iloc[-15:].min())), 4)
        if ts.minute % 5 == 4 and vwap and r["Close"] < vwap:
            self._close(p, now, float(r["Close"]), "runner: 5-min close under VWAP", quote); return True
        return False

    # ---- output ----------------------------------------------------------------
    def summary(self, now=None):
        s = self.s
        c = s.get("closed", [])
        slips = [x["slip_bps"] for x in c + s.get("open", []) if x.get("slip_bps") is not None]
        return {
            "date": s.get("date"), "updated_ct": (now or dt.datetime.now(CT)).strftime("%H:%M:%S"),
            "paper_only": True,
            "realized": round(self.realized(), 2), "trades": len(c),
            "wins": sum(1 for x in c if x["pnl"] > 0), "losses": sum(1 for x in c if x["pnl"] <= 0),
            "done_for_day": self.realized() >= WIN_DONE or self.realized() <= LOSS_DONE,
            "open": s.get("open", []), "closed": c, "pending": s.get("pending", []),
            "skipped": s.get("skipped", [])[-30:],
            "entry_slip_bps_vs_model": round(sum(slips) / len(slips), 1) if slips else None,
            "history": s.get("history", []),
            "rules": "NOW + strong/analyst armed breaks 08:30-10:30 CT · $200 risk, 0.4% stop floor, $15k cap · "
                     "card stop, ATR target or runner (HOD above VWAP), 20-min clock · 3 open, one loss per name, +$1,200/-$400",
        }

    def save(self, path):
        with open(path, "w") as f:
            json.dump(self.s, f, separators=(",", ":"), default=str)


def load(path=None, repo=None, token=None, requests=None):
    """Restore today's state: the local file if present, else paper.json on the scan-data branch."""
    if path and os.path.exists(path):
        try:
            with open(path) as f:
                return PaperBot(json.load(f))
        except Exception:
            pass
    if repo and requests:
        try:
            h = {"Accept": "application/vnd.github.raw+json"}
            if token:
                h["Authorization"] = f"Bearer {token}"
            r = requests.get(f"https://api.github.com/repos/{repo}/contents/paper.json?ref=scan-data", headers=h, timeout=20)
            if r.status_code == 200:
                return PaperBot(r.json())
        except Exception:
            pass
    return PaperBot()
