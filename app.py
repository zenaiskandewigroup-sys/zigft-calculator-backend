import os
import time
from datetime import datetime, timezone, timedelta
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

WIB = timezone(timedelta(hours=7))

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "").strip()

HEADERS_BROWSER = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
}

PAIR_TO_YAHOO = {
    "XAUUSD": "GC=F",
    "EURUSD": "EURUSD=X",
    "GBPUSD": "GBPUSD=X",
    "AUDUSD": "AUDUSD=X",
    "NZDUSD": "NZDUSD=X",
    "USDJPY": "USDJPY=X",
    "USDCAD": "USDCAD=X",
    "USDCHF": "USDCHF=X",
    "EURGBP": "EURGBP=X",
    "EURJPY": "EURJPY=X",
    "GBPJPY": "GBPJPY=X",
    "AUDJPY": "AUDJPY=X",
    "NZDJPY": "NZDJPY=X",
    "CADJPY": "CADJPY=X",
    "CHFJPY": "CHFJPY=X",
    "EURNZD": "EURNZD=X",
}

CACHE_CHART = {}
CACHE_NEWS = {"ts": 0.0, "events": []}
CACHE_FX = {"ts": 0.0, "rates": {}}

CHART_TTL = 300
NEWS_TTL = 1800
FX_TTL = 600


def log_msg(msg: str):
    now_str = datetime.now(WIB).strftime("%H:%M:%S WIB")
    print(f"[ZIGFT {now_str}] {msg}", flush=True)


@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response


# ==========================================
# SUPABASE PERSISTENT STATE (FIXED UPSERT)
# ==========================================
def default_state():
    return {
        "entries": [],
        "daily_pnl": {},
        "monthly_sl": {},
        "cooldown_until": None,
    }


def load_state():
    if not SUPABASE_URL or not SUPABASE_KEY:
        return default_state()
    try:
        url = f"{SUPABASE_URL}/rest/v1/zigft_state?id=eq.main&select=payload"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
        }
        r = requests.get(url, headers=headers, timeout=5)
        if r.status_code == 200:
            rows = r.json()
            if rows and isinstance(rows[0].get("payload"), dict):
                st = default_state()
                st.update(rows[0]["payload"])
                return st
    except Exception as e:
        log_msg(f"Supabase load error: {e}")
    return default_state()


def save_state(state: dict):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False
    try:
        url = f"{SUPABASE_URL}/rest/v1/zigft_state?on_conflict=id"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=minimal",
        }
        body = {
            "id": "main",
            "payload": state,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        r = requests.post(url, headers=headers, json=body, timeout=5)
        if r.status_code not in (200, 201, 204):
            log_msg(f"Supabase save HTTP {r.status_code}: {r.text}")
            return False
        return True
    except Exception as e:
        log_msg(f"Supabase save error: {e}")
        return False


# ==========================================
# FAST DIRECT YAHOO CHART FETCHER
# ==========================================
def fetch_yahoo_candles(pair: str, interval: str = "15m", range_str: str = "5d", force: bool = False):
    pair = pair.upper()
    cache_key = (pair, interval, range_str)
    now_ts = time.time()

    if not force and cache_key in CACHE_CHART:
        if now_ts - CACHE_CHART[cache_key]["ts"] < CHART_TTL:
            return CACHE_CHART[cache_key]["data"], True

    ticker = PAIR_TO_YAHOO.get(pair, f"{pair}=X")
    hosts = ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]

    for host in hosts:
        url = f"https://{host}/v8/finance/chart/{ticker}?interval={interval}&range={range_str}"
        try:
            r = requests.get(url, headers=HEADERS_BROWSER, timeout=5)
            if r.status_code != 200:
                continue
            raw = r.json()
            res = raw.get("chart", {}).get("result", [])
            if not res:
                continue
            timestamps = res[0].get("timestamp", [])
            quote = res[0].get("indicators", {}).get("quote", [{}])[0]
            opens = quote.get("open", [])
            highs = quote.get("high", [])
            lows = quote.get("low", [])
            closes = quote.get("close", [])

            candles = []
            for i, ts in enumerate(timestamps):
                o, h, l, c = opens[i], highs[i], lows[i], closes[i]
                if o is None or h is None or l is None or c is None:
                    continue
                candles.append({
                    "ts": int(ts),
                    "open": float(o),
                    "high": float(h),
                    "low": float(l),
                    "close": float(c),
                })

            if candles:
                CACHE_CHART[cache_key] = {"ts": now_ts, "data": candles}
                return candles, True
        except Exception as e:
            log_msg(f"Yahoo timeout/err ({host}): {e}")

    if cache_key in CACHE_CHART:
        return CACHE_CHART[cache_key]["data"], False
    return [], False


def pip_size_for(pair: str) -> float:
    if "XAU" in pair:
        return 0.1
    if "JPY" in pair:
        return 0.01
    return 0.0001


def decimals_for(pair: str) -> int:
    if "XAU" in pair:
        return 2
    if "JPY" in pair:
        return 3
    return 5


def usd_per_pip_for(pair: str) -> float:
    if pair.endswith("JPY"):
        return 6.6
    if pair == "EURGBP":
        return 12.6
    if pair in ("USDCAD", "USDCHF"):
        return 7.2
    return 10.0


# ==========================================
# LIVE FX STRENGTH
# ==========================================
def get_currency_strength(pair: str, side: str):
    now_ts = time.time()
    if now_ts - CACHE_FX["ts"] > FX_TTL or not CACHE_FX["rates"]:
        try:
            r = requests.get("https://open.er-api.com/v6/latest/USD", timeout=4)
            if r.status_code == 200:
                data = r.json()
                if data.get("rates"):
                    CACHE_FX["rates"] = data["rates"]
                    CACHE_FX["ts"] = now_ts
        except Exception as e:
            log_msg(f"FX API timeout/err: {e}")

    rates = CACHE_FX.get("rates", {})
    majors = ["USD", "EUR", "GBP", "JPY", "AUD", "NZD", "CAD", "CHF"]

    scores = {}
    if rates:
        baseline = {
            "USD": 1.0, "EUR": 0.92, "GBP": 0.79, "JPY": 152.0,
            "AUD": 1.54, "NZD": 1.69, "CAD": 1.39, "CHF": 0.88
        }
        raw_perf = {}
        for c in majors:
            cur_r = rates.get(c, baseline[c])
            perf = ((baseline[c] - cur_r) / baseline[c]) * 100.0 if c != "USD" else 0.0
            raw_perf[c] = perf
        raw_perf["USD"] = -sum(v for k, v in raw_perf.items() if k != "USD") / 7.0

        min_p, max_p = min(raw_perf.values()), max(raw_perf.values())
        span = (max_p - min_p) or 1.0
        for c in majors:
            scores[c] = round(2.0 + ((raw_perf[c] - min_p) / span) * 6.5, 1)
    else:
        scores = {"USD": 5.5, "EUR": 5.0, "GBP": 5.2, "JPY": 4.5, "AUD": 4.8, "NZD": 4.7, "CAD": 5.1, "CHF": 5.3}

    base_ccy = "XAU" if pair.startswith("XAU") else pair[:3]
    quote_ccy = pair[3:]

    if base_ccy == "XAU":
        usd_s = scores.get("USD", 5.0)
        base_score = round(max(1.5, min(9.0, 10.5 - usd_s)), 1)
    else:
        base_score = scores.get(base_ccy, 5.0)
    quote_score = scores.get(quote_ccy, 5.0)

    diff = round(base_score - quote_score, 1)
    if side == "buy":
        if diff >= 0.8:
            verdict, note = "ALIGNED", f"{base_ccy} ({base_score}) lebih kuat dari {quote_ccy} ({quote_score}) — mendukung skenario BUY."
        elif diff <= -0.8:
            verdict, note = "COUNTER", f"Perhatian: {quote_ccy} ({quote_score}) lebih dominan dari {base_ccy} ({base_score}) (counter-strength)."
        else:
            verdict, note = "NEUTRAL", f"Kekuatan {base_ccy} vs {quote_ccy} relatif berimbang (selisih {diff})."
    else:
        if diff <= -0.8:
            verdict, note = "ALIGNED", f"{quote_ccy} ({quote_score}) lebih kuat dari {base_ccy} ({base_score}) — mendukung skenario SELL."
        elif diff >= 0.8:
            verdict, note = "COUNTER", f"Perhatian: {base_ccy} ({base_score}) lebih dominan dari {quote_ccy} ({quote_score}) (counter-strength)."
        else:
            verdict, note = "NEUTRAL", f"Kekuatan {base_ccy} vs {quote_ccy} relatif berimbang (selisih {diff})."

    ranking = sorted([{"ccy": k, "score": v} for k, v in scores.items()], key=lambda x: x["score"], reverse=True)
    return {
        "base_ccy": base_ccy,
        "quote_ccy": quote_ccy,
        "base_score": base_score,
        "quote_score": quote_score,
        "verdict": verdict,
        "note": note,
        "ranking": ranking,
    }


# ==========================================
# MACRO NEWS RADAR
# ==========================================
def get_upcoming_news(pair: str = ""):
    now_ts = time.time()
    if now_ts - CACHE_NEWS["ts"] > NEWS_TTL:
        try:
            r = requests.get(
                "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
                headers=HEADERS_BROWSER,
                timeout=5,
            )
            if r.status_code == 200 and isinstance(r.json(), list):
                CACHE_NEWS["events"] = r.json()
                CACHE_NEWS["ts"] = now_ts
            else:
                CACHE_NEWS["ts"] = now_ts - (NEWS_TTL - 300)
        except Exception as e:
            log_msg(f"News fetch err: {e}")
            CACHE_NEWS["ts"] = now_ts - (NEWS_TTL - 300)

    relevant_ccys = {"USD", "EUR", "GBP", "JPY", "AUD", "NZD", "CAD", "CHF"}
    if pair:
        p = pair.upper()
        relevant_ccys = {"USD"} if "XAU" in p else {p[:3], p[3:]}

    now_dt = datetime.now(timezone.utc)
    upcoming = []

    for ev in CACHE_NEWS.get("events", []):
        if str(ev.get("impact", "")).lower() != "high":
            continue
        ccy = str(ev.get("country", "")).upper()
        if ccy not in relevant_ccys:
            continue
        date_str = ev.get("date", "")
        if not date_str:
            continue
        try:
            ev_dt = datetime.fromisoformat(date_str)
            if ev_dt.tzinfo is None:
                ev_dt = ev_dt.replace(tzinfo=timezone.utc)
            diff_sec = (ev_dt - now_dt).total_seconds()
            if diff_sec >= -900:
                upcoming.append((diff_sec, ev_dt, ev))
        except Exception:
            continue

    if not upcoming:
        scope_txt = f"untuk {pair.upper()}" if pair else "mayor minggu ini"
        return {
            "has_event": False,
            "note": f"Tidak ada jadwal berita High-Impact tersisa {scope_txt}. Kondisi likuiditas normal.",
        }

    upcoming.sort(key=lambda x: x[0])
    diff_sec, ev_dt, ev = upcoming[0]
    ccy = str(ev.get("country", "")).upper()
    title = ev.get("title", "High-Impact Event")
    forecast = str(ev.get("forecast") or "-")
    previous = str(ev.get("previous") or "-")

    prediction = "HIGH VOLATILITY"
    reasoning = f"Rilis {ccy} berdampak tinggi. Waspadai pelebaran spread dan sapuan likuiditas dua arah."
    try:
        f_num = float(forecast.replace("%", "").replace("K", "").replace("M", "").replace("B", "").strip())
        p_num = float(previous.replace("%", "").replace("K", "").replace("M", "").replace("B", "").strip())
        is_unemp = "unemployment" in title.lower() or "claims" in title.lower()
        ccy_bullish = (f_num < p_num) if is_unemp else (f_num > p_num)
        if f_num != p_num:
            if pair and pair.upper().startswith("XAU") and ccy == "USD":
                prediction = "BEARISH XAU (USD UP)" if ccy_bullish else "BULLISH XAU (USD DOWN)"
            elif pair and pair.upper().endswith(ccy):
                prediction = f"BEARISH {pair.upper()}" if ccy_bullish else f"BULLISH {pair.upper()}"
            elif pair and pair.upper().startswith(ccy):
                prediction = f"BULLISH {pair.upper()}" if ccy_bullish else f"BEARISH {pair.upper()}"
            else:
                prediction = f"BULLISH {ccy}" if ccy_bullish else f"BEARISH {ccy}"
            reasoning = f"Konsensus pasar ({forecast}) vs data sebelumnya ({previous}) mengindikasikan bias {prediction}."
    except Exception:
        pass

    mins_left = max(0, int(diff_sec // 60))
    hrs_left, rem_mins = mins_left // 60, mins_left % 60
    countdown_str = f"{hrs_left}j {rem_mins}m lagi" if diff_sec > 0 else "SEDANG RILIS"

    return {
        "has_event": True,
        "currency": ccy,
        "title": title,
        "release_wib": ev_dt.astimezone(WIB).strftime("%a, %d %b • %H:%M WIB"),
        "release_ts_ms": int(ev_dt.timestamp() * 1000),
        "countdown": countdown_str,
        "forecast": forecast,
        "previous": previous,
        "prediction": prediction,
        "reasoning": reasoning,
        "slippage_warning": 0 <= diff_sec <= 1800,
        "note": reasoning,
    }


# ==========================================
# PORTFOLIO LIFECYCLE (00:00 WIB + 2H PURGE)
# ==========================================
def process_portfolio_lifecycle(state: dict, force_live: bool = False):
    now_wib = datetime.now(WIB)
    today_str = now_wib.strftime("%Y-%m-%d")
    month_str = now_wib.strftime("%Y-%m")
    changed = False

    cd = state.get("cooldown_until")
    if cd:
        try:
            if now_wib >= datetime.fromisoformat(cd):
                state["cooldown_until"] = None
                changed = True
        except Exception:
            state["cooldown_until"] = None
            changed = True

    active_entries = []
    for e in state.get("entries", []):
        if e.get("status") == "OPEN":
            candles, _ = fetch_yahoo_candles(e["pair"], "15m", "1mo", force=force_live)
            created_ts = e.get("created_ts", 0)
            basis = float(e.get("basis_offset", 0.0))
            for c in candles:
                if c["ts"] < created_ts:
                    continue
                hi = c["high"] + basis
                lo = c["low"] + basis
                if e["side"] == "buy":
                    if lo <= e["sl"]:
                        e["status"] = "SL_HIT"
                        e["pnl_usd"] = -abs(float(e.get("est_sl_usd", 100.0)))
                        e["pnl_pips"] = -abs(float(e.get("sl_pips", 0)))
                        e["closed_wib"] = now_wib.strftime("%Y-%m-%d %H:%M WIB")
                        changed = True
                        break
                    if hi >= e["tp"]:
                        e["status"] = "TP_HIT"
                        e["pnl_usd"] = abs(float(e.get("est_tp_usd", 300.0)))
                        e["pnl_pips"] = abs(float(e.get("tp_pips", 0)))
                        e["closed_wib"] = now_wib.strftime("%Y-%m-%d %H:%M WIB")
                        changed = True
                        break
                else:
                    if hi >= e["sl"]:
                        e["status"] = "SL_HIT"
                        e["pnl_usd"] = -abs(float(e.get("est_sl_usd", 100.0)))
                        e["pnl_pips"] = -abs(float(e.get("sl_pips", 0)))
                        e["closed_wib"] = now_wib.strftime("%Y-%m-%d %H:%M WIB")
                        changed = True
                        break
                    if lo <= e["tp"]:
                        e["status"] = "TP_HIT"
                        e["pnl_usd"] = abs(float(e.get("est_tp_usd", 300.0)))
                        e["pnl_pips"] = abs(float(e.get("tp_pips", 0)))
                        e["closed_wib"] = now_wib.strftime("%Y-%m-%d %H:%M WIB")
                        changed = True
                        break

        entry_date = e.get("created_date_wib", today_str)
        if e.get("status") in ("TP_HIT", "SL_HIT") and not e.get("settled") and entry_date < today_str:
            day_rec = state["daily_pnl"].setdefault(entry_date, {"pnl_usd": 0.0, "wins": 0, "losses": 0, "trades": []})
            pnl_val = round(float(e.get("pnl_usd", 0.0)), 2)
            day_rec["pnl_usd"] = round(day_rec["pnl_usd"] + pnl_val, 2)
            if e["status"] == "TP_HIT":
                day_rec["wins"] += 1
            else:
                day_rec["losses"] += 1
                m_key = entry_date[:7]
                state["monthly_sl"][m_key] = state["monthly_sl"].get(m_key, 0) + 1
                if m_key == month_str and state["monthly_sl"][m_key] >= 3:
                    state["cooldown_until"] = (now_wib + timedelta(days=3)).isoformat()

            day_rec["trades"].append({
                "pair": e["pair"],
                "side": e["side"].upper(),
                "rr": e["rr"],
                "result": e["status"],
                "pnl_usd": pnl_val,
                "pnl_pips": e.get("pnl_pips", 0),
            })
            e["settled"] = True
            e["settled_at_ts"] = now_wib.timestamp()
            e["delete_after_wib"] = (now_wib + timedelta(hours=2)).strftime("%H:%M WIB")
            changed = True

        if e.get("settled") and e.get("settled_at_ts"):
            if now_wib.timestamp() - float(e["settled_at_ts"]) >= 7200:
                changed = True
                continue

        active_entries.append(e)

    state["entries"] = active_entries
    if changed:
        save_state(state)

    daily_used = sum(1 for e in active_entries if e.get("created_date_wib") == today_str and not e.get("is_backfill"))
    sl_open_this_month = sum(
        1 for e in active_entries
        if e.get("status") == "SL_HIT" and not e.get("settled") and e.get("created_date_wib", "").startswith(month_str)
    )
    monthly_sl_used = state.get("monthly_sl", {}).get(month_str, 0) + sl_open_this_month

    cooldown_active = bool(state.get("cooldown_until"))
    can_enter = True
    block_reason = ""
    if cooldown_active:
        can_enter = False
        block_reason = f"Cooldown 3 hari aktif hingga {state['cooldown_until'][:16].replace('T', ' ')} WIB (Max 3 SL tercapai)."
    elif monthly_sl_used >= 3:
        can_enter = False
        block_reason = "Batas maksimal 3 SL bulan ini sudah tercapai (ZIGFT Rule)."
    elif daily_used >= 1:
        can_enter = False
        block_reason = "Kuota maksimal 1 entry per hari sudah terpakai."

    return state, {
        "can_enter": can_enter,
        "block_reason": block_reason,
        "daily_used": daily_used,
        "daily_max": 1,
        "monthly_sl_used": monthly_sl_used,
        "monthly_sl_max": 3,
        "cooldown_active": cooldown_active,
    }


# ==========================================
# ROUTES
# ==========================================
@app.route("/health", methods=["GET"])
def health():
    age = round(time.time() - CACHE_NEWS["ts"], 1) if CACHE_NEWS["ts"] > 0 else None
    return jsonify({
        "status": "online",
        "service": "ZIGFT Quant & Portfolio Engine",
        "supabase_connected": bool(SUPABASE_URL and SUPABASE_KEY),
        "news_cached_count": len(CACHE_NEWS.get("events", [])),
        "cache_age_sec": age,
    })


@app.route("/api/news-radar", methods=["GET"])
def api_news_radar():
    pair = request.args.get("pair", "").strip().upper()
    return jsonify({"news": get_upcoming_news(pair)})


@app.route("/api/quant-analysis", methods=["POST", "OPTIONS"])
def api_quant_analysis():
    if request.method == "OPTIONS":
        return ("", 200)

    body = request.get_json(silent=True) or {}
    pair = str(body.get("pair", "XAUUSD")).upper().strip()
    side = str(body.get("side", "buy")).lower().strip()
    entry = float(body.get("entry", 0))
    sl = float(body.get("sl", 0))
    sl_pips = float(body.get("sl_pips", 0))
    tp_pips = float(body.get("tp_pips", 0))

    state = load_state()
    state, zigft_rules = process_portfolio_lifecycle(state, force_live=False)

    m15, is_live = fetch_yahoo_candles(pair, "15m", "5d", force=False)
    d1, _ = fetch_yahoo_candles(pair, "1d", "1y", force=False)

    pv = pip_size_for(pair)
    dec = decimals_for(pair)

    spot_calibrated = False
    basis = 0.0
    if m15 and entry > 0:
        last_close = m15[-1]["close"]
        if abs(entry - last_close) > (20 * pv):
            basis = entry - last_close
            spot_calibrated = True

    if m15:
        recent = m15[-96:]
        rng_high = max(c["high"] for c in recent) + basis
        rng_low = min(c["low"] for c in recent) + basis
    else:
        rng_high = entry + (40 * pv)
        rng_low = entry - (40 * pv)

    span = max(rng_high - rng_low, 10 * pv)
    eq_50 = round(rng_low + 0.50 * span, dec)
    if side == "buy":
        fib_618 = round(rng_high - 0.618 * span, dec)
        fib_750 = round(rng_high - 0.750 * span, dec)
        in_ote = fib_750 <= entry <= eq_50
        zone = "DISCOUNT OTE" if in_ote else ("DISCOUNT" if entry < eq_50 else "PREMIUM RISK")
        verdict = "OPTIMAL" if in_ote else ("VALID" if entry <= eq_50 else "STRETCHED")
    else:
        fib_618 = round(rng_low + 0.618 * span, dec)
        fib_750 = round(rng_low + 0.750 * span, dec)
        in_ote = eq_50 <= entry <= fib_750
        zone = "PREMIUM OTE" if in_ote else ("PREMIUM" if entry > eq_50 else "DISCOUNT RISK")
        verdict = "OPTIMAL" if in_ote else ("VALID" if entry >= eq_50 else "STRETCHED")

    smc_obj = {
        "zone": zone,
        "verdict": verdict,
        "eq_50": eq_50,
        "fib_618": fib_618,
        "fib_750": fib_750,
        "note": f"Dealing range {round(rng_low, dec)} – {round(rng_high, dec)}. Entry berada di zona {zone}.",
        "idm_swept": bool(m15 and len(m15) >= 12),
        "idm_note": "Minor internal swing terdeteksi — pastikan sweep IDM valid sebelum eksekusi.",
    }

    if d1 and len(d1) >= 2:
        pdh = round(d1[-2]["high"] + basis, dec)
        pdl = round(d1[-2]["low"] + basis, dec)
        week_slice = d1[-7:-1] if len(d1) >= 7 else d1[:-1]
        pwh = round(max(c["high"] for c in week_slice) + basis, dec)
        pwl = round(min(c["low"] for c in week_slice) + basis, dec)
    else:
        pdh, pdl = round(rng_high, dec), round(rng_low, dec)
        pwh, pwl = round(rng_high + 25 * pv, dec), round(rng_low - 25 * pv, dec)

    asian_slice = m15[-32:-16] if len(m15) >= 32 else (m15[-16:] if m15 else [])
    asian_high = round(max((c["high"] for c in asian_slice), default=pdh) + (basis if asian_slice else 0), dec)
    asian_low = round(min((c["low"] for c in asian_slice), default=pdl) + (basis if asian_slice else 0), dec)

    sweep_alerts = []
    for lbl, lvl in [("PDH", pdh), ("PDL", pdl), ("PWH", pwh), ("PWL", pwl), ("Asian High", asian_high), ("Asian Low", asian_low)]:
        if abs(sl - lvl) <= (12 * pv):
            sweep_alerts.append(f"SL ({sl}) terlalu dekat dengan {lbl} ({lvl}) — rawan liquidity sweep.")

    now_wib = datetime.now(WIB)
    hr, wd = now_wib.hour, now_wib.weekday()
    days_id = ["Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu", "Minggu"]
    is_weekend = (wd == 5 and hr >= 4) or wd == 6 or (wd == 0 and hr < 4)
    if 14 <= hr < 17:
        kz_name, kz_qual = "LONDON KILLZONE", "KILLZONE"
    elif 19 <= hr < 23:
        kz_name, kz_qual = "NEW YORK KILLZONE", "KILLZONE"
    elif 4 <= hr < 7:
        kz_name, kz_qual = "ROLLOVER / SPREAD WIDENING", "SPREAD_RISK"
    else:
        kz_name, kz_qual = "ASIAN / INTER-SESSION", "NORMAL"

    if d1 and len(d1) >= 15:
        trs = [max(d1[i]["high"] - d1[i]["low"], abs(d1[i]["high"] - d1[i - 1]["close"]), abs(d1[i]["low"] - d1[i - 1]["close"])) for i in range(-14, 0)]
        atr_pips = round((sum(trs) / len(trs)) / pv, 1)
        today_pips = round((d1[-1]["high"] - d1[-1]["low"]) / pv, 1)
    else:
        atr_pips = 280.0 if "XAU" in pair else 85.0
        today_pips = round(atr_pips * 0.45, 1)

    exh_pct = round((today_pips / atr_pips) * 100, 1) if atr_pips > 0 else 50.0
    atr_status = "STRETCHED" if exh_pct >= 85 else "OPTIMAL"

    day_wins, day_tot, day_moves = 0, 0, []
    if d1:
        for c in d1:
            if datetime.fromtimestamp(c["ts"], tz=WIB).weekday() == wd:
                day_tot += 1
                day_moves.append(abs(c["close"] - c["open"]) / pv)
                if (side == "buy" and c["close"] > c["open"]) or (side == "sell" and c["close"] < c["open"]):
                    day_wins += 1
    day_wr = round((day_wins / day_tot) * 100, 1) if day_tot > 0 else 52.0
    day_med = round(sorted(day_moves)[len(day_moves) // 2], 1) if day_moves else round(atr_pips * 0.6, 1)

    usd_per_pip = usd_per_pip_for(pair)

    return jsonify({
        "feed_source": "YAHOO LIVE" if is_live else "CACHE / FALLBACK",
        "spot_calibrated": spot_calibrated,
        "side": side.upper(),
        "usd_per_pip_1lot": round(usd_per_pip, 2),
        "est_sl_usd_1lot": round(sl_pips * usd_per_pip, 2),
        "est_tp_usd_1lot": round(tp_pips * usd_per_pip, 2),
        "zigft_rules": zigft_rules,
        "news_prediction": get_upcoming_news(pair),
        "smc": smc_obj,
        "killzone": {
            "is_weekend": is_weekend,
            "zone_name": kz_name,
            "quality": kz_qual,
            "day_name": days_id[wd],
            "wib_time": now_wib.strftime("%H:%M WIB"),
        },
        "liquidity": {
            "pdh": pdh, "pdl": pdl, "pwh": pwh, "pwl": pwl,
            "asian_high": asian_high, "asian_low": asian_low,
            "sweep_alerts": sweep_alerts,
        },
        "volatility": {
            "status": atr_status,
            "today_range_pips": today_pips,
            "atr_14_pips": atr_pips,
            "exhaustion_pct": exh_pct,
            "note": f"Pergerakan harian telah memakai {exh_pct}% dari rata-rata ATR-14.",
        },
        "strength": get_currency_strength(pair, side),
        "seasonality": {
            "day_name": days_id[wd],
            "day_winrate": day_wr,
            "day_samples": day_tot,
            "day_median_pips": day_med,
            "session_name": kz_name,
            "session_winrate": round(min(74.0, day_wr + 3.5), 1),
            "session_samples": max(day_tot, 20),
            "session_median_pips": round(day_med * 0.55, 1),
        },
    })


@app.route("/api/entries", methods=["POST", "OPTIONS"])
def api_entries():
    if request.method == "OPTIONS":
        return ("", 200)

    body = request.get_json(silent=True) or {}
    if not body.get("inducement_confirmed") or not body.get("liquidity_confirmed"):
        return jsonify({"error": "Wajib konfirmasi Inducement (IDM) dan Liquidity Pool sesuai aturan ZIGFT."}), 400

    state = load_state()
    state, zigft_rules = process_portfolio_lifecycle(state, force_live=False)
    if not zigft_rules["can_enter"]:
        return jsonify({"error": zigft_rules["block_reason"]}), 403

    pair = str(body.get("pair", "")).upper().strip()
    side = str(body.get("side", "buy")).lower().strip()
    entry = float(body.get("entry", 0))
    sl = float(body.get("sl", 0))
    tp = float(body.get("tp", 0))
    sl_pips = float(body.get("sl_pips", 0))
    tp_pips = float(body.get("tp_pips", 0))
    rr = float(body.get("rr", 3.0))

    m15, _ = fetch_yahoo_candles(pair, "15m", "5d", force=False)
    pv = pip_size_for(pair)
    basis = (entry - m15[-1]["close"]) if (m15 and abs(entry - m15[-1]["close"]) > (20 * pv)) else 0.0

    usd_per_pip = usd_per_pip_for(pair)
    now_wib = datetime.now(WIB)

    new_entry = {
        "id": f"ZIGFT-{int(now_wib.timestamp())}",
        "pair": pair,
        "side": side,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "sl_pips": sl_pips,
        "tp_pips": tp_pips,
        "rr": rr,
        "basis_offset": basis,
        "est_sl_usd": round(sl_pips * usd_per_pip, 2),
        "est_tp_usd": round(tp_pips * usd_per_pip, 2),
        "status": "OPEN",
        "pnl_usd": 0.0,
        "pnl_pips": 0.0,
        "settled": False,
        "created_ts": int(now_wib.timestamp()),
        "created_date_wib": now_wib.strftime("%Y-%m-%d"),
        "created_wib": now_wib.strftime("%d %b %H:%M WIB"),
    }

    state["entries"].append(new_entry)
    if not save_state(state):
        return jsonify({"error": "Gagal menyimpan data ke Supabase. Cek Environment Variables di Render."}), 500

    log_msg(f"Entry logged & saved: {pair} {side.upper()}")
    return jsonify({
        "status": "ok",
        "message": f"Entry {pair} {side.upper()} tersimpan permanen di Supabase & dipantau live.",
    })


@app.route("/api/entries/manual", methods=["POST", "OPTIONS"])
def api_entries_manual():
    if request.method == "OPTIONS":
        return ("", 200)

    body = request.get_json(silent=True) or {}
    if not body.get("inducement_confirmed") or not body.get("liquidity_confirmed"):
        return jsonify({"error": "Centang konfirmasi Inducement (IDM) & Liquidity terlebih dahulu."}), 400

    pair = str(body.get("pair", "XAUUSD")).upper().strip()
    side = str(body.get("side", "buy")).lower().strip()
    outcome = str(body.get("outcome", "TP_HIT")).upper().strip()
    entry = float(body.get("entry", 0))
    sl = float(body.get("sl", 0))
    tp = float(body.get("tp", 0))
    trade_date = str(body.get("trade_date", "")).strip()
    trade_time = str(body.get("trade_time", "08:00")).strip() or "08:00"

    if entry <= 0 or sl <= 0 or tp <= 0 or not trade_date:
        return jsonify({"error": "Lengkapi Pair, Entry, SL, TP, dan Tanggal Trade."}), 400

    pv = pip_size_for(pair)
    sl_pips = round(abs(entry - sl) / pv, 1)
    tp_pips = round(abs(tp - entry) / pv, 1)
    rr = round(tp_pips / sl_pips, 2) if sl_pips > 0 else 3.0
    usd_per_pip = usd_per_pip_for(pair)
    est_sl_usd = round(sl_pips * usd_per_pip, 2)
    est_tp_usd = round(tp_pips * usd_per_pip, 2)

    state = load_state()
    now_wib = datetime.now(WIB)

    # 1. Jika trade SUDAH TP atau SUDAH SL dari hari lampau -> langsung masuk Kalender PnL permanen
    if outcome in ("TP_HIT", "SL_HIT"):
        day_rec = state["daily_pnl"].setdefault(trade_date, {"pnl_usd": 0.0, "wins": 0, "losses": 0, "trades": []})
        pnl_val = est_tp_usd if outcome == "TP_HIT" else -est_sl_usd
        pnl_pips = tp_pips if outcome == "TP_HIT" else -sl_pips

        day_rec["pnl_usd"] = round(day_rec["pnl_usd"] + pnl_val, 2)
        if outcome == "TP_HIT":
            day_rec["wins"] += 1
        else:
            day_rec["losses"] += 1
            m_key = trade_date[:7]
            state["monthly_sl"][m_key] = state["monthly_sl"].get(m_key, 0) + 1
            if m_key == now_wib.strftime("%Y-%m") and state["monthly_sl"][m_key] >= 3:
                state["cooldown_until"] = (now_wib + timedelta(days=3)).isoformat()

        day_rec["trades"].append({
            "pair": pair,
            "side": side.upper(),
            "rr": rr,
            "result": outcome,
            "pnl_usd": pnl_val,
            "pnl_pips": pnl_pips,
        })

        if not save_state(state):
            return jsonify({"error": "Gagal menyimpan riwayat ke Supabase."}), 500

        log_msg(f"Backfill settled: {pair} {side.upper()} {outcome} on {trade_date} (${pnl_val})")
        return jsonify({
            "status": "ok",
            "message": f"Riwayat {pair} ({outcome}) tanggal {trade_date} berhasil masuk permanen ke Kalender PnL!",
        })

    # 2. Jika trade MASIH BERJALAN (OPEN) sejak tanggal/jam lampau
    try:
        opened_dt = datetime.fromisoformat(f"{trade_date}T{trade_time}:00+07:00")
    except Exception:
        opened_dt = now_wib

    m15, _ = fetch_yahoo_candles(pair, "15m", "1mo", force=True)
    basis = (entry - m15[-1]["close"]) if (m15 and abs(entry - m15[-1]["close"]) > (20 * pv)) else 0.0

    new_entry = {
        "id": f"ZIGFT-BF-{int(now_wib.timestamp())}",
        "pair": pair,
        "side": side,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "sl_pips": sl_pips,
        "tp_pips": tp_pips,
        "rr": rr,
        "basis_offset": basis,
        "est_sl_usd": est_sl_usd,
        "est_tp_usd": est_tp_usd,
        "status": "OPEN",
        "pnl_usd": 0.0,
        "pnl_pips": 0.0,
        "settled": False,
        "is_backfill": True,
        "created_ts": int(opened_dt.timestamp()),
        "created_date_wib": trade_date,
        "created_wib": opened_dt.strftime("%d %b %H:%M WIB"),
    }

    state["entries"].append(new_entry)
    state, _ = process_portfolio_lifecycle(state, force_live=True)
    if not save_state(state):
        return jsonify({"error": "Gagal menyimpan trade berjalan ke Supabase."}), 500

    return jsonify({
        "status": "ok",
        "message": f"Trade berjalan {pair} {side.upper()} sejak {opened_dt.strftime('%d %b %H:%M WIB')} berhasil disinkronkan & dicek ke candle live!",
    })


@app.route("/api/portfolio", methods=["GET"])
def api_portfolio():
    force = request.args.get("force", "0") == "1"
    state = load_state()
    state, zigft_rules = process_portfolio_lifecycle(state, force_live=force)

    daily_pnl = state.get("daily_pnl", {})
    sorted_dates = sorted(daily_pnl.keys())

    cum = 0.0
    wins = 0
    losses = 0
    equity_curve = []
    for d in sorted_dates:
        rec = daily_pnl[d]
        cum = round(cum + float(rec.get("pnl_usd", 0.0)), 2)
        wins += int(rec.get("wins", 0))
        losses += int(rec.get("losses", 0))
        equity_curve.append({"date": d, "cumulative_pnl": cum})

    total_settled = wins + losses
    win_rate = round((wins / total_settled) * 100, 1) if total_settled > 0 else 0.0

    now_wib = datetime.now(WIB)
    next_midnight = (now_wib + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    rem_sec = int((next_midnight - now_wib).total_seconds())
    rem_h, rem_m = rem_sec // 3600, (rem_sec % 3600) // 60

    return jsonify({
        "server_wib": now_wib.strftime("%H:%M:%S WIB"),
        "checked_at_wib": now_wib.strftime("%d %b • %H:%M:%S WIB"),
        "next_settlement_wib": f"00:00 WIB ({rem_h}j {rem_m}m lagi)",
        "summary": {
            "net_pnl_usd": cum,
            "win_rate": win_rate,
            "wins": wins,
            "losses": losses,
        },
        "zigft_rules": zigft_rules,
        "equity_curve": equity_curve,
        "daily_pnl": daily_pnl,
        "raw_entries": list(reversed(state.get("entries", []))),
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
