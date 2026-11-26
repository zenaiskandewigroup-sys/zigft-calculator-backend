import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

WIB = timezone(timedelta(hours=7))

RAW_SB_URL = os.environ.get("SUPABASE_URL", "").strip().strip('"').strip("'").rstrip("/")
if RAW_SB_URL and not RAW_SB_URL.startswith("http"):
    RAW_SB_URL = f"https://{RAW_SB_URL}"

SUPABASE_URL = RAW_SB_URL
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "").strip().strip('"').strip("'")

HEADERS_BROWSER = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
}

# Hanya instrumen komoditas futures yang memakai simbol futures Yahoo Finance
FUTURES_TICKERS = {
    "XAUUSD": "GC=F",
    "XAGUSD": "SI=F",
}

CACHE_CHART = {}
CACHE_NEWS = {"ts": 0.0, "events": []}
CACHE_FX = {"ts": 0.0, "rates": {}}
CACHE_STRENGTH = {"ts": 0.0, "scores": {}}

CHART_TTL = 300
NEWS_TTL = 1800
FX_TTL = 600
STRENGTH_TTL = 600


def log_msg(msg: str):
    now_str = datetime.now(WIB).strftime("%H:%M:%S WIB")
    print(f"[ZIGFT {now_str}] {msg}", flush=True)


@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PATCH, DELETE, OPTIONS"
    return response


# ==========================================
# SUPABASE PERSISTENT STATE (DUAL KEY SUPPORT)
# ==========================================
def supabase_headers():
    headers = {
        "apikey": SUPABASE_KEY,
        "Content-Type": "application/json",
    }
    if SUPABASE_KEY.startswith("eyJ"):
        headers["Authorization"] = f"Bearer {SUPABASE_KEY}"
    return headers


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
        r = requests.get(url, headers=supabase_headers(), timeout=8)
        if r.status_code == 200:
            rows = r.json()
            if rows and isinstance(rows[0].get("payload"), dict):
                st = default_state()
                st.update(rows[0]["payload"])
                return st
        else:
            log_msg(f"Supabase load HTTP {r.status_code}: {r.text[:160]}")
    except Exception as e:
        log_msg(f"Supabase load error: {e}")
    return default_state()


def save_state(state: dict):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False, "ENV SUPABASE_URL atau SUPABASE_KEY di Render belum terbaca (kosong)."
    try:
        url = f"{SUPABASE_URL}/rest/v1/zigft_state?on_conflict=id"
        headers = supabase_headers()
        headers["Prefer"] = "resolution=merge-duplicates,return=minimal"
        body = {
            "id": "main",
            "payload": state,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        r = requests.post(url, headers=headers, json=body, timeout=8)
        if r.status_code not in (200, 201, 204):
            err_txt = r.text[:180]
            log_msg(f"Supabase save HTTP {r.status_code}: {err_txt}")
            return False, f"Supabase HTTP {r.status_code}: {err_txt}"
        return True, ""
    except Exception as e:
        log_msg(f"Supabase save error: {e}")
        return False, f"Koneksi Supabase gagal: {e}"


# ==========================================
# DIRECT YAHOO CHART FETCHER
# ==========================================
def yahoo_ticker_for(pair: str) -> str:
    p = pair.upper().strip()
    return FUTURES_TICKERS.get(p, f"{p}=X")


def fetch_yahoo_candles(pair: str, interval: str = "15m", range_str: str = "5d", force: bool = False):
    pair = pair.upper().strip()
    cache_key = (pair, interval, range_str)
    now_ts = time.time()

    if not force and cache_key in CACHE_CHART:
        if now_ts - CACHE_CHART[cache_key]["ts"] < CHART_TTL:
            return CACHE_CHART[cache_key]["data"], True

    ticker = yahoo_ticker_for(pair)
    hosts = ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]

    for host in hosts:
        url = f"https://{host}/v8/finance/chart/{ticker}?interval={interval}&range={range_str}"
        try:
            r = requests.get(url, headers=HEADERS_BROWSER, timeout=6)
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
            log_msg(f"Yahoo timeout/err ({host} {ticker}): {e}")

    if cache_key in CACHE_CHART:
        return CACHE_CHART[cache_key]["data"], False
    return [], False


def pip_size_for(pair: str) -> float:
    p = pair.upper()
    if "XAU" in p:
        return 0.1
    if "XAG" in p:
        return 0.01
    if "JPY" in p:
        return 0.01
    return 0.0001


def decimals_for(pair: str) -> int:
    p = pair.upper()
    if "XAU" in p:
        return 2
    if "XAG" in p:
        return 3
    if "JPY" in p:
        return 3
    return 5


# ==========================================
# LIVE PIP VALUE & FUTURES BASIS CALIBRATION
# ==========================================
def get_live_usd_rates() -> dict:
    now_ts = time.time()
    if now_ts - CACHE_FX["ts"] < FX_TTL and CACHE_FX["rates"]:
        return CACHE_FX["rates"]
    try:
        r = requests.get("https://open.er-api.com/v6/latest/USD", timeout=5)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data.get("rates"), dict) and data["rates"]:
                CACHE_FX["rates"] = data["rates"]
                CACHE_FX["ts"] = now_ts
    except Exception as e:
        log_msg(f"FX rates API err: {e}")
    return CACHE_FX.get("rates", {})


def usd_per_pip_for(pair: str) -> float:
    """
    Menghitung nilai USD per 1 pip untuk 1.00 Standard Lot secara dinamis dari kurs live:
    - Pair berakhiran USD (EURUSD, GBPUSD, AUDUSD, XAUUSD): tepat $10.00/pip (spesifikasi kontrak).
    - Cross / USD-base pair (USDJPY, EURGBP, USDCAD, dll.): konversi unit quote currency
      menggunakan kurs live USD/{QUOTE} saat ini.
    """
    p = pair.upper().strip()
    if p.endswith("USD"):
        return 10.0

    quote_ccy = p[3:]
    pv = pip_size_for(p)
    pip_in_quote = 100000.0 * pv  # 100,000 unit standard lot * ukuran 1 pip

    rates = get_live_usd_rates()
    usd_to_quote = rates.get(quote_ccy)

    if not usd_to_quote or usd_to_quote <= 0:
        # Fallback langsung ke live candle Yahoo Finance untuk USD{QUOTE}=X
        c_direct, _ = fetch_yahoo_candles(f"USD{quote_ccy}", "15m", "1d", force=False)
        if c_direct:
            usd_to_quote = c_direct[-1]["close"]
        else:
            c_inv, _ = fetch_yahoo_candles(f"{quote_ccy}USD", "15m", "1d", force=False)
            if c_inv and c_inv[-1]["close"] > 0:
                usd_to_quote = 1.0 / c_inv[-1]["close"]

    if usd_to_quote and usd_to_quote > 0:
        return round(pip_in_quote / float(usd_to_quote), 4)

    raise ValueError(f"Kurs live untuk konversi pip {p} ({quote_ccy}) sedang tidak tersedia.")


def calc_basis_offset(pair: str, entry: float, candles: list, ref_ts: int = None) -> float:
    """
    Kalibrasi selisih harga HANYA untuk instrumen futures (seperti GC=F vs spot XAUUSD),
    dan dicocokkan pada timestamp waktu entry dibuka (ref_ts), BUKAN harga live hari ini.
    Pair spot FX selalu 0.0 agar tidak menggeser candle historis.
    """
    p = pair.upper().strip()
    if p not in FUTURES_TICKERS or not candles or entry <= 0:
        return 0.0

    pv = pip_size_for(p)
    ref_price = candles[-1]["close"]

    if ref_ts:
        matched = None
        for c in candles:
            if c["ts"] <= ref_ts:
                matched = c["close"]
            else:
                if matched is None:
                    matched = c["open"]
                break
        if matched is not None:
            ref_price = matched

    diff = entry - ref_price
    return round(diff, decimals_for(p)) if abs(diff) > (5 * pv) else 0.0


# ==========================================
# LIVE CURRENCY STRENGTH (100% MARKET CANDLES)
# ==========================================
def get_currency_strength(pair: str, side: str):
    now_ts = time.time()
    scores = CACHE_STRENGTH.get("scores", {}) if (now_ts - CACHE_STRENGTH["ts"] < STRENGTH_TTL) else {}

    if not scores:
        usd_pairs = ["EURUSD", "GBPUSD", "AUDUSD", "NZDUSD", "USDJPY", "USDCAD", "USDCHF"]

        def fetch_pct(sym: str):
            d1, _ = fetch_yahoo_candles(sym, "1d", "5d", force=False)
            if not d1:
                return sym, None
            last = d1[-1]
            if last["open"] <= 0:
                return sym, None
            pct = ((last["close"] - last["open"]) / last["open"]) * 100.0
            return sym, pct

        raw_perf = {}
        with ThreadPoolExecutor(max_workers=7) as ex:
            for sym, pct in ex.map(fetch_pct, usd_pairs):
                if pct is None:
                    continue
                if sym.startswith("USD"):
                    ccy = sym[3:]
                    raw_perf[ccy] = -pct
                else:
                    ccy = sym[:3]
                    raw_perf[ccy] = pct

        if raw_perf:
            raw_perf["USD"] = -sum(raw_perf.values()) / len(raw_perf)
            min_p, max_p = min(raw_perf.values()), max(raw_perf.values())
            span = max_p - min_p
            scores = {}
            for ccy, val in raw_perf.items():
                if span > 0:
                    scores[ccy] = round(1.0 + ((val - min_p) / span) * 8.0, 1)
                else:
                    scores[ccy] = 5.0
            CACHE_STRENGTH["scores"] = scores
            CACHE_STRENGTH["ts"] = now_ts

    base_ccy = "XAU" if pair.startswith("XAU") else ("XAG" if pair.startswith("XAG") else pair[:3])
    quote_ccy = pair[3:]

    if not scores:
        return {
            "available": False,
            "base_ccy": base_ccy,
            "quote_ccy": quote_ccy,
            "base_score": None,
            "quote_score": None,
            "verdict": "UNAVAILABLE",
            "note": "Data live performa mata uang mayor sedang tidak tersedia dari feed market.",
            "ranking": [],
        }

    if base_ccy in ("XAU", "XAG"):
        usd_s = scores.get("USD")
        base_score = round(max(1.0, min(9.0, 10.0 - usd_s)), 1) if usd_s is not None else None
    else:
        base_score = scores.get(base_ccy)
    quote_score = scores.get(quote_ccy)

    if base_score is None or quote_score is None:
        verdict = "UNAVAILABLE"
        note = f"Skor live untuk {base_ccy}/{quote_ccy} tidak lengkap."
    else:
        diff = round(base_score - quote_score, 1)
        if side == "buy":
            if diff >= 0.8:
                verdict, note = "ALIGNED", f"{base_ccy} ({base_score}) lebih kuat dari {quote_ccy} ({quote_score}) berdasarkan performa candle harian live."
            elif diff <= -0.8:
                verdict, note = "COUNTER", f"Perhatian: {quote_ccy} ({quote_score}) lebih dominan dari {base_ccy} ({base_score}) (counter-strength)."
            else:
                verdict, note = "NEUTRAL", f"Kekuatan {base_ccy} vs {quote_ccy} relatif berimbang (selisih {diff})."
        else:
            if diff <= -0.8:
                verdict, note = "ALIGNED", f"{quote_ccy} ({quote_score}) lebih kuat dari {base_ccy} ({base_score}) berdasarkan performa candle harian live."
            elif diff >= 0.8:
                verdict, note = "COUNTER", f"Perhatian: {base_ccy} ({base_score}) lebih dominan dari {quote_ccy} ({quote_score}) (counter-strength)."
            else:
                verdict, note = "NEUTRAL", f"Kekuatan {base_ccy} vs {quote_ccy} relatif berimbang (selisih {diff})."

    ranking = sorted([{"ccy": k, "score": v} for k, v in scores.items()], key=lambda x: x["score"], reverse=True)
    return {
        "available": True,
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
                timeout=6,
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
        relevant_ccys = {"USD"} if ("XAU" in p or "XAG" in p) else {p[:3], p[3:]}

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
            if pair and (pair.upper().startswith("XAU") or pair.upper().startswith("XAG")) and ccy == "USD":
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
            # Paksa basis = 0.0 untuk spot FX meski ada entri lama yang tersimpan sebelum update
            basis = float(e.get("basis_offset", 0.0)) if e["pair"] in FUTURES_TICKERS else 0.0
            if e.get("basis_offset", 0.0) != basis:
                e["basis_offset"] = basis
                changed = True

            for c in candles:
                if c["ts"] < created_ts:
                    continue
                hi = c["high"] + basis
                lo = c["low"] + basis
                if e["side"] == "buy":
                    if lo <= e["sl"]:
                        e["status"] = "SL_HIT"
                        e["pnl_usd"] = -abs(float(e.get("est_sl_usd", 0.0)))
                        e["pnl_pips"] = -abs(float(e.get("sl_pips", 0)))
                        e["closed_wib"] = now_wib.strftime("%Y-%m-%d %H:%M WIB")
                        changed = True
                        break
                    if hi >= e["tp"]:
                        e["status"] = "TP_HIT"
                        e["pnl_usd"] = abs(float(e.get("est_tp_usd", 0.0)))
                        e["pnl_pips"] = abs(float(e.get("tp_pips", 0)))
                        e["closed_wib"] = now_wib.strftime("%Y-%m-%d %H:%M WIB")
                        changed = True
                        break
                else:
                    if hi >= e["sl"]:
                        e["status"] = "SL_HIT"
                        e["pnl_usd"] = -abs(float(e.get("est_sl_usd", 0.0)))
                        e["pnl_pips"] = -abs(float(e.get("sl_pips", 0)))
                        e["closed_wib"] = now_wib.strftime("%Y-%m-%d %H:%M WIB")
                        changed = True
                        break
                    if lo <= e["tp"]:
                        e["status"] = "TP_HIT"
                        e["pnl_usd"] = abs(float(e.get("est_tp_usd", 0.0)))
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
                "id": e.get("id"),
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


def rebuild_monthly_sl_and_cooldown(state: dict):
    """Menghitung ulang kuota SL bulanan & cooldown dari catatan daily_pnl aktual."""
    monthly_sl = {}
    for d_str, rec in state.get("daily_pnl", {}).items():
        losses = int(rec.get("losses", 0))
        if losses > 0:
            m_key = d_str[:7]
            monthly_sl[m_key] = monthly_sl.get(m_key, 0) + losses
    state["monthly_sl"] = monthly_sl
    now_wib = datetime.now(WIB)
    cur_month = now_wib.strftime("%Y-%m")
    if monthly_sl.get(cur_month, 0) < 3:
        state["cooldown_until"] = None


# ==========================================
# ROUTES
# ==========================================
@app.route("/health", methods=["GET"])
def health():
    age = round(time.time() - CACHE_NEWS["ts"], 1) if CACHE_NEWS["ts"] > 0 else None
    key_type = "EMPTY"
    if SUPABASE_KEY.startswith("eyJ"):
        key_type = "JWT_LEGACY"
    elif SUPABASE_KEY.startswith("sb_"):
        key_type = "SB_NEW_KEY"
    elif SUPABASE_KEY:
        key_type = "CUSTOM"

    return jsonify({
        "status": "online",
        "service": "ZIGFT Quant & Portfolio Engine",
        "supabase_url_set": bool(SUPABASE_URL),
        "supabase_key_type": key_type,
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
    h1, _ = fetch_yahoo_candles(pair, "1h", "1mo", force=False)
    d1, _ = fetch_yahoo_candles(pair, "1d", "1y", force=False)

    pv = pip_size_for(pair)
    dec = decimals_for(pair)

    basis = calc_basis_offset(pair, entry, m15)
    spot_calibrated = basis != 0.0

    # 1. SMC Dealing Range & OTE (100% dari candle M15 live)
    if m15:
        recent = m15[-96:]
        rng_high = max(c["high"] for c in recent) + basis
        rng_low = min(c["low"] for c in recent) + basis
        span = rng_high - rng_low
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
            "available": True,
            "zone": zone,
            "verdict": verdict,
            "eq_50": eq_50,
            "fib_618": fib_618,
            "fib_750": fib_750,
            "note": f"Dealing range 24 jam {round(rng_low, dec)} – {round(rng_high, dec)}. Entry berada di zona {zone}.",
            "idm_swept": len(m15) >= 12,
            "idm_note": "Minor internal swing terdeteksi — pastikan sweep IDM valid sebelum eksekusi.",
        }
    else:
        smc_obj = {
            "available": False,
            "zone": "UNAVAILABLE",
            "verdict": "UNAVAILABLE",
            "eq_50": None,
            "fib_618": None,
            "fib_750": None,
            "note": "Candle M15 live sedang tidak tersedia untuk menghitung zona dealing range.",
            "idm_swept": False,
            "idm_note": "Menunggu feed candle live.",
        }

    # 2. Liquidity Pools (100% dari candle D1 & M15 asli)
    pdh = round(d1[-2]["high"] + basis, dec) if len(d1) >= 2 else None
    pdl = round(d1[-2]["low"] + basis, dec) if len(d1) >= 2 else None
    if len(d1) >= 6:
        week_slice = d1[-6:-1]
        pwh = round(max(c["high"] for c in week_slice) + basis, dec)
        pwl = round(min(c["low"] for c in week_slice) + basis, dec)
    else:
        pwh, pwl = None, None

    asian_candles = [c for c in m15[-96:] if 4 <= datetime.fromtimestamp(c["ts"], tz=WIB).hour < 13]
    asian_high = round(max(c["high"] for c in asian_candles) + basis, dec) if asian_candles else None
    asian_low = round(min(c["low"] for c in asian_candles) + basis, dec) if asian_candles else None

    sweep_alerts = []
    for lbl, lvl in [("PDH", pdh), ("PDL", pdl), ("PWH", pwh), ("PWL", pwl), ("Asian High", asian_high), ("Asian Low", asian_low)]:
        if lvl is not None and abs(sl - lvl) <= (12 * pv):
            sweep_alerts.append(f"SL ({sl}) terlalu dekat dengan {lbl} ({lvl}) — rawan liquidity sweep.")

    # 3. Killzone & Session Window (WIB)
    now_wib = datetime.now(WIB)
    hr, wd = now_wib.hour, now_wib.weekday()
    days_id = ["Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu", "Minggu"]
    is_weekend = (wd == 5 and hr >= 4) or wd == 6 or (wd == 0 and hr < 4)
    if 14 <= hr < 17:
        kz_name, kz_qual, kz_hours = "LONDON KILLZONE", "KILLZONE", {14, 15, 16}
    elif 19 <= hr < 23:
        kz_name, kz_qual, kz_hours = "NEW YORK KILLZONE", "KILLZONE", {19, 20, 21, 22}
    elif 4 <= hr < 7:
        kz_name, kz_qual, kz_hours = "ROLLOVER / SPREAD WIDENING", "SPREAD_RISK", {4, 5, 6}
    else:
        kz_name, kz_qual, kz_hours = "ASIAN / INTER-SESSION", "NORMAL", {0, 1, 2, 3, 7, 8, 9, 10, 11, 12, 13, 17, 18, 23}

    # 4. Volatility ATR-14 (100% dari candle D1 asli)
    if len(d1) >= 2:
        lookback = min(14, len(d1) - 1)
        trs = [
            max(
                d1[i]["high"] - d1[i]["low"],
                abs(d1[i]["high"] - d1[i - 1]["close"]),
                abs(d1[i]["low"] - d1[i - 1]["close"]),
            )
            for i in range(-lookback, 0)
        ]
        atr_pips = round((sum(trs) / len(trs)) / pv, 1)
        today_pips = round((d1[-1]["high"] - d1[-1]["low"]) / pv, 1)
        exh_pct = round((today_pips / atr_pips) * 100, 1) if atr_pips > 0 else 0.0
        atr_status = "STRETCHED" if exh_pct >= 85 else "OPTIMAL"
        vol_note = f"Pergerakan harian telah memakai {exh_pct}% dari rata-rata ATR-{lookback} ({atr_pips} pips)."
    else:
        atr_pips, today_pips, exh_pct = None, None, None
        atr_status = "UNAVAILABLE"
        vol_note = "Data harian tidak cukup untuk menghitung ATR."

    # 5. Seasonality Day & Session (100% dari candle historis D1 & H1 asli)
    day_wins, day_tot, day_moves = 0, 0, []
    for c in d1:
        if datetime.fromtimestamp(c["ts"], tz=WIB).weekday() == wd:
            day_tot += 1
            day_moves.append(abs(c["close"] - c["open"]) / pv)
            if (side == "buy" and c["close"] > c["open"]) or (side == "sell" and c["close"] < c["open"]):
                day_wins += 1
    day_wr = round((day_wins / day_tot) * 100, 1) if day_tot > 0 else None
    day_med = round(sorted(day_moves)[len(day_moves) // 2], 1) if day_moves else None

    sess_by_date = {}
    for c in h1:
        dt_c = datetime.fromtimestamp(c["ts"], tz=WIB)
        if dt_c.hour in kz_hours:
            d_key = dt_c.strftime("%Y-%m-%d")
            grp = sess_by_date.setdefault(d_key, {"open": c["open"], "close": c["close"], "high": c["high"], "low": c["low"]})
            grp["close"] = c["close"]
            grp["high"] = max(grp["high"], c["high"])
            grp["low"] = min(grp["low"], c["low"])

    sess_wins, sess_tot, sess_moves = 0, len(sess_by_date), []
    for grp in sess_by_date.values():
        sess_moves.append((grp["high"] - grp["low"]) / pv)
        if (side == "buy" and grp["close"] > grp["open"]) or (side == "sell" and grp["close"] < grp["open"]):
            sess_wins += 1
    sess_wr = round((sess_wins / sess_tot) * 100, 1) if sess_tot > 0 else None
    sess_med = round(sorted(sess_moves)[len(sess_moves) // 2], 1) if sess_moves else None

    try:
        usd_per_pip = usd_per_pip_for(pair)
        est_sl_usd = round(sl_pips * usd_per_pip, 2)
        est_tp_usd = round(tp_pips * usd_per_pip, 2)
    except ValueError:
        usd_per_pip, est_sl_usd, est_tp_usd = None, None, None

    return jsonify({
        "feed_source": "YAHOO LIVE" if is_live else "CACHE / UNAVAILABLE",
        "spot_calibrated": spot_calibrated,
        "side": side.upper(),
        "usd_per_pip_1lot": round(usd_per_pip, 2) if usd_per_pip is not None else None,
        "est_sl_usd_1lot": est_sl_usd,
        "est_tp_usd_1lot": est_tp_usd,
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
            "note": vol_note,
        },
        "strength": get_currency_strength(pair, side),
        "seasonality": {
            "day_name": days_id[wd],
            "day_winrate": day_wr,
            "day_samples": day_tot,
            "day_median_pips": day_med,
            "session_name": kz_name,
            "session_winrate": sess_wr,
            "session_samples": sess_tot,
            "session_median_pips": sess_med,
        },
    })


@app.route("/api/entries", methods=["POST", "OPTIONS"])
def api_entries():
    if request.method == "OPTIONS":
        return ("", 200)

    body = request.get_json(silent=True) or {}
    if not body.get("inducement_confirmed") or not body.get("liquidity_confirmed"):
        return jsonify({"error": "Wajib konfirmasi Inducement (IDM) dan Liquidity Pool sesuai aturan ZIGFT."}), 400

    pair = str(body.get("pair", "")).upper().strip()
    side = str(body.get("side", "buy")).lower().strip()
    entry = float(body.get("entry", 0))
    sl = float(body.get("sl", 0))
    tp = float(body.get("tp", 0))
    sl_pips = float(body.get("sl_pips", 0))
    tp_pips = float(body.get("tp_pips", 0))
    rr = float(body.get("rr", 0))

    if side == "buy" and not (sl < entry < tp):
        return jsonify({"error": "Arah BUY tidak valid: SL harus di bawah Entry dan TP di atas Entry."}), 400
    if side == "sell" and not (tp < entry < sl):
        return jsonify({"error": "Arah SELL tidak valid: SL harus di atas Entry dan TP di bawah Entry."}), 400

    state = load_state()
    state, zigft_rules = process_portfolio_lifecycle(state, force_live=False)
    if not zigft_rules["can_enter"]:
        return jsonify({"error": zigft_rules["block_reason"]}), 403

    try:
        usd_per_pip = usd_per_pip_for(pair)
    except ValueError as e:
        return jsonify({"error": str(e)}), 502

    m15, _ = fetch_yahoo_candles(pair, "15m", "5d", force=False)
    basis = calc_basis_offset(pair, entry, m15)
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
        "rr": rr if rr > 0 else (round(tp_pips / sl_pips, 2) if sl_pips > 0 else 0.0),
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
    ok, err_msg = save_state(state)
    if not ok:
        return jsonify({"error": err_msg}), 500

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
    trade_time = str(body.get("trade_time", "")).strip()

    if entry <= 0 or sl <= 0 or tp <= 0 or not trade_date:
        return jsonify({"error": "Lengkapi Pair, Entry, SL, TP, dan Tanggal Trade."}), 400

    if side == "buy" and not (sl < entry < tp):
        return jsonify({"error": "Arah BUY tidak valid: SL harus di bawah Entry dan TP di atas Entry."}), 400
    if side == "sell" and not (tp < entry < sl):
        return jsonify({"error": "Arah SELL tidak valid: SL harus di atas Entry dan TP di bawah Entry."}), 400

    try:
        usd_per_pip = usd_per_pip_for(pair)
    except ValueError as e:
        return jsonify({"error": str(e)}), 502

    pv = pip_size_for(pair)
    sl_pips = round(abs(entry - sl) / pv, 1)
    tp_pips = round(abs(tp - entry) / pv, 1)
    rr = round(tp_pips / sl_pips, 2) if sl_pips > 0 else 0.0
    est_sl_usd = round(sl_pips * usd_per_pip, 2)
    est_tp_usd = round(tp_pips * usd_per_pip, 2)

    state = load_state()
    now_wib = datetime.now(WIB)

    # 1. Trade SUDAH TP atau SUDAH SL -> langsung masuk Kalender PnL permanen
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
            "id": f"ZIGFT-M-{int(now_wib.timestamp())}",
            "pair": pair,
            "side": side.upper(),
            "entry": entry,
            "sl": sl,
            "tp": tp,
            "rr": rr,
            "result": outcome,
            "pnl_usd": pnl_val,
            "pnl_pips": pnl_pips,
        })

        ok, err_msg = save_state(state)
        if not ok:
            return jsonify({"error": err_msg}), 500

        log_msg(f"Backfill settled: {pair} {side.upper()} {outcome} on {trade_date} (${pnl_val})")
        return jsonify({
            "status": "ok",
            "message": f"Riwayat {pair} ({outcome}) tanggal {trade_date} berhasil masuk permanen ke Kalender PnL!",
        })

    # 2. Trade MASIH BERJALAN (OPEN) sejak tanggal/jam lampau
    eff_time = trade_time if trade_time else now_wib.strftime("%H:%M")
    try:
        opened_dt = datetime.fromisoformat(f"{trade_date}T{eff_time}:00+07:00")
    except Exception:
        return jsonify({"error": "Format Tanggal / Jam Trade tidak valid."}), 400

    m15, _ = fetch_yahoo_candles(pair, "15m", "1mo", force=True)
    basis = calc_basis_offset(pair, entry, m15, int(opened_dt.timestamp()))

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
    ok, err_msg = save_state(state)
    if not ok:
        return jsonify({"error": err_msg}), 500

    return jsonify({
        "status": "ok",
        "message": f"Trade berjalan {pair} {side.upper()} sejak {opened_dt.strftime('%d %b %H:%M WIB')} berhasil disinkronkan & dicek ke candle live!",
    })


# ==========================================
# AUDIT ENDPOINTS (EDIT / HAPUS DATA)
# ==========================================
@app.route("/api/entries/<entry_id>", methods=["PATCH", "DELETE", "OPTIONS"])
def api_audit_raw_entry(entry_id: str):
    if request.method == "OPTIONS":
        return ("", 200)

    state = load_state()
    entries = state.get("entries", [])
    idx = next((i for i, e in enumerate(entries) if e.get("id") == entry_id), None)
    if idx is None:
        return jsonify({"error": "Entry tidak ditemukan (mungkin sudah di-purge otomatis)."}), 404

    if request.method == "DELETE":
        removed = entries.pop(idx)
        ok, err_msg = save_state(state)
        if not ok:
            return jsonify({"error": err_msg}), 500
        return jsonify({"status": "ok", "message": f"Entry {removed['pair']} dihapus dari daftar aktif."})

    body = request.get_json(silent=True) or {}
    e = entries[idx]
    pair = str(body.get("pair", e["pair"])).upper().strip()
    side = str(body.get("side", e["side"])).lower().strip()
    entry = float(body.get("entry", e["entry"]))
    sl = float(body.get("sl", e["sl"]))
    tp = float(body.get("tp", e["tp"]))

    if side == "buy" and not (sl < entry < tp):
        return jsonify({"error": "Arah BUY tidak valid: SL harus di bawah Entry dan TP di atas Entry."}), 400
    if side == "sell" and not (tp < entry < sl):
        return jsonify({"error": "Arah SELL tidak valid: SL harus di atas Entry dan TP di bawah Entry."}), 400

    usd_per_pip = usd_per_pip_for(pair)
    pv = pip_size_for(pair)
    sl_pips = round(abs(entry - sl) / pv, 1)
    tp_pips = round(abs(tp - entry) / pv, 1)
    m15, _ = fetch_yahoo_candles(pair, "15m", "1mo", force=True)

    e.update({
        "pair": pair,
        "side": side,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "sl_pips": sl_pips,
        "tp_pips": tp_pips,
        "rr": round(tp_pips / sl_pips, 2) if sl_pips > 0 else 0.0,
        "basis_offset": calc_basis_offset(pair, entry, m15, e.get("created_ts")),
        "est_sl_usd": round(sl_pips * usd_per_pip, 2),
        "est_tp_usd": round(tp_pips * usd_per_pip, 2),
        "status": "OPEN",
        "pnl_usd": 0.0,
        "pnl_pips": 0.0,
        "settled": False,
        "audited_wib": datetime.now(WIB).strftime("%d %b %H:%M WIB"),
    })

    state, _ = process_portfolio_lifecycle(state, force_live=True)
    ok, err_msg = save_state(state)
    if not ok:
        return jsonify({"error": err_msg}), 500
    return jsonify({"status": "ok", "message": f"Audit tersimpan & dicek ulang ke candle live: {pair} {side.upper()}."})


@app.route("/api/portfolio/settled", methods=["PATCH", "DELETE", "OPTIONS"])
def api_audit_settled_trade():
    if request.method == "OPTIONS":
        return ("", 200)

    body = request.get_json(silent=True) or {}
    trade_date = str(body.get("date", "")).strip()
    trade_idx = int(body.get("index", -1))

    state = load_state()
    day_rec = state.get("daily_pnl", {}).get(trade_date)
    if not day_rec or trade_idx < 0 or trade_idx >= len(day_rec.get("trades", [])):
        return jsonify({"error": "Catatan trade kalender tidak ditemukan."}), 404

    if request.method == "DELETE":
        day_rec["trades"].pop(trade_idx)
    else:
        tr = day_rec["trades"][trade_idx]
        if "pair" in body:
            tr["pair"] = str(body["pair"]).upper().strip()
        if "side" in body:
            tr["side"] = str(body["side"]).upper().strip()
        if "result" in body:
            new_res = str(body["result"]).upper().strip()
            tr["result"] = new_res
            mag_usd = abs(float(tr.get("pnl_usd", 0.0)))
            mag_pips = abs(float(tr.get("pnl_pips", 0.0)))
            tr["pnl_usd"] = mag_usd if new_res == "TP_HIT" else -mag_usd
            tr["pnl_pips"] = mag_pips if new_res == "TP_HIT" else -mag_pips
        if "pnl_usd" in body:
            tr["pnl_usd"] = round(float(body["pnl_usd"]), 2)

    if not day_rec["trades"]:
        del state["daily_pnl"][trade_date]
    else:
        day_rec["pnl_usd"] = round(sum(float(t.get("pnl_usd", 0.0)) for t in day_rec["trades"]), 2)
        day_rec["wins"] = sum(1 for t in day_rec["trades"] if t.get("result") == "TP_HIT")
        day_rec["losses"] = sum(1 for t in day_rec["trades"] if t.get("result") == "SL_HIT")

    rebuild_monthly_sl_and_cooldown(state)
    ok, err_msg = save_state(state)
    if not ok:
        return jsonify({"error": err_msg}), 500
    return jsonify({"status": "ok", "message": f"Audit kalender tanggal {trade_date} berhasil diperbarui."})


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
