import gc
import json
import os
import re
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
import numpy as np
import pandas as pd
import requests
import yfinance as yf
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

INTRADAY_TTL = int(os.environ.get("INTRADAY_TTL", 300))       # 5 menit
HISTORICAL_TTL = int(os.environ.get("HISTORICAL_TTL", 21600)) # 6 jam
NEWS_TTL = int(os.environ.get("NEWS_TTL", 1800))              # 30 menit

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "").strip()
LOCAL_STORE_PATH = os.environ.get("LOCAL_STORE_PATH", "/tmp/zigft_portfolio_store.json")

WIB = timezone(timedelta(hours=7))

TF_PAIRS = {
    "NZDUSD": {"vp": 2, "min_sl": 10, "max_sl": 200, "pv": 0.0001, "tickers": ["NZDUSD=X"]},
    "AUDUSD": {"vp": 2, "min_sl": 10, "max_sl": 200, "pv": 0.0001, "tickers": ["AUDUSD=X"]},
    "EURGBP": {"vp": 2, "min_sl": 10, "max_sl": 200, "pv": 0.0001, "tickers": ["EURGBP=X"]},
    "USDCHF": {"vp": 2, "min_sl": 10, "max_sl": 200, "pv": 0.0001, "tickers": ["USDCHF=X"]},
    "USDCAD": {"vp": 1.5, "min_sl": 10, "max_sl": 200, "pv": 0.0001, "tickers": ["USDCAD=X"]},
    "EURUSD": {"vp": 1.5, "min_sl": 10, "max_sl": 200, "pv": 0.0001, "tickers": ["EURUSD=X"]},
    "GBPUSD": {"vp": 1.5, "min_sl": 10, "max_sl": 200, "pv": 0.0001, "tickers": ["GBPUSD=X"]},
    "NZDJPY": {"vp": 1.5, "min_sl": 15, "max_sl": 300, "pv": 0.01, "tickers": ["NZDJPY=X"]},
    "CADJPY": {"vp": 1.5, "min_sl": 15, "max_sl": 300, "pv": 0.01, "tickers": ["CADJPY=X"]},
    "AUDJPY": {"vp": 1.5, "min_sl": 15, "max_sl": 300, "pv": 0.01, "tickers": ["AUDJPY=X"]},
    "CHFJPY": {"vp": 1, "min_sl": 20, "max_sl": 400, "pv": 0.01, "tickers": ["CHFJPY=X"]},
    "USDJPY": {"vp": 1, "min_sl": 20, "max_sl": 400, "pv": 0.01, "tickers": ["USDJPY=X"]},
    "EURJPY": {"vp": 1, "min_sl": 20, "max_sl": 400, "pv": 0.01, "tickers": ["EURJPY=X"]},
    "GBPJPY": {"vp": 1, "min_sl": 20, "max_sl": 400, "pv": 0.01, "tickers": ["GBPJPY=X"]},
    "EURNZD": {"vp": 1, "min_sl": 20, "max_sl": 400, "pv": 0.0001, "tickers": ["EURNZD=X"]},
    "XAUUSD": {"vp": 0.5, "min_sl": 30, "max_sl": 500, "pv": 0.1, "tickers": ["GC=F"]},
}

HARI_ID = ["Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu", "Minggu"]

_CACHE = {
    "intraday_ts": 0.0,
    "historical_ts": 0.0,
    "news_ts": 0.0,
    "intraday_data": {},
    "intraday_bars": {},
    "historical_data": {},
    "csm": {},
    "news_events": [],
}
_CACHE_LOCK = threading.Lock()
_STORE_LOCK = threading.Lock()


def _default_store():
    return {
        "raw_entries": [],
        "daily_pnl": {},
        "sl_events": [],
        "cooldown_until": None,
    }


def _supabase_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=representation",
    }


def load_store() -> dict:
    if SUPABASE_URL and SUPABASE_KEY:
        try:
            url = f"{SUPABASE_URL}/rest/v1/zigft_state?id=eq.main&select=payload"
            r = requests.get(url, headers=_supabase_headers(), timeout=8)
            if r.status_code == 200:
                rows = r.json()
                if rows and isinstance(rows[0].get("payload"), dict):
                    data = _default_store()
                    data.update(rows[0]["payload"])
                    return data
        except Exception:
            pass

    if os.path.exists(LOCAL_STORE_PATH):
        try:
            with open(LOCAL_STORE_PATH, "r", encoding="utf-8") as f:
                data = _default_store()
                data.update(json.load(f))
                return data
        except Exception:
            pass
    return _default_store()


def save_store(store: dict):
    try:
        with open(LOCAL_STORE_PATH, "w", encoding="utf-8") as f:
            json.dump(store, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    if SUPABASE_URL and SUPABASE_KEY:
        try:
            url = f"{SUPABASE_URL}/rest/v1/zigft_state"
            body = {"id": "main", "payload": store, "updated_at": datetime.now(timezone.utc).isoformat()}
            requests.post(url, headers=_supabase_headers(), json=body, timeout=8)
        except Exception:
            pass


def get_killzone_info(now_utc: datetime):
    now_wib = now_utc.astimezone(WIB)
    wd = now_utc.weekday()
    hr_utc = now_utc.hour

    is_weekend = (wd == 4 and hr_utc >= 22) or (wd == 5) or (wd == 6 and hr_utc < 22)

    if 0 <= hr_utc < 6:
        session_key, zone_name, quality = "ASIA", "Asian Accumulation Range", "LOW_VOL"
    elif 6 <= hr_utc < 7:
        session_key, zone_name, quality = "PRE_LONDON", "Pre-London Manipulation", "TRANSITION"
    elif 7 <= hr_utc < 10:
        session_key, zone_name, quality = "LONDON", "London Open Killzone", "KILLZONE"
    elif 10 <= hr_utc < 12:
        session_key, zone_name, quality = "LONDON_MID", "London-NY Pre-Overlap", "MODERATE"
    elif 12 <= hr_utc < 15:
        session_key, zone_name, quality = "NY", "New York Open Killzone", "KILLZONE"
    elif 15 <= hr_utc < 17:
        session_key, zone_name, quality = "LONDON_CLOSE", "London Close Reversal Window", "MODERATE"
    elif 17 <= hr_utc < 21:
        session_key, zone_name, quality = "NY_PM", "NY Afternoon Session", "LOW_VOL"
    else:
        session_key, zone_name, quality = "DEAD_ZONE", "Daily Rollover / Dead Zone", "SPREAD_RISK"

    return {
        "wib_time": now_wib.strftime("%H:%M WIB"),
        "wib_date": now_wib.strftime("%Y-%m-%d"),
        "day_name": HARI_ID[now_wib.weekday()],
        "day_idx": now_wib.weekday() if now_wib.weekday() < 5 else 4,
        "session_key": session_key,
        "zone_name": zone_name,
        "quality": quality,
        "is_weekend": is_weekend,
    }


def _extract_ticker_df(raw_df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if raw_df is None or raw_df.empty:
        return pd.DataFrame()
    if isinstance(raw_df.columns, pd.MultiIndex):
        lvl0 = raw_df.columns.get_level_values(0)
        lvl1 = raw_df.columns.get_level_values(1)
        if ticker in lvl0:
            sub = raw_df[ticker].copy()
        elif ticker in lvl1:
            sub = raw_df.xs(ticker, axis=1, level=1).copy()
        else:
            return pd.DataFrame()
    else:
        sub = raw_df.copy()

    sub = sub.dropna(subset=["Open", "High", "Low", "Close"])
    if not sub.empty and sub.index.tz is None:
        sub.index = sub.index.tz_localize("UTC")
    elif not sub.empty:
        sub.index = sub.index.tz_convert("UTC")
    return sub


def _download_batch(tickers: list, period: str, interval: str) -> pd.DataFrame:
    return yf.download(
        tickers=tickers,
        period=period,
        interval=interval,
        group_by="ticker",
        auto_adjust=False,
        progress=False,
        threads=False,
    )


def _compute_smc_structure(df_1h: pd.DataFrame, pv: float) -> dict:
    if df_1h is None or len(df_1h) < 20:
        return {}

    df_4h = df_1h.resample("4h").agg({
        "Open": "first",
        "High": "max",
        "Low": "min",
        "Close": "last"
    }).dropna()

    if len(df_4h) < 8:
        df_4h = df_1h.copy()

    closed_4h = df_4h.iloc[:-1] if len(df_4h) > 6 else df_4h
    highs = closed_4h["High"].values
    lows = closed_4h["Low"].values

    swing_highs = []
    swing_lows = []
    for i in range(2, len(closed_4h) - 2):
        if highs[i] >= highs[i - 1] and highs[i] >= highs[i - 2] and highs[i] >= highs[i + 1] and highs[i] >= highs[i + 2]:
            swing_highs.append(float(highs[i]))
        if lows[i] <= lows[i - 1] and lows[i] <= lows[i - 2] and lows[i] <= lows[i + 1] and lows[i] <= lows[i + 2]:
            swing_lows.append(float(lows[i]))

    range_high = swing_highs[-1] if swing_highs else float(df_4h["High"].tail(18).max())
    range_low = swing_lows[-1] if swing_lows else float(df_4h["Low"].tail(18).min())

    if range_high <= range_low:
        range_high = float(df_4h["High"].tail(24).max())
        range_low = float(df_4h["Low"].tail(24).min())

    recent_1h = df_1h.tail(36)
    h1_highs = recent_1h["High"].values
    h1_lows = recent_1h["Low"].values
    internal_highs = []
    internal_lows = []
    for i in range(2, len(recent_1h) - 3):
        if h1_highs[i] > h1_highs[i - 1] and h1_highs[i] > h1_highs[i + 1]:
            internal_highs.append(float(h1_highs[i]))
        if h1_lows[i] < h1_lows[i - 1] and h1_lows[i] < h1_lows[i + 1]:
            internal_lows.append(float(h1_lows[i]))

    last_6h_high = float(recent_1h["High"].tail(6).max())
    last_6h_low = float(recent_1h["Low"].tail(6).min())

    idm_buy_swept = False
    idm_buy_lvl = None
    if internal_lows:
        idm_buy_lvl = internal_lows[-1]
        idm_buy_swept = last_6h_low < idm_buy_lvl

    idm_sell_swept = False
    idm_sell_lvl = None
    if internal_highs:
        idm_sell_lvl = internal_highs[-1]
        idm_sell_swept = last_6h_high > idm_sell_lvl

    return {
        "range_high": range_high,
        "range_low": range_low,
        "idm_buy_swept": idm_buy_swept,
        "idm_buy_lvl": idm_buy_lvl,
        "idm_sell_swept": idm_sell_swept,
        "idm_sell_lvl": idm_sell_lvl,
    }


def _refresh_historical():
    all_tickers = list({t for info in TF_PAIRS.values() for t in info["tickers"]})
    df_1d_raw = _download_batch(all_tickers, period="2y", interval="1d")
    df_1h_raw = _download_batch(all_tickers, period="60d", interval="1h")

    hist_out = {}
    for pair, info in TF_PAIRS.items():
        pv = info["pv"]
        used_ticker = info["tickers"][0]
        df_1d = _extract_ticker_df(df_1d_raw, used_ticker)
        df_1h = _extract_ticker_df(df_1h_raw, used_ticker)

        if df_1d.empty or len(df_1d) < 16:
            continue

        closed_1d = df_1d.iloc[:-1].copy()
        prev_close = closed_1d["Close"].shift(1)
        tr = pd.concat([
            closed_1d["High"] - closed_1d["Low"],
            (closed_1d["High"] - prev_close).abs(),
            (closed_1d["Low"] - prev_close).abs(),
        ], axis=1).max(axis=1)
        atr_14_pips = round(float(tr.tail(14).mean()) / pv, 1)

        pdh = float(closed_1d["High"].iloc[-1])
        pdl = float(closed_1d["Low"].iloc[-1])

        iso = closed_1d.index.isocalendar()
        closed_1d["yw"] = iso.year.astype(str) + "-" + iso.week.astype(str).str.zfill(2)
        weeks = closed_1d["yw"].unique()
        if len(weeks) >= 2:
            prev_week_df = closed_1d[closed_1d["yw"] == weeks[-2]]
            pwh = float(prev_week_df["High"].max())
            pwl = float(prev_week_df["Low"].min())
        else:
            pwh = float(closed_1d["High"].tail(5).max())
            pwl = float(closed_1d["Low"].tail(5).min())

        closed_1d["wd"] = closed_1d.index.weekday
        closed_1d["bullish"] = closed_1d["Close"] > closed_1d["Open"]
        closed_1d["range_pips"] = (closed_1d["High"] - closed_1d["Low"]) / pv

        dow_stats = {}
        for wd_i in range(5):
            sub_wd = closed_1d[closed_1d["wd"] == wd_i]
            n_samples = int(len(sub_wd))
            if n_samples > 0:
                bull_pct = round(float(sub_wd["bullish"].mean() * 100), 1)
                dow_stats[wd_i] = {
                    "bull_pct": bull_pct,
                    "bear_pct": round(100.0 - bull_pct, 1),
                    "median_pips": round(float(sub_wd["range_pips"].median()), 1),
                    "samples": n_samples,
                }
            else:
                dow_stats[wd_i] = {"bull_pct": 50.0, "bear_pct": 50.0, "median_pips": atr_14_pips, "samples": 0}

        session_stats = {}
        if not df_1h.empty:
            df_1h["hr"] = df_1h.index.hour
            df_1h["date_str"] = df_1h.index.strftime("%Y-%m-%d")
            df_1h["sess"] = df_1h["hr"].apply(lambda h: "ASIA" if 0 <= h < 6 else ("LONDON" if 6 <= h < 12 else "NY"))
            grouped = df_1h.groupby(["date_str", "sess"]).agg(
                open_p=("Open", "first"),
                high_p=("High", "max"),
                low_p=("Low", "min"),
                close_p=("Close", "last"),
            )
            grouped["bull"] = grouped["close_p"] > grouped["open_p"]
            grouped["exp_pips"] = (grouped["high_p"] - grouped["low_p"]) / pv

            for sk in ["ASIA", "LONDON", "NY"]:
                sub_s = grouped.xs(sk, level="sess") if sk in grouped.index.get_level_values("sess") else pd.DataFrame()
                n_s = int(len(sub_s))
                if n_s >= 5:
                    s_bull = round(float(sub_s["bull"].mean() * 100), 1)
                    session_stats[sk] = {
                        "bull_pct": s_bull,
                        "bear_pct": round(100.0 - s_bull, 1),
                        "median_pips": round(float(sub_s["exp_pips"].median()), 1),
                        "samples": n_s,
                    }
                else:
                    session_stats[sk] = {"bull_pct": 50.0, "bear_pct": 50.0, "median_pips": round(atr_14_pips * 0.45, 1), "samples": n_s}

        hist_out[pair] = {
            "ticker": used_ticker,
            "atr_14_pips": atr_14_pips,
            "pdh": pdh,
            "pdl": pdl,
            "pwh": pwh,
            "pwl": pwl,
            "dow_stats": dow_stats,
            "session_stats": session_stats,
        }

    del df_1d_raw, df_1h_raw
    gc.collect()
    return hist_out


def _refresh_intraday():
    all_tickers = list({t for info in TF_PAIRS.values() for t in info["tickers"]})
    df_15m_raw = _download_batch(all_tickers, period="5d", interval="15m")
    df_1h_raw = _download_batch(all_tickers, period="10d", interval="1h")

    intra_out = {}
    bars_out = {}
    raw_strength = {c: [] for c in ["USD", "EUR", "GBP", "JPY", "AUD", "NZD", "CAD", "CHF", "XAU"]}

    for pair, info in TF_PAIRS.items():
        pv = info["pv"]
        ticker = info["tickers"][0]
        df_15m = _extract_ticker_df(df_15m_raw, ticker)
        df_1h = _extract_ticker_df(df_1h_raw, ticker)

        if df_15m.empty:
            continue

        last_price = float(df_15m["Close"].iloc[-1])
        latest_date = df_15m.index[-1].date()
        today_df = df_15m[df_15m.index.date == latest_date]
        today_high = float(today_df["High"].max())
        today_low = float(today_df["Low"].min())
        today_range_pips = round((today_high - today_low) / pv, 1)

        asian_df = today_df[(today_df.index.hour >= 0) & (today_df.index.hour < 6)]
        asian_high = float(asian_df["High"].max()) if not asian_df.empty else today_high
        asian_low = float(asian_df["Low"].min()) if not asian_df.empty else today_low

        if len(df_1h) >= 2:
            c_now = float(df_1h["Close"].iloc[-1])
            c_4h = float(df_1h["Close"].iloc[-min(5, len(df_1h))])
            c_24h = float(df_1h["Close"].iloc[-min(25, len(df_1h))])
            ret_4h = ((c_now - c_4h) / c_4h) * 100.0 if c_4h else 0.0
            ret_24h = ((c_now - c_24h) / c_24h) * 100.0 if c_24h else 0.0
            momentum = (0.6 * ret_4h) + (0.4 * ret_24h)

            base_ccy, quote_ccy = pair[:3], pair[3:]
            if base_ccy in raw_strength:
                raw_strength[base_ccy].append(momentum)
            if quote_ccy in raw_strength:
                raw_strength[quote_ccy].append(-momentum)

        smc_struct = _compute_smc_structure(df_1h, pv)

        bars_list = []
        for ts, row in df_15m.tail(350).iterrows():
            bars_list.append({
                "ts": ts.isoformat(),
                "h": float(row["High"]),
                "l": float(row["Low"]),
                "c": float(row["Close"]),
            })
        bars_out[pair] = bars_list

        intra_out[pair] = {
            "last_price": last_price,
            "today_high": today_high,
            "today_low": today_low,
            "today_range_pips": today_range_pips,
            "asian_high": asian_high,
            "asian_low": asian_low,
            "smc": smc_struct,
        }

    avg_scores = {ccy: (float(np.mean(vals)) if vals else 0.0) for ccy, vals in raw_strength.items()}
    min_s = min(avg_scores.values()) if avg_scores else 0.0
    max_s = max(avg_scores.values()) if avg_scores else 0.0
    span = max_s - min_s
    csm_normalized = {
        ccy: (round(((val - min_s) / span) * 10.0, 1) if span > 1e-6 else 5.0)
        for ccy, val in avg_scores.items()
    }

    del df_15m_raw, df_1h_raw
    gc.collect()
    return intra_out, bars_out, csm_normalized


def _refresh_news():
    """Mengambil kalender ekonomi high-impact mingguan dari mirror live tanpa dummy."""
    endpoints = [
        "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
        "https://cdn-nfs.faireconomy.media/ff_calendar_thisweek.json",
    ]
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
    }
    for url in endpoints:
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                raw = resp.json()
                if isinstance(raw, list) and len(raw) > 0:
                    high_events = []
                    for item in raw:
                        if str(item.get("impact", "")).lower() == "high":
                            high_events.append({
                                "title": item.get("title", ""),
                                "country": str(item.get("country", "")).upper(),
                                "date": item.get("date", ""),
                                "forecast": str(item.get("forecast", "")).strip(),
                                "previous": str(item.get("previous", "")).strip(),
                            })
                    return high_events
        except Exception:
            continue
    return _CACHE.get("news_events", [])


def ensure_fresh_cache(force_intraday: bool = False, force_news: bool = False):
    now = time.time()
    with _CACHE_LOCK:
        if (now - _CACHE["historical_ts"]) > HISTORICAL_TTL or not _CACHE["historical_data"]:
            _CACHE["historical_data"] = _refresh_historical()
            _CACHE["historical_ts"] = time.time()

        if force_intraday or (now - _CACHE["intraday_ts"]) > INTRADAY_TTL or not _CACHE["intraday_data"]:
            intra, bars, csm = _refresh_intraday()
            _CACHE["intraday_data"] = intra
            _CACHE["intraday_bars"] = bars
            _CACHE["csm"] = csm
            _CACHE["intraday_ts"] = time.time()

        if force_news or (now - _CACHE["news_ts"]) > NEWS_TTL or not _CACHE["news_events"]:
            _CACHE["news_events"] = _refresh_news()
            _CACHE["news_ts"] = time.time()


def get_usd_per_pip_1lot(pair: str) -> float:
    pair = pair.upper()
    intra = _CACHE.get("intraday_data", {})
    pv = TF_PAIRS.get(pair, {}).get("pv", 0.0001)

    if pair == "XAUUSD":
        return 10.0
    if pair.endswith("USD"):
        return 10.0
    if pair.startswith("USD"):
        px = intra.get(pair, {}).get("last_price", 0)
        return round((pv * 100000.0) / px, 4) if px > 0 else (6.5 if "JPY" in pair else 10.0)
    if pair.endswith("JPY"):
        usdjpy = intra.get("USDJPY", {}).get("last_price", 0)
        return round(1000.0 / usdjpy, 4) if usdjpy > 0 else 6.5
    if pair == "EURGBP":
        gbpusd = intra.get("GBPUSD", {}).get("last_price", 0)
        return round(10.0 * gbpusd, 4) if gbpusd > 0 else 12.7
    if pair == "EURNZD":
        nzdusd = intra.get("NZDUSD", {}).get("last_price", 0)
        return round(10.0 * nzdusd, 4) if nzdusd > 0 else 6.0
    return 10.0


def _parse_num(val_str: str):
    if not val_str:
        return None
    cleaned = re.sub(r"[^0-9.\-]", "", val_str)
    if cleaned in ("", "-", "."):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def analyze_pre_news_prediction(pair: str, now_utc: datetime, smc_zone: str = "NEUTRAL", csm_data: dict = None):
    csm_data = csm_data or {}
    pair = pair.upper().strip()
    if pair in TF_PAIRS:
        base_ccy, quote_ccy = pair[:3], pair[3:]
        target_ccys = {"USD"} if pair == "XAUUSD" else {base_ccy, quote_ccy}
    else:
        base_ccy, quote_ccy = "XAU", "USD"
        target_ccys = {"USD", "EUR", "GBP", "JPY", "AUD", "NZD", "CAD", "CHF"}

    upcoming = []
    for ev in _CACHE.get("news_events", []):
        if ev["country"] not in target_ccys:
            continue
        try:
            ev_dt = datetime.fromisoformat(ev["date"]).astimezone(timezone.utc)
        except Exception:
            continue
        diff_min = (ev_dt - now_utc).total_seconds() / 60.0
        if diff_min >= -15:
            upcoming.append((diff_min, ev_dt, ev))

    upcoming.sort(key=lambda x: x[0])

    if not upcoming:
        return {
            "has_event": False,
            "pair_filter": pair or "ALL MAJORS",
            "note": f"Tidak ada jadwal berita High-Impact untuk {', '.join(sorted(target_ccys))} dalam sisa minggu ini.",
        }

    diff_min, ev_dt, ev = upcoming[0]
    ev_wib = ev_dt.astimezone(WIB)
    ccy = ev["country"]
    title = ev["title"]
    f_val = _parse_num(ev["forecast"])
    p_val = _parse_num(ev["previous"])

    is_inverse = any(k in title.lower() for k in ["unemployment", "jobless", "claims"])
    ccy_bias = "NEUTRAL"
    consensus_reason = "Konsensus Forecast sama dengan Previous (menunggu deviasi rilis)."

    if f_val is not None and p_val is not None and f_val != p_val:
        delta = f_val - p_val
        is_ccy_bullish = (delta < 0) if is_inverse else (delta > 0)
        ccy_bias = "BULLISH" if is_ccy_bullish else "BEARISH"
        consensus_reason = f"Forecast ({ev['forecast']}) {'lebih kuat' if is_ccy_bullish else 'lebih lemah'} dari Previous ({ev['previous']}) untuk {ccy}."
    elif f_val is None and p_val is not None:
        consensus_reason = f"Data Previous {ccy}: {ev['previous']} (tanpa angka Forecast)."

    pair_fundamental_bias = "NEUTRAL"
    if ccy_bias != "NEUTRAL":
        if pair in TF_PAIRS:
            if ccy == base_ccy:
                pair_fundamental_bias = ccy_bias
            elif ccy == quote_ccy:
                pair_fundamental_bias = "BEARISH" if ccy_bias == "BULLISH" else "BULLISH"
        else:
            pair_fundamental_bias = f"{ccy} {ccy_bias}"

    smc_bias = "NEUTRAL"
    if "PREMIUM" in smc_zone or "OTE_SELL" in smc_zone:
        smc_bias = "BEARISH"
    elif "DISCOUNT" in smc_zone or "OTE_BUY" in smc_zone:
        smc_bias = "BULLISH"

    if pair_fundamental_bias != "NEUTRAL" and pair_fundamental_bias == smc_bias:
        final_pred = f"STRONG {pair_fundamental_bias}"
        logic_summary = f"Konsensus & SMC searah: {consensus_reason} Harga sudah di zona {smc_zone}."
    elif pair_fundamental_bias != "NEUTRAL":
        final_pred = f"ANTICIPATE {pair_fundamental_bias}"
        logic_summary = f"Prediksi pra-rilis berbasis konsensus: {consensus_reason}"
    elif smc_bias != "NEUTRAL":
        final_pred = f"PRE-SWEEP {smc_bias}"
        logic_summary = f"{consensus_reason} Harga di zona {smc_zone} (waspadai sapuan likuiditas berlawanan sebelum berbalik)."
    else:
        final_pred = "WAIT SWEEP / NEUTRAL"
        logic_summary = consensus_reason

    mins_int = int(round(diff_min))
    if mins_int < 0:
        countdown_txt = f"Baru rilis {abs(mins_int)} menit lalu"
    elif mins_int < 60:
        countdown_txt = f"{mins_int} menit lagi"
    else:
        countdown_txt = f"{mins_int // 60} jam {mins_int % 60} menit lagi"

    return {
        "has_event": True,
        "pair_filter": pair or "ALL MAJORS",
        "title": title,
        "currency": ccy,
        "release_wib": f"{HARI_ID[ev_wib.weekday()]}, {ev_wib.strftime('%H:%M WIB')}",
        "release_ts_ms": int(ev_dt.timestamp() * 1000),
        "countdown": countdown_txt,
        "minutes_to_release": mins_int,
        "slippage_warning": 0 <= mins_int <= 30,
        "forecast": ev["forecast"] or "-",
        "previous": ev["previous"] or "-",
        "prediction": final_pred,
        "reasoning": logic_summary,
    }


def process_portfolio_lifecycle(force_check: bool = False):
    if force_check:
        ensure_fresh_cache(force_intraday=True)

    now_utc = datetime.now(timezone.utc)
    now_wib = now_utc.astimezone(WIB)
    today_wib_str = now_wib.strftime("%Y-%m-%d")

    with _STORE_LOCK:
        store = load_store()
        changed = False
        bars_cache = _CACHE.get("intraday_bars", {})
        intra_cache = _CACHE.get("intraday_data", {})

        for entry in store["raw_entries"]:
            if entry.get("status") != "OPEN":
                continue

            pair = entry["pair"]
            side = entry["side"].lower()
            entry_px = float(entry["entry"])
            sl_px = float(entry["sl"])
            tp_px = float(entry["tp"])
            created_dt = datetime.fromisoformat(entry["created_at"]).astimezone(timezone.utc)

            basis_offset = 0.0
            last_px = intra_cache.get(pair, {}).get("last_price", 0)
            if pair == "XAUUSD" and last_px > 0:
                diff = entry_px - last_px
                if abs(diff) / last_px < 0.03:
                    basis_offset = diff

            bars = bars_cache.get(pair, [])
            for b in bars:
                b_dt = datetime.fromisoformat(b["ts"]).astimezone(timezone.utc)
                if b_dt < created_dt:
                    continue

                b_high = b["h"] + basis_offset
                b_low = b["l"] + basis_offset

                hit_status = None
                if side == "buy":
                    if b_low <= sl_px:
                        hit_status = "SL_HIT"
                    elif b_high >= tp_px:
                        hit_status = "TP_HIT"
                else:
                    if b_high >= sl_px:
                        hit_status = "SL_HIT"
                    elif b_low <= tp_px:
                        hit_status = "TP_HIT"

                if hit_status:
                    pips = float(entry["tp_pips"]) if hit_status == "TP_HIT" else -float(entry["sl_pips"])
                    usd_pip = float(entry.get("usd_per_pip", 10.0))
                    pnl_usd = round(pips * usd_pip, 2)
                    entry["status"] = hit_status
                    entry["pnl_pips"] = round(pips, 1)
                    entry["pnl_usd"] = pnl_usd
                    entry["hit_at"] = now_utc.isoformat()
                    changed = True
                    break

        for entry in store["raw_entries"]:
            if entry.get("settled"):
                continue
            if entry.get("status") not in ("TP_HIT", "SL_HIT"):
                continue

            ref_iso = entry.get("hit_at") or entry["created_at"]
            ref_wib_date = datetime.fromisoformat(ref_iso).astimezone(WIB).strftime("%Y-%m-%d")

            if ref_wib_date < today_wib_str:
                day_bucket = store["daily_pnl"].setdefault(ref_wib_date, {
                    "pnl_usd": 0.0,
                    "pnl_pips": 0.0,
                    "wins": 0,
                    "losses": 0,
                    "trades": [],
                })
                pnl_usd = float(entry["pnl_usd"])
                pnl_pips = float(entry["pnl_pips"])
                day_bucket["pnl_usd"] = round(day_bucket["pnl_usd"] + pnl_usd, 2)
                day_bucket["pnl_pips"] = round(day_bucket["pnl_pips"] + pnl_pips, 1)
                if entry["status"] == "TP_HIT":
                    day_bucket["wins"] += 1
                else:
                    day_bucket["losses"] += 1
                    store["sl_events"].append({
                        "date": ref_wib_date,
                        "ts": now_utc.isoformat(),
                        "pair": entry["pair"],
                        "pnl_usd": pnl_usd,
                    })

                day_bucket["trades"].append({
                    "pair": entry["pair"],
                    "side": entry["side"].upper(),
                    "rr": entry["rr"],
                    "result": entry["status"],
                    "pnl_pips": pnl_pips,
                    "pnl_usd": pnl_usd,
                })

                ym_prefix = ref_wib_date[:7]
                month_sls = [s for s in store["sl_events"] if s["date"].startswith(ym_prefix)]
                if len(month_sls) >= 3:
                    cd_end = now_utc + timedelta(days=3)
                    existing_cd = store.get("cooldown_until")
                    if not existing_cd or datetime.fromisoformat(existing_cd) < cd_end:
                        store["cooldown_until"] = cd_end.isoformat()

                entry["settled"] = True
                entry["settled_at"] = now_utc.isoformat()
                entry["delete_after"] = (now_utc + timedelta(hours=2)).isoformat()
                changed = True

        remaining_raw = []
        for entry in store["raw_entries"]:
            if entry.get("settled") and entry.get("delete_after"):
                del_dt = datetime.fromisoformat(entry["delete_after"]).astimezone(timezone.utc)
                if now_utc >= del_dt:
                    changed = True
                    continue
            remaining_raw.append(entry)

        if len(remaining_raw) != len(store["raw_entries"]):
            store["raw_entries"] = remaining_raw
            changed = True

        if changed:
            save_store(store)

        return store


def _background_lifecycle_loop():
    while True:
        try:
            ensure_fresh_cache()
            process_portfolio_lifecycle(force_check=False)
        except Exception:
            pass
        time.sleep(180)


threading.Thread(target=_background_lifecycle_loop, daemon=True).start()


def get_zigft_rule_status(store: dict, now_utc: datetime):
    now_wib = now_utc.astimezone(WIB)
    today_wib = now_wib.strftime("%Y-%m-%d")
    ym_wib = now_wib.strftime("%Y-%m")

    today_entries = [
        e for e in store["raw_entries"]
        if datetime.fromisoformat(e["created_at"]).astimezone(WIB).strftime("%Y-%m-%d") == today_wib
    ]
    today_settled_trades = len(store["daily_pnl"].get(today_wib, {}).get("trades", []))
    daily_used = len(today_entries) + today_settled_trades

    perm_month_sl = len([s for s in store["sl_events"] if s["date"].startswith(ym_wib)])
    unsettled_month_sl = len([
        e for e in store["raw_entries"]
        if not e.get("settled") and e.get("status") == "SL_HIT"
        and datetime.fromisoformat(e["created_at"]).astimezone(WIB).strftime("%Y-%m") == ym_wib
    ])
    monthly_sl_used = perm_month_sl + unsettled_month_sl

    cooldown_active = False
    cooldown_until_wib = None
    if store.get("cooldown_until"):
        cd_dt = datetime.fromisoformat(store["cooldown_until"]).astimezone(timezone.utc)
        if now_utc < cd_dt:
            cooldown_active = True
            cooldown_until_wib = cd_dt.astimezone(WIB).strftime("%d %b %Y, %H:%M WIB")

    can_enter = (daily_used < 1) and (monthly_sl_used < 3) and (not cooldown_active)
    block_reason = None
    if cooldown_active:
        block_reason = f"3-Day Cooldown aktif hingga {cooldown_until_wib} (Max 3 SL/bulan tercapai)."
    elif monthly_sl_used >= 3:
        block_reason = "Batas maksimal 3 SL bulan ini telah tercapai."
    elif daily_used >= 1:
        block_reason = "Kuota disiplin 1 Entry/hari sudah terpakai hari ini."

    return {
        "can_enter": can_enter,
        "daily_used": daily_used,
        "daily_max": 1,
        "monthly_sl_used": monthly_sl_used,
        "monthly_sl_max": 3,
        "cooldown_active": cooldown_active,
        "cooldown_until_wib": cooldown_until_wib,
        "block_reason": block_reason,
    }


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "online",
        "service": "ZIGFT Quant & Portfolio Engine",
        "supabase_connected": bool(SUPABASE_URL and SUPABASE_KEY),
        "news_cached_count": len(_CACHE.get("news_events", [])),
        "cache_age_sec": round(time.time() - _CACHE["intraday_ts"], 1) if _CACHE["intraday_ts"] else None,
    })


@app.route("/api/news-radar", methods=["GET"])
def news_radar():
    """Endpoint ringan khusus banner hitung mundur news & prediksi pre-news di bagian atas form."""
    try:
        pair = str(request.args.get("pair", "")).upper().strip()
        with _CACHE_LOCK:
            if (time.time() - _CACHE["news_ts"]) > NEWS_TTL or not _CACHE["news_events"]:
                _CACHE["news_events"] = _refresh_news()
                _CACHE["news_ts"] = time.time()

        now_utc = datetime.now(timezone.utc)
        pred = analyze_pre_news_prediction(pair=pair, now_utc=now_utc, csm_data=_CACHE.get("csm", {}))
        return jsonify({
            "server_now_ms": int(now_utc.timestamp() * 1000),
            "news": pred,
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/quant-analysis", methods=["GET", "POST"])
def quant_analysis():
    try:
        payload = request.get_json(silent=True) or request.args
        pair = str(payload.get("pair", "")).upper().strip()
        side = str(payload.get("side", "buy")).lower().strip()
        entry = float(payload.get("entry", 0))
        sl = float(payload.get("sl", 0))
        tp = float(payload.get("tp", 0))
        sl_pips = float(payload.get("sl_pips", 0))
        tp_pips = float(payload.get("tp_pips", 0))

        if pair not in TF_PAIRS:
            return jsonify({"error": f"Pair {pair} tidak terdaftar di sistem TF 2025"}), 400

        ensure_fresh_cache()
        store = process_portfolio_lifecycle(force_check=False)

        hist = _CACHE["historical_data"].get(pair)
        intra = _CACHE["intraday_data"].get(pair)
        csm = _CACHE["csm"]

        if not hist or not intra:
            return jsonify({"error": "Data pasar sementara tidak tersedia dari feed"}), 503

        pv = TF_PAIRS[pair]["pv"]
        decimals = 1 if pair == "XAUUSD" else (3 if "JPY" in pair else 5)
        now_utc = datetime.now(timezone.utc)
        kz = get_killzone_info(now_utc)
        rules_status = get_zigft_rule_status(store, now_utc)
        usd_per_pip = get_usd_per_pip_1lot(pair)

        basis_offset = 0.0
        if pair == "XAUUSD" and hist["ticker"] == "GC=F" and entry > 0:
            diff = entry - intra["last_price"]
            if abs(diff) / intra["last_price"] < 0.03:
                basis_offset = diff

        pdh = round(hist["pdh"] + basis_offset, decimals)
        pdl = round(hist["pdl"] + basis_offset, decimals)
        pwh = round(hist["pwh"] + basis_offset, decimals)
        pwl = round(hist["pwl"] + basis_offset, decimals)
        asian_high = round(intra["asian_high"] + basis_offset, decimals)
        asian_low = round(intra["asian_low"] + basis_offset, decimals)

        smc_raw = intra.get("smc", {})
        r_high = round(smc_raw.get("range_high", pdh) + basis_offset, decimals)
        r_low = round(smc_raw.get("range_low", pdl) + basis_offset, decimals)
        span = max(r_high - r_low, pv)
        eq_50 = round(r_low + (0.5 * span), decimals)

        if side == "buy":
            fib_618 = round(r_high - (0.618 * span), decimals)
            fib_750 = round(r_high - (0.750 * span), decimals)
            retracement_pct = round(((r_high - entry) / span) * 100.0, 1) if entry > 0 else 50.0
            if 61.8 <= retracement_pct <= 79.0:
                smc_zone, smc_verdict = "OTE_BUY (61.8%–75%)", "OPTIMAL"
                smc_note = f"Entry di zona OTE Discount ({retracement_pct}% retracement dari H4 Swing High)."
            elif 50.0 <= retracement_pct < 61.8:
                smc_zone, smc_verdict = "DISCOUNT_BUY (>50%)", "VALID"
                smc_note = f"Entry berada di bawah Equilibrium 50% ({retracement_pct}% Discount)."
            elif retracement_pct > 79.0:
                smc_zone, smc_verdict = "DEEP_DISCOUNT (>75%)", "STRETCHED"
                smc_note = f"Entry sangat dalam ({retracement_pct}%), waspadai break di bawah {r_low}."
            else:
                smc_zone, smc_verdict = "PREMIUM_CHASING (<50%)", "CHASING"
                smc_note = f"Entry masih di area Premium ({retracement_pct}%), belum masuk diskon 50%–75%."

            idm_swept = bool(smc_raw.get("idm_buy_swept", False))
            idm_lvl = round(smc_raw["idm_buy_lvl"] + basis_offset, decimals) if smc_raw.get("idm_buy_lvl") else None
            idm_note = (
                f"Inducement SSL ({idm_lvl}) sudah tersapu sebelum entry."
                if idm_swept and idm_lvl
                else f"Belum ada sapuan Inducement SSL terbaru{f' (IDM di {idm_lvl})' if idm_lvl else ''}."
            )
        else:
            fib_618 = round(r_low + (0.618 * span), decimals)
            fib_750 = round(r_low + (0.750 * span), decimals)
            retracement_pct = round(((entry - r_low) / span) * 100.0, 1) if entry > 0 else 50.0
            if 61.8 <= retracement_pct <= 79.0:
                smc_zone, smc_verdict = "OTE_SELL (61.8%–75%)", "OPTIMAL"
                smc_note = f"Entry di zona OTE Premium ({retracement_pct}% retracement dari H4 Swing Low)."
            elif 50.0 <= retracement_pct < 61.8:
                smc_zone, smc_verdict = "PREMIUM_SELL (>50%)", "VALID"
                smc_note = f"Entry berada di atas Equilibrium 50% ({retracement_pct}% Premium)."
            elif retracement_pct > 79.0:
                smc_zone, smc_verdict = "DEEP_PREMIUM (>75%)", "STRETCHED"
                smc_note = f"Entry sangat tinggi ({retracement_pct}%), waspadai breakout di atas {r_high}."
            else:
                smc_zone, smc_verdict = "DISCOUNT_CHASING (<50%)", "CHASING"
                smc_note = f"Entry masih di area Discount ({retracement_pct}%), belum masuk premium 50%–75%."

            idm_swept = bool(smc_raw.get("idm_sell_swept", False))
            idm_lvl = round(smc_raw["idm_sell_lvl"] + basis_offset, decimals) if smc_raw.get("idm_sell_lvl") else None
            idm_note = (
                f"Inducement BSL ({idm_lvl}) sudah tersapu sebelum entry."
                if idm_swept and idm_lvl
                else f"Belum ada sapuan Inducement BSL terbaru{f' (IDM di {idm_lvl})' if idm_lvl else ''}."
            )

        sweep_alerts = []
        if sl > 0:
            if side == "buy":
                for lbl, lvl in [("PDL", pdl), ("PWL", pwl), ("Asian Low", asian_low)]:
                    dist_pips = round((lvl - sl) / pv, 1)
                    if -12.0 <= dist_pips <= 22.0:
                        pos_desc = f"{abs(dist_pips)} pips di bawah {lbl}" if dist_pips >= 0 else f"{abs(dist_pips)} pips di atas {lbl}"
                        sweep_alerts.append(f"SL {pos_desc} (zona stop-hunt SSL)")
            else:
                for lbl, lvl in [("PDH", pdh), ("PWH", pwh), ("Asian High", asian_high)]:
                    dist_pips = round((sl - lvl) / pv, 1)
                    if -12.0 <= dist_pips <= 22.0:
                        pos_desc = f"{abs(dist_pips)} pips di atas {lbl}" if dist_pips >= 0 else f"{abs(dist_pips)} pips di bawah {lbl}"
                        sweep_alerts.append(f"SL {pos_desc} (zona stop-hunt BSL)")

        atr_14_pips = hist["atr_14_pips"]
        today_range_pips = intra["today_range_pips"]
        exhaustion_pct = round((today_range_pips / atr_14_pips) * 100.0, 1) if atr_14_pips > 0 else 0.0
        if exhaustion_pct >= 85.0:
            atr_status = "EXHAUSTED"
            atr_note = f"Range hari ini sudah terpakai {exhaustion_pct}% dari rata-rata ATR-14."
        elif tp_pips > (atr_14_pips * 1.25):
            atr_status = "STRETCHED"
            atr_note = f"Target TP ({tp_pips} pips) melampaui 125% ATR-14 ({atr_14_pips} pips)."
        else:
            atr_status = "OPTIMAL"
            atr_note = f"Target TP ({tp_pips} pips) realistis dalam kapasitas ATR-14 ({atr_14_pips} pips)."

        base_ccy, quote_ccy = pair[:3], pair[3:]
        base_str = csm.get(base_ccy, 5.0)
        quote_str = csm.get(quote_ccy, 5.0)
        str_diff = round(base_str - quote_str, 1)

        if side == "buy":
            csm_verdict = "ALIGNED" if str_diff >= 1.2 else ("COUNTER" if str_diff <= -1.2 else "NEUTRAL")
        else:
            csm_verdict = "ALIGNED" if str_diff <= -1.2 else ("COUNTER" if str_diff >= 1.2 else "NEUTRAL")

        if csm_verdict == "ALIGNED":
            stronger = base_ccy if side == "buy" else quote_ccy
            weaker = quote_ccy if side == "buy" else base_ccy
            csm_note = f"{stronger} dominan lebih kuat dari {weaker} ({base_ccy} {base_str} vs {quote_ccy} {quote_str})."
        elif csm_verdict == "COUNTER":
            csm_note = f"Melawan arus momentum H1/H4 ({base_ccy} {base_str} vs {quote_ccy} {quote_str})."
        else:
            csm_note = f"Kekuatan {base_ccy} ({base_str}) & {quote_ccy} ({quote_str}) berimbang."

        sorted_csm = sorted(csm.items(), key=lambda x: x[1], reverse=True)

        dow_data = hist["dow_stats"].get(kz["day_idx"], {})
        sess_lookup = "ASIA" if kz["session_key"] in ["ASIA", "DEAD_ZONE"] else (
            "LONDON" if kz["session_key"] in ["PRE_LONDON", "LONDON", "LONDON_MID"] else "NY"
        )
        sess_data = hist["session_stats"].get(sess_lookup, {})

        news_pred = analyze_pre_news_prediction(
            pair=pair,
            now_utc=now_utc,
            smc_zone=smc_zone,
            csm_data=csm,
        )

        return jsonify({
            "pair": pair,
            "side": side.upper(),
            "feed_source": hist["ticker"],
            "spot_calibrated": basis_offset != 0.0,
            "usd_per_pip_1lot": usd_per_pip,
            "est_sl_usd_1lot": round(-sl_pips * usd_per_pip, 2),
            "est_tp_usd_1lot": round(tp_pips * usd_per_pip, 2),
            "zigft_rules": rules_status,
            "news_prediction": news_pred,
            "smc": {
                "range_high": r_high,
                "range_low": r_low,
                "eq_50": eq_50,
                "fib_618": fib_618,
                "fib_750": fib_750,
                "retracement_pct": retracement_pct,
                "zone": smc_zone,
                "verdict": smc_verdict,
                "note": smc_note,
                "idm_swept": idm_swept,
                "idm_note": idm_note,
            },
            "killzone": kz,
            "liquidity": {
                "pdh": pdh,
                "pdl": pdl,
                "pwh": pwh,
                "pwl": pwl,
                "asian_high": asian_high,
                "asian_low": asian_low,
                "sweep_alerts": sweep_alerts,
            },
            "volatility": {
                "atr_14_pips": atr_14_pips,
                "today_range_pips": today_range_pips,
                "exhaustion_pct": exhaustion_pct,
                "status": atr_status,
                "note": atr_note,
            },
            "strength": {
                "base_ccy": base_ccy,
                "quote_ccy": quote_ccy,
                "base_score": base_str,
                "quote_score": quote_str,
                "verdict": csm_verdict,
                "note": csm_note,
                "ranking": [{"ccy": k, "score": v} for k, v in sorted_csm],
            },
            "seasonality": {
                "day_name": kz["day_name"],
                "day_winrate": dow_data.get("bull_pct" if side == "buy" else "bear_pct", 50.0),
                "day_median_pips": dow_data.get("median_pips", 0),
                "day_samples": dow_data.get("samples", 0),
                "session_name": sess_lookup,
                "session_winrate": sess_data.get("bull_pct" if side == "buy" else "bear_pct", 50.0),
                "session_median_pips": sess_data.get("median_pips", 0),
                "session_samples": sess_data.get("samples", 0),
            },
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/entries", methods=["POST"])
def create_entry():
    try:
        payload = request.get_json(force=True)
        pair = str(payload.get("pair", "")).upper().strip()
        side = str(payload.get("side", "")).lower().strip()
        entry = float(payload.get("entry", 0))
        sl = float(payload.get("sl", 0))
        tp = float(payload.get("tp", 0))
        sl_pips = float(payload.get("sl_pips", 0))
        tp_pips = float(payload.get("tp_pips", 0))
        rr = float(payload.get("rr", 1))
        idm_ok = bool(payload.get("inducement_confirmed", False))
        liq_ok = bool(payload.get("liquidity_confirmed", False))

        if pair not in TF_PAIRS:
            return jsonify({"error": "Pair tidak valid."}), 400
        if not (idm_ok and liq_ok):
            return jsonify({"error": "Syarat wajib ZIGFT: Inducement & Liquidity harus tervalidasi sebelum entry."}), 400

        ensure_fresh_cache()
        store = process_portfolio_lifecycle(force_check=False)
        now_utc = datetime.now(timezone.utc)
        rules_status = get_zigft_rule_status(store, now_utc)

        if not rules_status["can_enter"]:
            return jsonify({"error": rules_status["block_reason"]}), 403

        usd_per_pip = get_usd_per_pip_1lot(pair)
        new_item = {
            "id": str(uuid.uuid4())[:8],
            "pair": pair,
            "side": side,
            "entry": entry,
            "sl": sl,
            "tp": tp,
            "sl_pips": sl_pips,
            "tp_pips": tp_pips,
            "rr": rr,
            "lot_size": 1.0,
            "usd_per_pip": usd_per_pip,
            "created_at": now_utc.isoformat(),
            "created_wib": now_utc.astimezone(WIB).strftime("%d %b %Y, %H:%M WIB"),
            "status": "OPEN",
            "pnl_pips": 0.0,
            "pnl_usd": 0.0,
            "hit_at": None,
            "settled": False,
            "settled_at": None,
            "delete_after": None,
        }

        with _STORE_LOCK:
            fresh_store = load_store()
            fresh_store["raw_entries"].append(new_item)
            save_store(fresh_store)

        return jsonify({
            "message": f"Entry {pair} {side.upper()} (1.00 Lot) berhasil dicatat. Sistem akan mengecek sentuhan TP/SL otomatis & mengolahnya jam 00:00 WIB.",
            "entry": new_item,
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/portfolio", methods=["GET"])
def get_portfolio():
    try:
        force = str(request.args.get("force", "0")) == "1"
        store = process_portfolio_lifecycle(force_check=force)
        now_utc = datetime.now(timezone.utc)
        now_wib = now_utc.astimezone(WIB)
        rules_status = get_zigft_rule_status(store, now_utc)

        next_midnight_wib = (now_wib + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        mins_to_midnight = int((next_midnight_wib - now_wib).total_seconds() // 60)

        sorted_dates = sorted(store["daily_pnl"].keys())
        equity_curve = []
        cum_usd = 0.0
        total_wins = 0
        total_losses = 0

        for d_str in sorted_dates:
            day_data = store["daily_pnl"][d_str]
            d_pnl = float(day_data.get("pnl_usd", 0.0))
            cum_usd = round(cum_usd + d_pnl, 2)
            total_wins += int(day_data.get("wins", 0))
            total_losses += int(day_data.get("losses", 0))
            equity_curve.append({
                "date": d_str,
                "daily_pnl": d_pnl,
                "cumulative_pnl": cum_usd,
                "wins": day_data.get("wins", 0),
                "losses": day_data.get("losses", 0),
            })

        total_closed = total_wins + total_losses
        win_rate = round((total_wins / total_closed) * 100.0, 1) if total_closed > 0 else 0.0

        formatted_raw = []
        for e in reversed(store["raw_entries"]):
            item = dict(e)
            if item.get("settled") and item.get("delete_after"):
                del_wib = datetime.fromisoformat(item["delete_after"]).astimezone(WIB)
                item["delete_after_wib"] = del_wib.strftime("%H:%M WIB")
            formatted_raw.append(item)

        return jsonify({
            "server_wib": now_wib.strftime("%d %b %Y, %H:%M WIB"),
            "checked_at_wib": now_wib.strftime("%H:%M:%S WIB"),
            "next_settlement_wib": f"00:00 WIB ({mins_to_midnight // 60}j {mins_to_midnight % 60}m lagi)",
            "zigft_rules": rules_status,
            "summary": {
                "net_pnl_usd": cum_usd,
                "total_settled_trades": total_closed,
                "wins": total_wins,
                "losses": total_losses,
                "win_rate": win_rate,
            },
            "daily_pnl": store["daily_pnl"],
            "equity_curve": equity_curve,
            "raw_entries": formatted_raw,
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
