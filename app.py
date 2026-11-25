import gc
import os
import time
import threading
from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd
import yfinance as yf
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Konfigurasi Cache (detik) - Bisa diatur lewat Environment Variable Render
INTRADAY_TTL = int(os.environ.get("INTRADAY_TTL", 300))     # 5 menit
HISTORICAL_TTL = int(os.environ.get("HISTORICAL_TTL", 21600)) # 6 jam

WIB = timezone(timedelta(hours=7))

# Aturan resmi 16 Pair TF 2025
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
    "XAUUSD": {"vp": 0.5, "min_sl": 30, "max_sl": 500, "pv": 0.1, "tickers": ["GC=F", "XAUUSD=X"]},
}

HARI_ID = ["Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu", "Minggu"]

_CACHE = {
    "intraday_ts": 0.0,
    "historical_ts": 0.0,
    "intraday_data": {},
    "historical_data": {},
    "csm": {},
}
_CACHE_LOCK = threading.Lock()


def get_killzone_info(now_utc: datetime):
    now_wib = now_utc.astimezone(WIB)
    wd = now_utc.weekday()
    hr_utc = now_utc.hour

    # Cek tutup pasar akhir pekan (Jumat 22:00 UTC s/d Minggu 22:00 UTC)
    is_weekend = (wd == 4 and hr_utc >= 22) or (wd == 5) or (wd == 6 and hr_utc < 22)

    if 0 <= hr_utc < 6:
        session_key = "ASIA"
        zone_name = "Asian Accumulation Range"
        quality = "LOW_VOL"
    elif 6 <= hr_utc < 7:
        session_key = "PRE_LONDON"
        zone_name = "Pre-London Manipulation"
        quality = "TRANSITION"
    elif 7 <= hr_utc < 10:
        session_key = "LONDON"
        zone_name = "London Open Killzone"
        quality = "KILLZONE"
    elif 10 <= hr_utc < 12:
        session_key = "LONDON_MID"
        zone_name = "London-NY Pre-Overlap"
        quality = "MODERATE"
    elif 12 <= hr_utc < 15:
        session_key = "NY"
        zone_name = "New York Open Killzone"
        quality = "KILLZONE"
    elif 15 <= hr_utc < 17:
        session_key = "LONDON_CLOSE"
        zone_name = "London Close Reversal Window"
        quality = "MODERATE"
    elif 17 <= hr_utc < 21:
        session_key = "NY_PM"
        zone_name = "NY Afternoon Session"
        quality = "LOW_VOL"
    else:
        session_key = "DEAD_ZONE"
        zone_name = "Daily Rollover / Dead Zone"
        quality = "SPREAD_RISK"

    return {
        "wib_time": now_wib.strftime("%H:%M WIB"),
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
        threads=False,  # Hemat RAM di container 512MB Render
    )


def _refresh_historical():
    all_tickers = []
    for info in TF_PAIRS.values():
        for t in info["tickers"]:
            if t not in all_tickers:
                all_tickers.append(t)

    df_1d_raw = _download_batch(all_tickers, period="2y", interval="1d")
    df_1h_raw = _download_batch(all_tickers, period="60d", interval="1h")

    hist_out = {}
    for pair, info in TF_PAIRS.items():
        pv = info["pv"]
        df_1d = pd.DataFrame()
        df_1h = pd.DataFrame()
        used_ticker = info["tickers"][0]

        for t in info["tickers"]:
            cand_1d = _extract_ticker_df(df_1d_raw, t)
            if len(cand_1d) >= 20:
                df_1d = cand_1d
                df_1h = _extract_ticker_df(df_1h_raw, t)
                used_ticker = t
                break

        if df_1d.empty or len(df_1d) < 16:
            continue

        # 1. Hitung 14-Day ATR (menggunakan candle harian yang sudah tutup: iloc[:-1])
        closed_1d = df_1d.iloc[:-1].copy()
        prev_close = closed_1d["Close"].shift(1)
        tr1 = closed_1d["High"] - closed_1d["Low"]
        tr2 = (closed_1d["High"] - prev_close).abs()
        tr3 = (closed_1d["Low"] - prev_close).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr_14_val = float(tr.tail(14).mean())
        atr_14_pips = round(atr_14_val / pv, 1)

        # 2. Previous Day High / Low (PDH / PDL) & Previous Week High / Low (PWH / PWL)
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

        # 3. Quantitative Seasonality & Conditional Probability (2 Tahun Daily + 60 Hari Hourly)
        closed_1d["wd"] = closed_1d.index.weekday
        closed_1d["bullish"] = closed_1d["Close"] > closed_1d["Open"]
        closed_1d["range_pips"] = (closed_1d["High"] - closed_1d["Low"]) / pv

        dow_stats = {}
        for wd_i in range(5):
            sub_wd = closed_1d[closed_1d["wd"] == wd_i]
            n_samples = int(len(sub_wd))
            if n_samples > 0:
                bull_pct = round(float(sub_wd["bullish"].mean() * 100), 1)
                bear_pct = round(100.0 - bull_pct, 1)
                med_exp = round(float(sub_wd["range_pips"].median()), 1)
            else:
                bull_pct, bear_pct, med_exp = 50.0, 50.0, atr_14_pips
            dow_stats[wd_i] = {
                "bull_pct": bull_pct,
                "bear_pct": bear_pct,
                "median_pips": med_exp,
                "samples": n_samples,
            }

        session_stats = {}
        if not df_1h.empty:
            df_1h["hr"] = df_1h.index.hour
            df_1h["date_str"] = df_1h.index.strftime("%Y-%m-%d")

            def _map_sess(h):
                if 0 <= h < 6:
                    return "ASIA"
                elif 6 <= h < 12:
                    return "LONDON"
                else:
                    return "NY"

            df_1h["sess"] = df_1h["hr"].apply(_map_sess)
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
                    s_bear = round(100.0 - s_bull, 1)
                    s_med = round(float(sub_s["exp_pips"].median()), 1)
                else:
                    s_bull, s_bear, s_med = 50.0, 50.0, round(atr_14_pips * 0.45, 1)
                session_stats[sk] = {
                    "bull_pct": s_bull,
                    "bear_pct": s_bear,
                    "median_pips": s_med,
                    "samples": n_s,
                }

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
    all_tickers = []
    for info in TF_PAIRS.values():
        for t in info["tickers"]:
            if t not in all_tickers:
                all_tickers.append(t)

    df_15m_raw = _download_batch(all_tickers, period="5d", interval="15m")
    df_1h_raw = _download_batch(all_tickers, period="5d", interval="1h")

    intra_out = {}
    raw_strength = {c: [] for c in ["USD", "EUR", "GBP", "JPY", "AUD", "NZD", "CAD", "CHF", "XAU"]}

    for pair, info in TF_PAIRS.items():
        pv = info["pv"]
        df_15m = pd.DataFrame()
        df_1h = pd.DataFrame()

        for t in info["tickers"]:
            cand_15m = _extract_ticker_df(df_15m_raw, t)
            if not cand_15m.empty:
                df_15m = cand_15m
                df_1h = _extract_ticker_df(df_1h_raw, t)
                break

        if df_15m.empty:
            continue

        last_price = float(df_15m["Close"].iloc[-1])
        latest_date = df_15m.index[-1].date()

        today_df = df_15m[df_15m.index.date == latest_date]
        today_high = float(today_df["High"].max())
        today_low = float(today_df["Low"].min())
        today_range_pips = round((today_high - today_low) / pv, 1)

        # Asian Session High/Low (00:00 - 06:00 UTC pada hari trading terbaru)
        asian_df = today_df[(today_df.index.hour >= 0) & (today_df.index.hour < 6)]
        if not asian_df.empty:
            asian_high = float(asian_df["High"].max())
            asian_low = float(asian_df["Low"].min())
        else:
            asian_high = today_high
            asian_low = today_low

        # Hitung Momentum untuk Currency Strength Meter (H1 4-bar & 24-bar)
        if len(df_1h) >= 2:
            c_now = float(df_1h["Close"].iloc[-1])
            c_4h = float(df_1h["Close"].iloc[-min(5, len(df_1h))])
            c_24h = float(df_1h["Close"].iloc[-min(25, len(df_1h))])
            ret_4h = ((c_now - c_4h) / c_4h) * 100.0 if c_4h != 0 else 0.0
            ret_24h = ((c_now - c_24h) / c_24h) * 100.0 if c_24h != 0 else 0.0
            momentum = (0.6 * ret_4h) + (0.4 * ret_24h)

            base_ccy = pair[:3]
            quote_ccy = pair[3:]
            if base_ccy in raw_strength:
                raw_strength[base_ccy].append(momentum)
            if quote_ccy in raw_strength:
                raw_strength[quote_ccy].append(-momentum)

        intra_out[pair] = {
            "last_price": last_price,
            "today_high": today_high,
            "today_low": today_low,
            "today_range_pips": today_range_pips,
            "asian_high": asian_high,
            "asian_low": asian_low,
        }

    # Normalisasi skor kekuatan 9 mata uang ke skala 0.0 - 10.0
    avg_scores = {}
    for ccy, vals in raw_strength.items():
        avg_scores[ccy] = float(np.mean(vals)) if vals else 0.0

    min_s = min(avg_scores.values()) if avg_scores else 0.0
    max_s = max(avg_scores.values()) if avg_scores else 0.0
    span = max_s - min_s

    csm_normalized = {}
    for ccy, val in avg_scores.items():
        if span > 1e-6:
            norm = round(((val - min_s) / span) * 10.0, 1)
        else:
            norm = 5.0
        csm_normalized[ccy] = norm

    del df_15m_raw, df_1h_raw
    gc.collect()
    return intra_out, csm_normalized


def ensure_fresh_cache():
    now = time.time()
    with _CACHE_LOCK:
        if (now - _CACHE["historical_ts"]) > HISTORICAL_TTL or not _CACHE["historical_data"]:
            _CACHE["historical_data"] = _refresh_historical()
            _CACHE["historical_ts"] = time.time()

        if (now - _CACHE["intraday_ts"]) > INTRADAY_TTL or not _CACHE["intraday_data"]:
            intra, csm = _refresh_intraday()
            _CACHE["intraday_data"] = intra
            _CACHE["csm"] = csm
            _CACHE["intraday_ts"] = time.time()


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "online",
        "service": "ZIGFT Quant Engine",
        "pairs_supported": len(TF_PAIRS),
        "cache_age_sec": round(time.time() - _CACHE["intraday_ts"], 1) if _CACHE["intraday_ts"] else None,
    })


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
        rr = float(payload.get("rr", 1))

        if pair not in TF_PAIRS:
            return jsonify({"error": f"Pair {pair} tidak terdaftar di sistem TF 2025"}), 400

        ensure_fresh_cache()

        hist = _CACHE["historical_data"].get(pair)
        intra = _CACHE["intraday_data"].get(pair)
        csm = _CACHE["csm"]

        if not hist or not intra:
            return jsonify({"error": "Data pasar sementara tidak tersedia dari feed"}), 503

        pv = TF_PAIRS[pair]["pv"]
        decimals = 1 if pair == "XAUUSD" else (3 if "JPY" in pair else 5)
        now_utc = datetime.now(timezone.utc)
        kz = get_killzone_info(now_utc)

        # Kalibrasi basis Futures vs Spot khusus jika GC=F dipakai untuk XAUUSD
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

        # 1. Analisis Sweep Risk Likuiditas terhadap titik SL Anda
        sweep_alerts = []
        if sl > 0:
            if side == "buy":
                # SL berada di bawah entry -> rawan tersapu jika berada tepat di bawah SSL (PDL/PWL/Asian Low)
                for lbl, lvl in [("PDL", pdl), ("PWL", pwl), ("Asian Low", asian_low)]:
                    dist_pips = round((lvl - sl) / pv, 1)  # positif berarti SL di bawah level likuiditas
                    if -12.0 <= dist_pips <= 22.0:
                        pos_desc = f"{abs(dist_pips)} pips di bawah {lbl}" if dist_pips >= 0 else f"{abs(dist_pips)} pips di atas {lbl}"
                        sweep_alerts.append(f"SL {pos_desc} (zona stop-hunt SSL)")
            else:
                # SL berada di atas entry -> rawan tersapu jika berada tepat di atas BSL (PDH/PWH/Asian High)
                for lbl, lvl in [("PDH", pdh), ("PWH", pwh), ("Asian High", asian_high)]:
                    dist_pips = round((sl - lvl) / pv, 1)  # positif berarti SL di atas level likuiditas
                    if -12.0 <= dist_pips <= 22.0:
                        pos_desc = f"{abs(dist_pips)} pips di atas {lbl}" if dist_pips >= 0 else f"{abs(dist_pips)} pips di bawah {lbl}"
                        sweep_alerts.append(f"SL {pos_desc} (zona stop-hunt BSL)")

        # 2. Dynamic ATR(14) & Daily Range Exhaustion
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

        # 3. Currency Strength Meter (Base vs Quote)
        base_ccy = pair[:3]
        quote_ccy = pair[3:]
        base_str = csm.get(base_ccy, 5.0)
        quote_str = csm.get(quote_ccy, 5.0)
        str_diff = round(base_str - quote_str, 1)

        if side == "buy":
            if str_diff >= 1.2:
                csm_verdict = "ALIGNED"
                csm_note = f"{base_ccy} ({base_str}) dominan lebih kuat dari {quote_ccy} ({quote_str})"
            elif str_diff <= -1.2:
                csm_verdict = "COUNTER"
                csm_note = f"Melawan arus: {quote_ccy} ({quote_str}) lebih kuat dari {base_ccy} ({base_str})"
            else:
                csm_verdict = "NEUTRAL"
                csm_note = f"Kekuatan {base_ccy} ({base_str}) & {quote_ccy} ({quote_str}) berimbang"
        else:
            if str_diff <= -1.2:
                csm_verdict = "ALIGNED"
                csm_note = f"{quote_ccy} ({quote_str}) dominan lebih kuat dari {base_ccy} ({base_str})"
            elif str_diff >= 1.2:
                csm_verdict = "COUNTER"
                csm_note = f"Melawan arus: {base_ccy} ({base_str}) lebih kuat dari {quote_ccy} ({quote_str})"
            else:
                csm_verdict = "NEUTRAL"
                csm_note = f"Kekuatan {base_ccy} ({base_str}) & {quote_ccy} ({quote_str}) berimbang"

        # Leaderboard top 3 & bottom 2
        sorted_csm = sorted(csm.items(), key=lambda x: x[1], reverse=True)

        # 4. Quantitative Seasonality & Conditional Probability
        dow_data = hist["dow_stats"].get(kz["day_idx"], {})
        sess_lookup = "ASIA" if kz["session_key"] in ["ASIA", "DEAD_ZONE"] else (
            "LONDON" if kz["session_key"] in ["PRE_LONDON", "LONDON", "LONDON_MID"] else "NY"
        )
        sess_data = hist["session_stats"].get(sess_lookup, {})

        dow_winrate = dow_data.get("bull_pct" if side == "buy" else "bear_pct", 50.0)
        sess_winrate = sess_data.get("bull_pct" if side == "buy" else "bear_pct", 50.0)

        return jsonify({
            "pair": pair,
            "side": side.upper(),
            "feed_source": hist["ticker"],
            "spot_calibrated": basis_offset != 0.0,
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
                "day_winrate": dow_winrate,
                "day_median_pips": dow_data.get("median_pips", 0),
                "day_samples": dow_data.get("samples", 0),
                "session_name": sess_lookup,
                "session_winrate": sess_winrate,
                "session_median_pips": sess_data.get("median_pips", 0),
                "session_samples": sess_data.get("samples", 0),
            },
        })

    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
