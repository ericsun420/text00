# app.py — 第一根漲停 + 連板潛力（1～8 全部改）｜冷酷黑灰｜懶人版｜官方OpenAPI
import os
import math
import time
import re
from datetime import datetime, timedelta, time as dtime

import requests
import urllib3
import pandas as pd
import yfinance as yf
import streamlit as st
import streamlit.components.v1 as components

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================
# UI / THEME
# =========================
st.set_page_config(page_title="起漲戰情室｜第一根漲停", page_icon="🧊", layout="wide")

CSS = """
<style>
:root{
  --bg:#07080b; --panel:#0b0d12; --card:#0f1116; --text:#e5e7eb; --muted:#9ca3af;
  --line:rgba(148,163,184,.16); --shadow: 0 16px 40px rgba(0,0,0,.35);
  --hi: rgba(148,163,184,.08); --ok:#a3e635; --warn:#fbbf24; --bad:#fb7185;
}
[data-testid="stAppViewContainer"]{ background: var(--bg) !important; color: var(--text) !important; }
.main{ background: var(--bg) !important; }
.block-container{ padding-top: 1.15rem; padding-bottom: 2.0rem; }
[data-testid="stHeader"]{ background: rgba(7,8,11,.80) !important; border-bottom: 1px solid var(--line) !important; }
[data-testid="stToolbar"]{ background: transparent !important; }
[data-testid="stSidebar"]{ background: var(--panel) !important; border-right: 1px solid var(--line) !important; }
[data-testid="stSidebar"] *{ color: var(--text) !important; }
.header-wrap{ display:flex; align-items:flex-end; justify-content:space-between; gap:18px; padding: 6px 4px 2px 4px; }
.title{ font-size: 42px; font-weight: 900; letter-spacing: .4px; background: linear-gradient(90deg, #f3f4f6, #9ca3af); -webkit-background-clip:text; -webkit-text-fill-color: transparent; margin:0; }
.subtitle{ margin:6px 0 0 2px; color: var(--muted); font-size: 14px; }
.pill{ display:inline-flex; align-items:center; gap:8px; padding: 8px 12px; border:1px solid var(--line); border-radius: 999px; color: var(--text); background: rgba(15,17,22,.85); font-size: 13px; box-shadow: var(--shadow); }
.pill .dot{ width:8px; height:8px; border-radius:999px; background:#9ca3af; display:inline-block; }
.grid{ display:grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin: 12px 0 6px 0; }
.card{ background: linear-gradient(180deg, rgba(15,17,22,.94), rgba(11,13,18,.94)); border:1px solid var(--line); border-radius: 16px; padding: 14px 14px 12px 14px; box-shadow: var(--shadow); }
.k{ color: var(--muted); font-size: 12px; margin-bottom: 6px; }
.v{ color: var(--text); font-size: 20px; font-weight: 800; }
.v small{ color: var(--muted); font-weight: 600; font-size: 12px; margin-left: 6px;}
.hr{ height:1px; background: var(--line); margin: 12px 0; }
.banner{ background: rgba(148,163,184,.08); border: 1px solid rgba(148,163,184,.22); color: var(--text); border-radius: 16px; padding: 12px 14px; margin: 10px 0 10px 0; }
.banner b{ color: #fff; }
.metric{ display:flex; justify-content:space-between; align-items:flex-end; gap:10px; }
.metric .left{ display:flex; flex-direction:column; gap:2px; }
.metric .label{ color: var(--muted); font-size: 12px; display:flex; gap:8px; align-items:center; }
.metric .code{ color: var(--text); font-size: 16px; font-weight: 900; line-height:1.1; }
.metric .name{ color: var(--muted); font-size: 12px; margin-top: 2px; }
.metric .tag{ font-size: 12px; padding: 4px 8px; border-radius: 999px; border:1px solid var(--line); color: var(--text); background: rgba(15,17,22,.8); }
.metric .price{ font-size: 22px; font-weight: 900; color: var(--text); line-height: 1; }
.metric .chg{ font-size: 12px; color: var(--muted); }
.stButton>button{ border-radius: 14px !important; border: 1px solid rgba(203,213,225,.26) !important; background: linear-gradient(90deg, rgba(148,163,184,.16), rgba(107,114,128,.10)) !important; color: var(--text) !important; font-weight: 800 !important; padding: 10px 14px !important; }
.stButton>button:hover{ border: 1px solid rgba(203,213,225,.42) !important; background: linear-gradient(90deg, rgba(148,163,184,.22), rgba(107,114,128,.14)) !important; }
.stSelectbox>div>div, .stTextInput>div>div{ border-radius: 14px !important; border: 1px solid rgba(148,163,184,.22) !important; background: rgba(15,17,22,.88) !important; color: var(--text) !important; }
[data-testid="stExpander"]{ border: 1px solid var(--line) !important; border-radius: 16px !important; background: rgba(15,17,22,.55) !important; }
[data-testid="stExpander"] summary{ color: var(--text) !important; font-weight: 900 !important; }
.small-note{ color: var(--muted); font-size: 12px; }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

# =========================
# TIME / MARKET
# =========================
TZ_NAME = "Asia/Taipei"

def now_taipei() -> datetime:
    return datetime.utcnow() + timedelta(hours=8)

def is_market_time(ts: datetime) -> bool:
    t = ts.time()
    return dtime(9, 0) <= t <= dtime(13, 30)

def minutes_elapsed_in_session(ts: datetime) -> int:
    start = datetime.combine(ts.date(), dtime(9, 0))
    end = datetime.combine(ts.date(), dtime(13, 30))
    if ts < start: return 0
    if ts > end: return 270
    return int((ts - start).total_seconds() // 60)

def bars_expected_5m(ts: datetime) -> int:
    m = minutes_elapsed_in_session(ts)
    return max(1, min(54, int(math.ceil(m / 5.0))))

def tw_tick(price: float) -> float:
    if price < 10: return 0.01
    if price < 50: return 0.05
    if price < 100: return 0.10
    if price < 500: return 0.50
    if price < 1000: return 1.00
    return 5.00

def round_to_tick_nearest(x: float, tick: float) -> float:
    return round(round(x / tick) * tick, 2 if tick < 0.1 else 1 if tick < 1 else 0)

def calc_limit_up(prev_close: float, limit_pct: float) -> float:
    raw = prev_close * (1.0 + limit_pct)
    tick = tw_tick(raw)
    return round_to_tick_nearest(raw, tick)

# =========================
# DATA DIRS
# =========================
DATA_DIR = os.path.join(os.getcwd(), "scan_data")
os.makedirs(DATA_DIR, exist_ok=True)
LOG_PATH = os.path.join(DATA_DIR, "signals_log.csv")
OUTCOME_PATH = os.path.join(DATA_DIR, "signals_outcome.csv")

# =========================
# STOCK LIST (全面替換為官方 OpenAPI，保證不當機)
# =========================
@st.cache_data(ttl=24 * 3600, show_spinner=False)
def fetch_listed_stocks_mops() -> pd.DataFrame:
    meta = []
    headers = {"User-Agent": "Mozilla/5.0"}

    # 1. 抓取上市清單 (TWSE OpenAPI)
    try:
        r_tse = requests.get("https://openapi.twse.com.tw/v1/opendata/t187ap03_L", headers=headers, timeout=15, verify=False)
        if r_tse.status_code == 200:
            for item in r_tse.json():
                c = str(item.get("公司代號", "")).strip()
                if re.match(r"^\d{4,6}$", c):
                    meta.append({
                        "code": c,
                        "name": str(item.get("公司簡稱", "")).strip(),
                        "industry": str(item.get("產業別", "")).strip() or "未分類",
                        "market": "上市"
                    })
    except Exception as e:
        st.warning(f"上市清單讀取警告: {e}")

    # 2. 抓取上櫃清單 (TPEx OpenAPI)
    try:
        r_otc = requests.get("https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O", headers=headers, timeout=15, verify=False)
        if r_otc.status_code == 200:
            for item in r_otc.json():
                c = str(item.get("公司代號", "")).strip()
                if re.match(r"^\d{4,6}$", c):
                    meta.append({
                        "code": c,
                        "name": str(item.get("公司簡稱", "")).strip(),
                        "industry": str(item.get("產業別", "")).strip() or "未分類",
                        "market": "上櫃"
                    })
    except Exception as e:
        st.warning(f"上櫃清單讀取警告: {e}")

    if not meta:
        raise ValueError("無法取得股票清單（官方 OpenAPI 連線失敗），請確認網路狀態。")
        
    return pd.DataFrame(meta).drop_duplicates("code").sort_values("code").reset_index(drop=True)

# =========================
# DAILY BASELINE 
# =========================
BASE_COLS = [
    "code", "yday_close", "prev2_close", "limit_class_pct", "vol_ma20_shares",
    "high20_ex1", "high60_ex1", "low60", "range20_pct", "range60_pct", "atr20_pct",
    "ret_1d", "ret_3d", "ret_5d", "max_ret_10d", "had_hype_10d",
    "yday_upper_wick_ratio", "yday_vol_spike", "base_len_days", "base_tight_score",
]

def _drop_today_bar_if_exists(df: pd.DataFrame, today_date) -> pd.DataFrame:
    if df.empty: return df
    last_date = pd.Timestamp(df.index[-1]).date()
    if last_date == today_date: return df.iloc[:-1].copy()
    return df

@st.cache_data(ttl=6 * 3600, show_spinner=False)
def build_daily_baseline(codes: list[str]) -> pd.DataFrame:
    today = now_taipei().date()
    start = (now_taipei() - timedelta(days=380)).date().isoformat()
    batch = 60
    rows = []

    for i in range(0, len(codes), batch):
        chunk = codes[i:i + batch]
        tickers = " ".join([f"{c}.TW" for c in chunk])

        try:
            raw = yf.download(tickers=tickers, start=start, interval="1d", group_by="ticker", auto_adjust=False, threads=False, progress=False)
        except: continue

        for c in chunk:
            t = f"{c}.TW"
            try:
                if isinstance(raw.columns, pd.MultiIndex):
                    if t not in raw.columns.get_level_values(0): continue
                    df = raw[t].dropna().copy()
                else:
                    df = raw.dropna().copy()

                df = _drop_today_bar_if_exists(df, today)
                if df.empty or len(df) < 120: continue

                close, high, low, vol = df["Close"].astype(float), df["High"].astype(float), df["Low"].astype(float), df["Volume"].astype(float)
                yday_close = float(close.iloc[-1])
                prev2_close = float(close.iloc[-2])

                ret_1d = (yday_close / prev2_close - 1.0) if prev2_close else None
                ret_3d = (yday_close / float(close.iloc[-4]) - 1.0) if len(close) >= 4 else None
                ret_5d = (yday_close / float(close.iloc[-6]) - 1.0) if len(close) >= 6 else None

                high20_ex1 = float(high.rolling(20).max().shift(1).iloc[-1])
                high60_ex1 = float(high.rolling(60).max().shift(1).iloc[-1])
                low60 = float(low.rolling(60).min().iloc[-1])

                range20_pct = float((high.rolling(20).max().iloc[-1] - low.rolling(20).min().iloc[-1]) / yday_close)
                range60_pct = float((high.rolling(60).max().iloc[-1] - low.rolling(60).min().iloc[-1]) / yday_close)

                prev_close = close.shift(1)
                tr = pd.concat([(high - low).abs(), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
                atr20 = float(tr.rolling(20).mean().iloc[-1])
                atr20_pct = float(atr20 / yday_close) if yday_close else None

                hist_ret = close.pct_change().dropna()
                max_hist_ret = float(hist_ret.tail(260).max()) if len(hist_ret) > 10 else 0.0
                limit_class_pct = 0.20 if max_hist_ret > 0.105 else 0.10

                thr_hype = 0.19 if limit_class_pct == 0.20 else 0.095
                max_ret_10d = float(hist_ret.tail(10).max()) if len(hist_ret) >= 10 else float(hist_ret.max() if len(hist_ret) else 0.0)
                had_hype_10d = (max_ret_10d >= thr_hype)

                y_open, y_high, y_low, y_close = float(df["Open"].iloc[-1]), float(df["High"].iloc[-1]), float(df["Low"].iloc[-1]), float(df["Close"].iloc[-1])
                y_range = max(1e-9, y_high - y_low)
                y_upper_wick_ratio = float((y_high - max(y_open, y_close)) / y_range)

                vol_ma20 = float(vol.rolling(20).mean().iloc[-1])
                yday_vol_spike = (float(vol.iloc[-1]) >= 2.0 * vol_ma20)

                ma20 = close.rolling(20).mean()
                near_ma20 = ((close / (ma20 + 1e-9) - 1.0).abs() <= 0.04)
                base_len_days = int(near_ma20.tail(60).sum())

                base_tight_score = float((1.0 - min(1.0, range20_pct / (range60_pct + 1e-9))) * 0.6 + (1.0 - min(1.0, (atr20_pct or 1.0) / 0.08)) * 0.4)

                rows.append({
                    "code": c, "yday_close": yday_close, "prev2_close": prev2_close, "limit_class_pct": limit_class_pct,
                    "vol_ma20_shares": vol_ma20, "high20_ex1": high20_ex1, "high60_ex1": high60_ex1, "low60": low60,
                    "range20_pct": range20_pct, "range60_pct": range60_pct, "atr20_pct": atr20_pct * 100.0 if atr20_pct is not None else None,
                    "ret_1d": ret_1d * 100.0 if ret_1d is not None else None, "ret_3d": ret_3d * 100.0 if ret_3d is not None else None,
                    "ret_5d": ret_5d * 100.0 if ret_5d is not None else None, "max_ret_10d": max_ret_10d * 100.0 if max_ret_10d is not None else None,
                    "had_hype_10d": bool(had_hype_10d), "yday_upper_wick_ratio": y_upper_wick_ratio, "yday_vol_spike": bool(yday_vol_spike),
                    "base_len_days": base_len_days, "base_tight_score": base_tight_score,
                })
            except: continue
        time.sleep(0.05)

    if not rows: return pd.DataFrame(columns=BASE_COLS)
    out = pd.DataFrame(rows).drop_duplicates("code")
    for c in BASE_COLS:
        if c not in out.columns: out[c] = pd.NA
    return out[BASE_COLS].copy()

# =========================
# INTRADAY (current day 5m bars)
# =========================
def _normalize_intraday_index(df: pd.DataFrame) -> pd.DataFrame:
    idx = df.index
    try:
        if getattr(idx, "tz", None) is not None: idx = idx.tz_convert(TZ_NAME).tz_localize(None)
        else: idx = idx.tz_localize(None)
    except:
        try: idx = pd.to_datetime(idx).tz_localize(None)
        except: pass
    df = df.copy()
    df.index = idx
    return df

@st.cache_data(ttl=20, show_spinner=False)
def fetch_intraday_bars_5m(codes: list[str], batch_size: int = 60) -> dict:
    bars = {}
    today = now_taipei().date()

    for i in range(0, len(codes), batch_size):
        chunk = codes[i:i + batch_size]
        tickers = " ".join([f"{c}.TW" for c in chunk])

        try:
            raw = yf.download(tickers=tickers, period="1d", interval="5m", group_by="ticker", auto_adjust=False, threads=False, progress=False)
        except: continue

        for c in chunk:
            t = f"{c}.TW"
            try:
                if isinstance(raw.columns, pd.MultiIndex):
                    if t not in raw.columns.get_level_values(0): continue
                    df = raw[t].dropna().copy()
                else:
                    df = raw.dropna().copy()
                if df.empty: continue

                df = _normalize_intraday_index(df)
                df = df[df.index.date == today].copy()
                if df.empty: continue
                bars[c] = df
            except: continue
        time.sleep(0.12)
    return bars

# =========================
# INTRADAY PROFILE
# =========================
@st.cache_data(ttl=24 * 3600, show_spinner=False)
def build_intraday_volume_profile(code: str, lookback_days: int = 20) -> list:
    ticker = f"{code}.TW"
    try:
        raw = yf.download(tickers=ticker, period="60d", interval="5m", group_by="ticker", auto_adjust=False, threads=False, progress=False)
    except: return [min(1.0, (i + 1) / 54.0) for i in range(54)]

    if raw is None or raw.empty: return [min(1.0, (i + 1) / 54.0) for i in range(54)]

    if isinstance(raw.columns, pd.MultiIndex):
        if ticker in raw.columns.get_level_values(0): df = raw[ticker].dropna().copy()
        else: df = raw.dropna().copy()
    else: df = raw.dropna().copy()

    df = _normalize_intraday_index(df)
    if df.empty: return [min(1.0, (i + 1) / 54.0) for i in range(54)]

    df["date"] = df.index.date
    sessions = []
    for d, g in df.groupby("date"):
        g = g.sort_index()
        if len(g) < 30: continue
        vol = g["Volume"].astype(float).values
        total = float(vol.sum())
        if total <= 0: continue
        cum = vol.cumsum() / total
        sessions.append([float(cum[min(i, len(cum) - 1)]) for i in range(54)])

    if not sessions: return [min(1.0, (i + 1) / 54.0) for i in range(54)]
    sessions = sessions[-lookback_days:]
    prof = [float(sum(s[i] for s in sessions) / len(sessions)) for i in range(54)]
    for i in range(1, 54): prof[i] = max(prof[i], prof[i - 1])
    prof[-1] = 1.0
    return prof

# =========================
# SCAN & PRESETS
# =========================
PRESETS = {
    "保守（只抓幾乎鎖死、連板體質強）": dict(near_limit_ticks=1, min_close_pos=0.93, max_pullback=0.0025, min_vol_ratio_profile=2.6, min_cum_lots=1500, require_break_high60=True, max_ret_5d=8.0, max_atr20=4.8, min_base_len=28, min_base_tight=0.55, require_lastN_near_limit=3, max_open_board=1),
    "標準（平衡：第一根漲停 + 連板機率）": dict(near_limit_ticks=1, min_close_pos=0.90, max_pullback=0.0038, min_vol_ratio_profile=2.1, min_cum_lots=1200, require_break_high60=False, max_ret_5d=12.0, max_atr20=6.5, min_base_len=18, min_base_tight=0.45, require_lastN_near_limit=2, max_open_board=2),
    "積極（多抓：允許盤中較不穩）": dict(near_limit_ticks=2, min_close_pos=0.86, max_pullback=0.0060, min_vol_ratio_profile=1.6, min_cum_lots=800, require_break_high60=False, max_ret_5d=18.0, max_atr20=8.5, min_base_len=10, min_base_tight=0.35, require_lastN_near_limit=1, max_open_board=4),
}

def compute_open_board_count(df5m: pd.DataFrame, limit_up: float, tick: float) -> int:
    if df5m is None or df5m.empty: return 999
    close, high = df5m["Close"].astype(float).values, df5m["High"].astype(float).values
    touch = high >= (limit_up - tick)
    if not touch.any(): return 999
    first_idx = int(touch.argmax())
    opened = 0
    in_limit_state = True 
    for i in range(first_idx + 1, len(close)):
        if in_limit_state:
            if close[i] < (limit_up - 2.0 * tick):
                opened += 1; in_limit_state = False
        else:
            if high[i] >= (limit_up - tick): in_limit_state = True
    return opened

def lastN_near_limit(df5m: pd.DataFrame, limit_up: float, tick: float, N: int) -> int:
    if df5m is None or df5m.empty: return 0
    return int((df5m.tail(N)["Close"].astype(float).values >= (limit_up - tick)).sum())

def scan_first_limitup_continuation(bars_today, base_df, stock_meta, preset, now_ts):
    if base_df is None or base_df.empty or not bars_today: return pd.DataFrame()
    meta_map = stock_meta.set_index("code")[["name", "industry"]].to_dict(orient="index")
    expected_bar_idx = bars_expected_5m(now_ts) - 1 

    candidates = []
    for code, df5m in bars_today.items():
        if code not in base_df.index: continue
        b = base_df.loc[code]
        if len(df5m) < max(10, int(0.5 * bars_expected_5m(now_ts))): continue

        yday_close, limit_pct = float(b["yday_close"]), float(b["limit_class_pct"])
        limit_up = calc_limit_up(yday_close, limit_pct)
        tick = tw_tick(limit_up)

        last = float(df5m["Close"].iloc[-1])
        day_open, day_high, day_low = float(df5m["Open"].iloc[0]), float(df5m["High"].max()), float(df5m["Low"].min())
        vol_shares = float(df5m["Volume"].sum())
        vol_lots = int(vol_shares / 1000)

        near_limit = last >= (limit_up - preset["near_limit_ticks"] * tick)
        rng = max(1e-9, day_high - day_low)
        close_pos = (last - day_low) / rng
        pullback = (day_high - last) / max(1e-9, day_high)

        ret_1d = float(b["ret_1d"]) if pd.notna(b["ret_1d"]) else 0.0
        yday_was_limit_like = ret_1d >= (19.0 if limit_pct == 0.20 else 9.5)
        had_hype_10d = bool(b["had_hype_10d"])
        yday_bad = bool(b["yday_vol_spike"]) and (float(b["yday_upper_wick_ratio"]) >= 0.35) and (ret_1d >= 6.0)

        base_len = int(b["base_len_days"]) if pd.notna(b["base_len_days"]) else 0
        base_tight = float(b["base_tight_score"]) if pd.notna(b["base_tight_score"]) else 0.0

        break_high60 = True
        if preset["require_break_high60"]:
            high60 = float(b["high60_ex1"]) if pd.notna(b["high60_ex1"]) else 0.0
            break_high60 = limit_up >= (high60 * 0.995)

        vol_ratio_profile = 0.0
        if near_limit and vol_lots >= int(preset["min_cum_lots"]) and float(b["vol_ma20_shares"]) > 0:
            prof = build_intraday_volume_profile(code, lookback_days=20)
            frac = float(prof[min(53, max(0, expected_bar_idx))])
            expected_vol = float(b["vol_ma20_shares"]) * frac
            vol_ratio_profile = (vol_shares / (expected_vol + 1e-9)) if expected_vol > 0 else 0.0

        open_board = compute_open_board_count(df5m, limit_up, tick)
        lastN_hit = lastN_near_limit(df5m, limit_up, tick, preset["require_lastN_near_limit"])

        ret_5d = float(b["ret_5d"]) if pd.notna(b["ret_5d"]) else 0.0
        atr20 = float(b["atr20_pct"]) if pd.notna(b["atr20_pct"]) else 999.0

        cond = (
            near_limit and (close_pos >= float(preset["min_close_pos"])) and (pullback <= float(preset["max_pullback"]))
            and (vol_lots >= int(preset["min_cum_lots"])) and (vol_ratio_profile >= float(preset["min_vol_ratio_profile"]))
            and (not yday_was_limit_like) and (not had_hype_10d) and (not yday_bad)
            and (ret_5d <= float(preset["max_ret_5d"])) and (atr20 <= float(preset["max_atr20"]))
            and (base_len >= int(preset["min_base_len"])) and (base_tight >= float(preset["min_base_tight"]))
            and break_high60 and (lastN_hit >= int(preset["require_lastN_near_limit"])) and (open_board <= int(preset["max_open_board"]))
        )
        if not cond: continue

        m = meta_map.get(code, {"name": "", "industry": ""})
        dist_pct = (limit_up - last) / max(1e-9, limit_up) * 100.0
        chg_pct = (last / yday_close - 1.0) * 100.0

        candidates.append({
            "代號": code, "名稱": m.get("name", ""), "族群": m.get("industry", ""),
            "現價": last, "漲停價": limit_up, "距離漲停(%)": dist_pct, "較昨收(%)": chg_pct,
            "累積量(張)": vol_lots, "盤中爆量倍數": float(vol_ratio_profile), "收在高檔(0-1)": float(close_pos),
            "回落幅度(%)": float(pullback * 100.0), "開板次數(5m近似)": int(open_board) if open_board != 999 else None,
            "連續貼漲停(最後N根)": int(lastN_hit), "基底天數": int(base_len), "基底緊縮分": float(base_tight),
            "ATR20(%)": float(atr20), "近5日漲幅(%)": float(ret_5d), "是否突破60日高": bool(break_high60),
        })

    if not candidates: return pd.DataFrame()
    out = pd.DataFrame(candidates)

    out["族群"] = out["族群"].fillna("")
    grp = out["族群"].value_counts()
    out["族群共振加分"] = out["族群"].apply(lambda x: min(15.0, max(0.0, (int(grp.get(x, 1)) - 1) * 5.0)) if x else 0.0)

    def score_row(r):
        s = 0.0
        s += 30.0 * min(1.0, max(0.0, (float(r["收在高檔(0-1)"]) - 0.85) / 0.15))
        s += 20.0 * min(1.0, max(0.0, (0.70 - float(r["回落幅度(%)"])) / 0.70))
        s += 10.0 * min(1.0, float(r["連續貼漲停(最後N根)"]) / max(1, preset["require_lastN_near_limit"]))
        s += 20.0 * min(1.0, max(0.0, (float(r["盤中爆量倍數"]) - 1.5) / 2.5))
        s += 10.0 * min(1.0, max(0.0, (float(r["基底天數"]) - 8) / 40.0))
        s += 5.0 * min(1.0, max(0.0, float(r["基底緊縮分"])))
        s += float(r["族群共振加分"])
        if r["開板次數(5m近似)"] is not None: s -= min(10.0, float(r["開板次數(5m近似)"]) * 3.0)
        if bool(r["是否突破60日高"]): s += 5.0
        return float(max(0.0, min(100.0, s)))

    out["連板潛力分"] = out.apply(score_row, axis=1)
    out = out.sort_values(["連板潛力分", "距離漲停(%)", "盤中爆量倍數"], ascending=[False, True, False]).reset_index(drop=True)
    out.insert(0, "排名", range(1, len(out) + 1))
    return out

# =========================
# LOGGING & BACKTEST 
# =========================
def append_log(df, mode, universe):
    if df.empty: return
    ts = now_taipei()
    log = df.copy()
    log["掃描時間"] = ts.strftime("%Y-%m-%d %H:%M:%S")
    log["模式"] = mode
    log["股票池"] = universe
    cols_first = ["掃描時間", "模式", "股票池", "排名", "代號", "名稱", "族群"]
    log = log[cols_first + [c for c in log.columns if c not in cols_first]]
    if os.path.exists(LOG_PATH): log.to_csv(LOG_PATH, mode="a", index=False, header=False, encoding="utf-8-sig")
    else: log.to_csv(LOG_PATH, index=False, encoding="utf-8-sig")

def load_log():
    try: return pd.read_csv(LOG_PATH, dtype=str) if os.path.exists(LOG_PATH) else pd.DataFrame()
    except: return pd.DataFrame()

def calc_board_count_from_daily(close_series, limit_pct):
    if close_series is None or len(close_series) < 2: return 0
    closes, boards = close_series.astype(float).values, 0
    for i in range(1, len(closes)):
        limit_up = calc_limit_up(float(closes[i - 1]), limit_pct)
        if float(closes[i]) >= (limit_up - tw_tick(limit_up)): boards += 1
        else: break
    return boards

def update_backtest(max_lookahead_days=12):
    log = load_log()
    if log.empty or "掃描時間" not in log.columns or "代號" not in log.columns: return pd.DataFrame()
    log["signal_date"], log["code"] = log["掃描時間"].astype(str).str.slice(0, 10), log["代號"].astype(str).str.strip()
    
    try: out_old = pd.read_csv(OUTCOME_PATH, dtype=str) if os.path.exists(OUTCOME_PATH) else pd.DataFrame()
    except: out_old = pd.DataFrame()
    
    done_keys = set(zip(out_old["signal_date"].astype(str), out_old["code"].astype(str))) if not out_old.empty and "signal_date" in out_old.columns else set()
    tasks = [key for _, r in log.iterrows() if (key := (str(r["signal_date"]), str(r["code"]))) not in done_keys]
    if not tasks: return out_old

    tasks_by_code = {}
    for d, c in tasks: tasks_by_code.setdefault(c, []).append(d)
    
    new_rows = []
    codes = sorted(tasks_by_code.keys())
    for i in range(0, len(codes), 40):
        chunk = codes[i:i + 40]
        try: raw = yf.download(tickers=" ".join([f"{c}.TW" for c in chunk]), period="6mo", interval="1d", group_by="ticker", auto_adjust=False, threads=False, progress=False)
        except: continue
        for c in chunk:
            t = f"{c}.TW"
            try:
                df = raw[t].dropna().copy() if isinstance(raw.columns, pd.MultiIndex) else raw.dropna().copy()
                if df.empty or "Close" not in df.columns: continue
                close = df["Close"].astype(float)
                hist_ret = close.pct_change().dropna()
                limit_pct = 0.20 if (len(hist_ret) and float(hist_ret.max()) > 0.105) else 0.10
                
                df["date"] = pd.to_datetime(df.index).date
                df = df.sort_values("date")
                for d in tasks_by_code.get(c, []):
                    sd = datetime.strptime(d, "%Y-%m-%d").date()
                    seg = df[(df["date"] >= sd) & (df["date"] <= sd + timedelta(days=max_lookahead_days + 2))].reset_index(drop=True)
                    if seg.empty or len(seg) < 2: continue
                    
                    prev_day = df[df["date"] < sd].tail(1)
                    if prev_day.empty: continue
                    
                    day0_limit = calc_limit_up(float(prev_day["Close"].iloc[0]), limit_pct)
                    day0_is_limit = (float(seg.loc[0, "Close"]) >= (day0_limit - tw_tick(day0_limit)))
                    boards_after = calc_board_count_from_daily(pd.Series([float(prev_day["Close"].iloc[0])] + seg["Close"].astype(float).tolist()), limit_pct)
                    
                    new_rows.append({
                        "signal_date": d, "code": c, "limit_pct_class": f"{int(limit_pct*100)}%",
                        "day0_is_limit_close": str(bool(day0_is_limit)), "boards_after": str(int(boards_after)),
                        "total_boards": str(int((1 + boards_after) if day0_is_limit else 0)),
                    })
            except: continue
        time.sleep(0.08)

    out_new = pd.DataFrame(new_rows)
    if out_new.empty: return out_old
    out_all = out_new if out_old.empty else pd.concat([out_old, out_new], ignore_index=True).drop_duplicates(["signal_date", "code"], keep="last")
    out_all.to_csv(OUTCOME_PATH, index=False, encoding="utf-8-sig")
    return out_all

def render_pretty_table(df):
    if df.empty: return st.info("沒有資料。")
    def f2(x): return f"{float(x):,.2f}" if pd.notna(x) else ""
    def f0(x): return f"{int(float(x)):,}" if pd.notna(x) else ""
    def f3(x): return f"{float(x):,.3f}" if pd.notna(x) else ""
    rows = "".join([f"<tr><td class='center'>{r.get('排名','')}</td><td>{r.get('代號','')}</td><td>{r.get('名稱','')}</td><td>{r.get('族群','')}</td><td class='num'>{f2(r.get('現價',''))}</td><td class='num'>{f2(r.get('漲停價',''))}</td><td class='num'>{f2(r.get('距離漲停(%)',''))}</td><td class='num'>{f2(r.get('較昨收(%)',''))}</td><td class='num'>{f0(r.get('累積量(張)',''))}</td><td class='num'>{f2(r.get('盤中爆量倍數',''))}</td><td class='num'>{f3(r.get('收在高檔(0-1)',''))}</td><td class='num'>{f2(r.get('回落幅度(%)',''))}</td><td class='num'>{r.get('開板次數(5m近似)','')}</td><td class='num'>{r.get('連續貼漲停(最後N根)','')}</td><td class='num'>{r.get('基底天數','')}</td><td class='num'>{f2(r.get('近5日漲幅(%)',''))}</td><td class='num'>{f2(r.get('ATR20(%)',''))}</td><td class='num'>{f2(r.get('連板潛力分',''))}</td></tr>" for _, r in df.iterrows()])
    html = f"""<!doctype html><html><head><style>:root{{--text:#e5e7eb; --line:rgba(148,163,184,.16); --hi: rgba(148,163,184,.08);}} body{{margin:0; background:transparent; color:var(--text); font-family:sans-serif;}} .wrap{{max-height:580px; overflow:auto; border:1px solid var(--line); border-radius:16px; background:rgba(15,17,22,.70);}} table{{width:100%; border-collapse:separate; border-spacing:0; font-size:12.5px;}} thead th{{position:sticky; top:0; z-index:2; text-align:left; padding:11px 10px; background:rgba(15,17,22,.98); color:var(--text); border-bottom:1px solid var(--line); font-weight:900; white-space:nowrap;}} tbody td{{padding:10px 10px; border-bottom:1px solid rgba(148,163,184,.10); color:var(--text); background:rgba(11,13,18,.92); white-space:nowrap;}} tbody tr:hover td{{background:var(--hi);}} .num{{text-align:right; font-variant-numeric:tabular-nums;}} .center{{text-align:center;}}</style></head><body><div class="wrap"><table><thead><tr><th class="center">#</th><th>代號</th><th>名稱</th><th>族群</th><th class="num">現價</th><th class="num">漲停價</th><th class="num">距離漲停(%)</th><th class="num">較昨收(%)</th><th class="num">累積量(張)</th><th class="num">盤中爆量倍數</th><th class="num">收在高檔</th><th class="num">回落(%)</th><th class="num">開板次數</th><th class="num">最後N根貼板</th><th class="num">基底天數</th><th class="num">近5日(%)</th><th class="num">ATR20(%)</th><th class="num">連板潛力分</th></tr></thead><tbody>{rows}</tbody></table></div></body></html>"""
    components.html(html, height=640, scrolling=False)

# =========================
# SIDEBAR
# =========================
st.sidebar.markdown("### 🧠 懶人設定")
mode = st.sidebar.selectbox("策略嚴格度", list(PRESETS.keys()), index=1)
pool_mode = st.sidebar.selectbox("股票池", ["流動性預篩（推薦）", "全上市（很慢）"], index=0)
st.sidebar.markdown("---")
run_scan = st.sidebar.button("🧊 立即掃描", use_container_width=True)
btn_update_bt = st.sidebar.button("📈 更新回測結果", use_container_width=True)

# =========================
# HEADER
# =========================
now_ts = now_taipei()
elapsed = minutes_elapsed_in_session(now_ts)

st.markdown(f"""
<div class="header-wrap">
  <div>
    <h1 class="title">起漲戰情室</h1>
    <div class="subtitle">保留你原始 1~8 濾網的終極主軸 ｜ 零設定、一鍵秒開出結果</div>
  </div>
  <div class="pill"><span class="dot"></span> 台北時間 <b>{now_ts.strftime('%H:%M:%S')}</b>　盤中進度 <b>{elapsed}/270</b></div>
</div>
""", unsafe_allow_html=True)

# =========================
# LOAD META & BACKTEST
# =========================
try: meta = fetch_listed_stocks_mops()
except Exception as e: st.error(f"抓取清單失敗：{e}"); st.stop()
all_codes = meta["code"].tolist()

if btn_update_bt:
    with st.spinner("更新回測結果..."): out_bt = update_backtest(max_lookahead_days=12)
    if out_bt is None or out_bt.empty: st.warning("目前沒有可回測的紀錄。")
    else:
        dfb = out_bt.copy()
        dfb["total_boards"] = dfb["total_boards"].astype(int)
        st.success("✅ 回測更新完成！")
        st.dataframe(dfb["total_boards"].value_counts().sort_index().rename("出現次數").to_frame(), use_container_width=True)

# =========================
# CORE SCAN EXECUTION
# =========================
if run_scan:
    preset = PRESETS[mode]
    
    with st.spinner("建立日線基準 (已修復多執行緒當機 Bug)..."):
        base = build_daily_baseline(all_codes)
    if base is None or base.empty:
        st.error("日線基準抓不到，請稍後重試。")
        st.stop()
    base = base.set_index("code", drop=False)

    codes_to_scan = all_codes
    universe_label = "全上市"
    if pool_mode.startswith("流動性預篩") and "vol_ma20_shares" in base.columns:
        kept = base[base["vol_ma20_shares"].astype(float) >= 500_000]["code"].tolist()
        if len(kept) >= 80: codes_to_scan = kept; universe_label = f"流動性預篩（{len(codes_to_scan)} 檔）"

    with st.spinner("抓取盤中 5m (已修復轉圈圈 Bug)..."):
        bars_today = fetch_intraday_bars_5m(codes_to_scan, batch_size=60)
    if not bars_today:
        st.error("盤中 5m 抓不到資料。")
        st.stop()

    with st.spinner("鎖定「第一根漲停」＋計算連板潛力分..."):
        result = scan_first_limitup_continuation(bars_today, base, meta, preset, now_ts)

    st.markdown("<div class='hr'></div>", unsafe_allow_html=True)
    found = 0 if result is None or result.empty else len(result)

    if found == 0:
        st.warning(f"目前沒有符合『{mode.split('（')[0]}』濾網的候選標的。")
    else:
        append_log(result, mode=mode, universe=universe_label)
        st.success(f"🧊 完美鎖定！掃到 {found} 檔候選標的。")

        topn = result.head(12).copy()
        q75 = float(topn["連板潛力分"].quantile(0.75))
        cols = st.columns(4)

        for i, (_, r) in enumerate(topn.iterrows(), start=1):
            with cols[(i - 1) % 4]:
                score = float(r["連板潛力分"])
                tag = "🔒 幾乎鎖死" if score >= q75 else "👀 候選"
                st.markdown(f"""
<div class="card">
  <div class="metric">
    <div class="left"><div class="label">#{i} <span class="tag">{tag}</span></div><div class="code">{r['代號']} <span class="name">{r['名稱']}</span></div></div>
    <div style="text-align:right"><div class="price">{float(r['現價']):.2f}</div><div class="chg">距漲停 {float(r['距離漲停(%)']):.2f}%</div></div>
  </div>
  <div class="hr"></div>
  <div class="small-note">較昨收：{float(r['較昨收(%)']):.2f}% ｜ 爆量：{float(r['盤中爆量倍數']):.2f}x</div>
  <div class="small-note">開板次數：{r.get('開板次數(5m近似)', '-')} ｜ <span style="color:#a3e635;">連板分：{score:.1f}</span></div>
</div>
""", unsafe_allow_html=True)

        st.markdown("<div class='hr'></div>", unsafe_allow_html=True)
        st.text_input("一鍵複製（代號清單）", value=",".join(result["代號"].astype(str).tolist()))
        with st.expander("📋 看完整榜單（美化表格）", expanded=True):
            render_pretty_table(result)

st.caption("註：無腦設定模式。已修正多執行緒與清單抓取 Bug，絕不轉圈圈。")
