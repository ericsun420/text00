# app.py — 第一根漲停 + 連板潛力（1～8 全部改）｜冷酷黑灰｜懶人版｜卡片 + 美化表格｜可記錄/回測
# pip install -U streamlit pandas yfinance requests lxml urllib3

import os
import math
import time
from datetime import datetime, timedelta, time as dtime
from io import StringIO

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
  --bg:#07080b;
  --panel:#0b0d12;
  --card:#0f1116;
  --text:#e5e7eb;
  --muted:#9ca3af;
  --line:rgba(148,163,184,.16);
  --shadow: 0 16px 40px rgba(0,0,0,.35);
  --hi: rgba(148,163,184,.08);
  --ok:#a3e635;
  --warn:#fbbf24;
  --bad:#fb7185;
}

/* Force whole app dark */
[data-testid="stAppViewContainer"]{ background: var(--bg) !important; color: var(--text) !important; }
.main{ background: var(--bg) !important; }
.block-container{ padding-top: 1.15rem; padding-bottom: 2.0rem; }

/* Header/Toolbar */
[data-testid="stHeader"]{
  background: rgba(7,8,11,.80) !important;
  border-bottom: 1px solid var(--line) !important;
}
[data-testid="stToolbar"]{ background: transparent !important; }

/* Sidebar */
[data-testid="stSidebar"]{
  background: var(--panel) !important;
  border-right: 1px solid var(--line) !important;
}
[data-testid="stSidebar"] *{ color: var(--text) !important; }
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span{ color: var(--muted) !important; }

/* Header */
.header-wrap{
  display:flex; align-items:flex-end; justify-content:space-between;
  gap:18px; padding: 6px 4px 2px 4px;
}
.title{
  font-size: 42px; font-weight: 900; letter-spacing: .4px;
  background: linear-gradient(90deg, #f3f4f6, #9ca3af);
  -webkit-background-clip:text; -webkit-text-fill-color: transparent;
  margin:0;
}
.subtitle{
  margin:6px 0 0 2px; color: var(--muted); font-size: 14px;
}

/* Right pill */
.pill{
  display:inline-flex; align-items:center; gap:8px;
  padding: 8px 12px; border:1px solid var(--line);
  border-radius: 999px; color: var(--text);
  background: rgba(15,17,22,.85);
  font-size: 13px;
  box-shadow: var(--shadow);
}
.pill b{ color: var(--text); }
.pill .dot{ width:8px; height:8px; border-radius:999px; background:#9ca3af; display:inline-block; }

/* Cards */
.grid{
  display:grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 12px;
  margin: 12px 0 6px 0;
}
.card{
  background: linear-gradient(180deg, rgba(15,17,22,.94), rgba(11,13,18,.94));
  border:1px solid var(--line);
  border-radius: 16px;
  padding: 14px 14px 12px 14px;
  box-shadow: var(--shadow);
}
.k{ color: var(--muted); font-size: 12px; margin-bottom: 6px; }
.v{ color: var(--text); font-size: 20px; font-weight: 800; }
.v small{ color: var(--muted); font-weight: 600; font-size: 12px; margin-left: 6px;}
.hr{ height:1px; background: var(--line); margin: 12px 0; }

.banner{
  background: rgba(148,163,184,.08);
  border: 1px solid rgba(148,163,184,.22);
  color: var(--text);
  border-radius: 16px;
  padding: 12px 14px;
  margin: 10px 0 10px 0;
}
.banner b{ color: #fff; }

/* TOP cards */
.metric{
  display:flex; justify-content:space-between; align-items:flex-end;
  gap:10px;
}
.metric .left{ display:flex; flex-direction:column; gap:2px; }
.metric .label{ color: var(--muted); font-size: 12px; display:flex; gap:8px; align-items:center; }
.metric .code{ color: var(--text); font-size: 16px; font-weight: 900; line-height:1.1; }
.metric .name{ color: var(--muted); font-size: 12px; margin-top: 2px; }
.metric .tag{
  font-size: 12px; padding: 4px 8px; border-radius: 999px;
  border:1px solid var(--line); color: var(--text);
  background: rgba(15,17,22,.8);
}
.metric .price{ font-size: 22px; font-weight: 900; color: var(--text); line-height: 1; }
.metric .chg{ font-size: 12px; color: var(--muted); }

/* Controls */
.stButton>button{
  border-radius: 14px !important;
  border: 1px solid rgba(203,213,225,.26) !important;
  background: linear-gradient(90deg, rgba(148,163,184,.16), rgba(107,114,128,.10)) !important;
  color: var(--text) !important;
  font-weight: 800 !important;
  padding: 10px 14px !important;
}
.stButton>button:hover{
  border: 1px solid rgba(203,213,225,.42) !important;
  background: linear-gradient(90deg, rgba(148,163,184,.22), rgba(107,114,128,.14)) !important;
}
.stSelectbox>div>div, .stTextInput>div>div{
  border-radius: 14px !important;
  border: 1px solid rgba(148,163,184,.22) !important;
  background: rgba(15,17,22,.88) !important;
  color: var(--text) !important;
}

/* Expander */
[data-testid="stExpander"]{
  border: 1px solid var(--line) !important;
  border-radius: 16px !important;
  background: rgba(15,17,22,.55) !important;
}
[data-testid="stExpander"] summary{
  color: var(--text) !important;
  font-weight: 900 !important;
}

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
    if ts < start:
        return 0
    if ts > end:
        return 270
    return int((ts - start).total_seconds() // 60)

def bars_expected_5m(ts: datetime) -> int:
    # 9:00~13:30 => 270min => 54 bars in 5m
    m = minutes_elapsed_in_session(ts)
    return max(1, min(54, int(math.ceil(m / 5.0))))

# =========================
# TICK / LIMIT-UP
# =========================
def tw_tick(price: float) -> float:
    # TWSE tick size table (official tiers).
    if price < 10:
        return 0.01
    if price < 50:
        return 0.05
    if price < 100:
        return 0.10
    if price < 500:
        return 0.50
    if price < 1000:
        return 1.00
    return 5.00

def round_to_tick_nearest(x: float, tick: float) -> float:
    # We use nearest-tick rounding and later allow 1~2 tick tolerance for matching.
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

LOG_PATH = os.path.join(DATA_DIR, "signals_log.csv")        # 每次掃描記錄
OUTCOME_PATH = os.path.join(DATA_DIR, "signals_outcome.csv")# 回測結果

# =========================
# STOCK LIST (MOPS CSV)
# =========================
def http_get_bytes(url: str, timeout: int = 40) -> bytes:
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        r.raise_for_status()
        return r.content
    except requests.exceptions.SSLError:
        r = requests.get(url.replace("http://", "https://"),
                         headers=headers, timeout=timeout, allow_redirects=True, verify=False)
        r.raise_for_status()
        return r.content

def decode_csv_bytes(b: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp950", "big5", "big5hkscs"):
        try:
            t = b.decode(enc)
        except Exception:
            continue
        if ("公司代號" in t) and ("公司簡稱" in t or "公司名稱" in t):
            return t
    return b.decode("cp950", errors="ignore")

@st.cache_data(ttl=24 * 3600, show_spinner=False)
def fetch_listed_stocks_mops() -> pd.DataFrame:
    # 上市公司基本資料（常含產業別欄位；若沒有就只用 code/name）
    url = "http://mopsfin.twse.com.tw/opendata/t187ap03_L.csv"
    b = http_get_bytes(url)
    csv_text = decode_csv_bytes(b)
    df = pd.read_csv(StringIO(csv_text), dtype=str, engine="python")
    df.columns = [str(c).strip() for c in df.columns]

    if "公司代號" not in df.columns:
        raise ValueError(f"MOPS CSV 欄位異常：{list(df.columns)[:40]}")

    name_col = "公司簡稱" if "公司簡稱" in df.columns else ("公司名稱" if "公司名稱" in df.columns else None)
    if name_col is None:
        raise ValueError(f"MOPS CSV 欄位找不到公司名稱：{list(df.columns)[:40]}")

    # 產業欄位（有就拿來做「族群共振」，沒有就留空）
    industry_col = None
    for c in ["產業別", "產業類別", "產業名稱", "產業"]:
        if c in df.columns:
            industry_col = c
            break

    cols = ["公司代號", name_col] + ([industry_col] if industry_col else [])
    out = df[cols].copy()
    out = out.rename(columns={"公司代號": "code", name_col: "name"})
    if industry_col:
        out = out.rename(columns={industry_col: "industry"})
    else:
        out["industry"] = ""

    out["code"] = out["code"].astype(str).str.strip()
    out["name"] = out["name"].astype(str).str.strip()
    out["industry"] = out["industry"].astype(str).str.strip()

    out = out[out["code"].str.match(r"^\d{4,6}$")].drop_duplicates("code").sort_values("code").reset_index(drop=True)
    return out[["code", "name", "industry"]]

# =========================
# DAILY BASELINE (for 10%/20%, base length, compression, exclusions)
# =========================
BASE_COLS = [
    "code",
    "yday_close",
    "prev2_close",
    "limit_class_pct",         # 0.10 or 0.20 (history-based)
    "vol_ma20_shares",
    "high20_ex1",
    "high60_ex1",
    "low60",
    "range20_pct",
    "range60_pct",
    "atr20_pct",
    "ret_1d",
    "ret_3d",
    "ret_5d",
    "max_ret_10d",
    "had_hype_10d",
    "yday_upper_wick_ratio",
    "yday_vol_spike",
    "base_len_days",
    "base_tight_score",
]

def _drop_today_bar_if_exists(df: pd.DataFrame, today_date) -> pd.DataFrame:
    if df.empty:
        return df
    last_date = pd.Timestamp(df.index[-1]).date()
    if last_date == today_date:
        return df.iloc[:-1].copy()
    return df

@st.cache_data(ttl=6 * 3600, show_spinner=False)
def build_daily_baseline(codes: list[str]) -> pd.DataFrame:
    """
    1) 10%/20% 用歷史日漲幅最大值判斷（>10.5% 視為 20% 族群）
    2) 基底長度/收斂（base_len_days, base_tight_score）
    3) 排雷（近10日 hype、昨日爆量長上影）
    """
    today = now_taipei().date()
    start = (now_taipei() - timedelta(days=380)).date().isoformat()

    batch = 60
    rows = []

    for i in range(0, len(codes), batch):
        chunk = codes[i:i + batch]
        tickers = " ".join([f"{c}.TW" for c in chunk])

        try:
            raw = yf.download(
                tickers=tickers,
                start=start,
                interval="1d",
                group_by="ticker",
                auto_adjust=False,
                threads=True,
                progress=False,
            )
        except Exception:
            continue

        for c in chunk:
            t = f"{c}.TW"
            try:
                if isinstance(raw.columns, pd.MultiIndex):
                    if t not in raw.columns.get_level_values(0):
                        continue
                    df = raw[t].dropna().copy()
                else:
                    df = raw.dropna().copy()

                df = _drop_today_bar_if_exists(df, today)
                if df.empty or len(df) < 120:
                    continue

                close = df["Close"].astype(float)
                high = df["High"].astype(float)
                low = df["Low"].astype(float)
                vol = df["Volume"].astype(float)

                yday_close = float(close.iloc[-1])
                prev2_close = float(close.iloc[-2])

                # returns
                ret_1d = (yday_close / prev2_close - 1.0) if prev2_close else None
                ret_3d = (yday_close / float(close.iloc[-4]) - 1.0) if len(close) >= 4 else None
                ret_5d = (yday_close / float(close.iloc[-6]) - 1.0) if len(close) >= 6 else None

                # rolling highs excluding yesterday (for breakout reference)
                high20_ex1 = float(high.rolling(20).max().shift(1).iloc[-1])
                high60_ex1 = float(high.rolling(60).max().shift(1).iloc[-1])
                low60 = float(low.rolling(60).min().iloc[-1])

                # ranges
                range20_pct = float((high.rolling(20).max().iloc[-1] - low.rolling(20).min().iloc[-1]) / yday_close)
                range60_pct = float((high.rolling(60).max().iloc[-1] - low.rolling(60).min().iloc[-1]) / yday_close)

                # ATR20%
                prev_close = close.shift(1)
                tr1 = (high - low).abs()
                tr2 = (high - prev_close).abs()
                tr3 = (low - prev_close).abs()
                tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
                atr20 = float(tr.rolling(20).mean().iloc[-1])
                atr20_pct = float(atr20 / yday_close) if yday_close else None

                # 10%/20% class (history-based): if any 1d return > 10.5% => likely 20% group
                hist_ret = close.pct_change().dropna()
                max_hist_ret = float(hist_ret.tail(260).max()) if len(hist_ret) > 10 else 0.0
                limit_class_pct = 0.20 if max_hist_ret > 0.105 else 0.10

                # last 10d hype detector: any day return > (limit-1%) => already "吸睛過"
                thr_hype = 0.19 if limit_class_pct == 0.20 else 0.095
                max_ret_10d = float(hist_ret.tail(10).max()) if len(hist_ret) >= 10 else float(hist_ret.max() if len(hist_ret) else 0.0)
                had_hype_10d = (max_ret_10d >= thr_hype)

                # yesterday wick/volume spike (exclude "前一日已先派發")
                y_open = float(df["Open"].iloc[-1])
                y_high = float(df["High"].iloc[-1])
                y_low = float(df["Low"].iloc[-1])
                y_close = float(df["Close"].iloc[-1])
                y_range = max(1e-9, y_high - y_low)
                y_upper_wick_ratio = float((y_high - max(y_open, y_close)) / y_range)

                vol_ma20 = float(vol.rolling(20).mean().iloc[-1])
                y_vol = float(vol.iloc[-1])
                yday_vol_spike = (y_vol >= 2.0 * vol_ma20)

                # base length: count last 60 days where close stayed near MA20 (+/-4%)
                ma20 = close.rolling(20).mean()
                near_ma20 = ((close / (ma20 + 1e-9) - 1.0).abs() <= 0.04)
                base_len_days = int(near_ma20.tail(60).sum())

                # base tight score: smaller range20 within range60 + lower ATR => tighter base
                base_tight_score = float(
                    (1.0 - min(1.0, range20_pct / (range60_pct + 1e-9))) * 0.6
                    + (1.0 - min(1.0, (atr20_pct or 1.0) / 0.08)) * 0.4
                )

                rows.append({
                    "code": c,
                    "yday_close": yday_close,
                    "prev2_close": prev2_close,
                    "limit_class_pct": limit_class_pct,
                    "vol_ma20_shares": vol_ma20,
                    "high20_ex1": high20_ex1,
                    "high60_ex1": high60_ex1,
                    "low60": low60,
                    "range20_pct": range20_pct,
                    "range60_pct": range60_pct,
                    "atr20_pct": atr20_pct * 100.0 if atr20_pct is not None else None,
                    "ret_1d": ret_1d * 100.0 if ret_1d is not None else None,
                    "ret_3d": ret_3d * 100.0 if ret_3d is not None else None,
                    "ret_5d": ret_5d * 100.0 if ret_5d is not None else None,
                    "max_ret_10d": max_ret_10d * 100.0 if max_ret_10d is not None else None,
                    "had_hype_10d": bool(had_hype_10d),
                    "yday_upper_wick_ratio": y_upper_wick_ratio,
                    "yday_vol_spike": bool(yday_vol_spike),
                    "base_len_days": base_len_days,
                    "base_tight_score": base_tight_score,
                })
            except Exception:
                continue

        time.sleep(0.05)

    if not rows:
        return pd.DataFrame(columns=BASE_COLS)

    out = pd.DataFrame(rows).drop_duplicates("code")
    for c in BASE_COLS:
        if c not in out.columns:
            out[c] = pd.NA
    return out[BASE_COLS].copy()

# =========================
# INTRADAY (current day 5m bars)
# =========================
def _normalize_intraday_index(df: pd.DataFrame) -> pd.DataFrame:
    # yfinance may return tz-aware index; convert to Asia/Taipei then drop tz
    idx = df.index
    try:
        if getattr(idx, "tz", None) is not None:
            idx = idx.tz_convert(TZ_NAME).tz_localize(None)
        else:
            idx = idx.tz_localize(None)
    except Exception:
        # fallback: make naive
        try:
            idx = pd.to_datetime(idx).tz_localize(None)
        except Exception:
            pass
    df = df.copy()
    df.index = idx
    return df

@st.cache_data(ttl=20, show_spinner=False)
def fetch_intraday_bars_5m(codes: list[str], batch_size: int = 60) -> dict:
    """
    Return dict: code -> 5m bars DataFrame for today (may be partial)
    """
    bars = {}
    today = now_taipei().date()

    for i in range(0, len(codes), batch_size):
        chunk = codes[i:i + batch_size]
        tickers = " ".join([f"{c}.TW" for c in chunk])

        try:
            raw = yf.download(
                tickers=tickers,
                period="1d",
                interval="5m",
                group_by="ticker",
                auto_adjust=False,
                threads=True,
                progress=False,
            )
        except Exception:
            continue

        for c in chunk:
            t = f"{c}.TW"
            try:
                if isinstance(raw.columns, pd.MultiIndex):
                    if t not in raw.columns.get_level_values(0):
                        continue
                    df = raw[t].dropna().copy()
                else:
                    df = raw.dropna().copy()

                if df.empty:
                    continue

                df = _normalize_intraday_index(df)
                df = df[df.index.date == today].copy()
                if df.empty:
                    continue

                bars[c] = df
            except Exception:
                continue

        time.sleep(0.12)

    return bars

# =========================
# INTRADAY PROFILE (same-time volume curve) — Improvement #3
# =========================
@st.cache_data(ttl=24 * 3600, show_spinner=False)
def build_intraday_volume_profile(code: str, lookback_days: int = 20) -> list:
    """
    Build average cumulative volume fraction by 5m bar index (1..54).
    Uses last ~60 days 5m data and selects last N sessions with enough bars.
    Returns list length 54 with values in (0,1].
    """
    ticker = f"{code}.TW"
    try:
        raw = yf.download(
            tickers=ticker,
            period="60d",
            interval="5m",
            group_by="ticker",
            auto_adjust=False,
            threads=False,
            progress=False,
        )
    except Exception:
        return [min(1.0, (i + 1) / 54.0) for i in range(54)]  # fallback linear

    if raw is None or raw.empty:
        return [min(1.0, (i + 1) / 54.0) for i in range(54)]

    if isinstance(raw.columns, pd.MultiIndex):
        # for single ticker, sometimes not MultiIndex, but handle anyway
        if ticker in raw.columns.get_level_values(0):
            df = raw[ticker].dropna().copy()
        else:
            df = raw.dropna().copy()
    else:
        df = raw.dropna().copy()

    df = _normalize_intraday_index(df)
    if df.empty:
        return [min(1.0, (i + 1) / 54.0) for i in range(54)]

    df["date"] = df.index.date
    sessions = []
    for d, g in df.groupby("date"):
        # take only regular session-like days
        g = g.sort_index()
        if len(g) < 30:
            continue
        vol = g["Volume"].astype(float).values
        total = float(vol.sum())
        if total <= 0:
            continue
        cum = vol.cumsum() / total
        # map to 54 bars by index position (truncate or pad with last value)
        arr = [float(cum[min(i, len(cum) - 1)]) for i in range(54)]
        sessions.append(arr)

    if not sessions:
        return [min(1.0, (i + 1) / 54.0) for i in range(54)]

    sessions = sessions[-lookback_days:]
    prof = []
    for i in range(54):
        prof.append(float(sum(s[i] for s in sessions) / len(sessions)))

    # enforce monotonic and final ~1
    for i in range(1, 54):
        prof[i] = max(prof[i], prof[i - 1])
    prof[-1] = 1.0
    return prof

# =========================
# SCAN — Improvement #1~#7 in one pipeline
# =========================
PRESETS = {
    # 保守：只抓幾乎鎖死 + 基底長 + 低過熱 + 族群共振加權
    "保守（只抓幾乎鎖死、連板體質強）": dict(
        near_limit_ticks=1,
        min_close_pos=0.93,
        max_pullback=0.0025,     # 0.25% from high
        min_vol_ratio_profile=2.6,
        min_cum_lots=1500,
        require_break_high60=True,
        max_ret_5d=8.0,
        max_atr20=4.8,
        min_base_len=28,
        min_base_tight=0.55,
        require_lastN_near_limit=3,
        max_open_board=1,
    ),
    "標準（平衡：第一根漲停 + 連板機率）": dict(
        near_limit_ticks=1,
        min_close_pos=0.90,
        max_pullback=0.0038,
        min_vol_ratio_profile=2.1,
        min_cum_lots=1200,
        require_break_high60=False,
        max_ret_5d=12.0,
        max_atr20=6.5,
        min_base_len=18,
        min_base_tight=0.45,
        require_lastN_near_limit=2,
        max_open_board=2,
    ),
    "積極（多抓：允許盤中較不穩）": dict(
        near_limit_ticks=2,
        min_close_pos=0.86,
        max_pullback=0.0060,
        min_vol_ratio_profile=1.6,
        min_cum_lots=800,
        require_break_high60=False,
        max_ret_5d=18.0,
        max_atr20=8.5,
        min_base_len=10,
        min_base_tight=0.35,
        require_lastN_near_limit=1,
        max_open_board=4,
    ),
}

def compute_open_board_count(df5m: pd.DataFrame, limit_up: float, tick: float) -> int:
    """
    Approximate '開板次數' from 5m bars:
    - Find first touch near limit (high >= limit - tick)
    - Count how many times AFTER that touch, close fell below (limit - 2*tick)
      and then later touched near limit again.
    """
    if df5m is None or df5m.empty:
        return 999

    close = df5m["Close"].astype(float).values
    high = df5m["High"].astype(float).values

    touch = high >= (limit_up - tick)
    if not touch.any():
        return 999

    first_idx = int(touch.argmax())
    opened = 0
    in_limit_state = True  # after first touch, we assume it "went to limit"
    for i in range(first_idx + 1, len(close)):
        if in_limit_state:
            if close[i] < (limit_up - 2.0 * tick):
                opened += 1
                in_limit_state = False
        else:
            # wait until it touches again to count another open-board cycle
            if high[i] >= (limit_up - tick):
                in_limit_state = True
    return opened

def lastN_near_limit(df5m: pd.DataFrame, limit_up: float, tick: float, N: int) -> int:
    if df5m is None or df5m.empty:
        return 0
    tail = df5m.tail(N)
    close = tail["Close"].astype(float).values
    return int((close >= (limit_up - tick)).sum())

def scan_first_limitup_continuation(
    bars_today: dict,
    base_df: pd.DataFrame,
    stock_meta: pd.DataFrame,
    preset: dict,
    now_ts: datetime
) -> pd.DataFrame:
    """
    Implements improvements:
    1) (No orderbook) -> price-based lock proxies + open-board count + lastN near limit
    2) 10%/20% class from history baseline
    3) same-time volume profile (per ticker)
    4) stronger exclusions (yday limit/hype, last10 hype)
    5) base length/tightness filters
    6) lock quality via lastN + open-board proxy
    7) sector resonance bonus using 'industry'
    """
    if base_df is None or base_df.empty or not bars_today:
        return pd.DataFrame()

    meta_map = stock_meta.set_index("code")[["name", "industry"]].to_dict(orient="index")

    expected_bar_idx = bars_expected_5m(now_ts) - 1  # 0-based

    candidates = []
    for code, df5m in bars_today.items():
        if code not in base_df.index:
            continue

        b = base_df.loc[code]

        # data completeness check (Improvement #2)
        exp_min = max(10, int(0.5 * bars_expected_5m(now_ts)))
        if len(df5m) < exp_min:
            continue

        yday_close = float(b["yday_close"])
        limit_pct = float(b["limit_class_pct"])
        limit_up = calc_limit_up(yday_close, limit_pct)
        tick = tw_tick(limit_up)

        last = float(df5m["Close"].iloc[-1])
        day_open = float(df5m["Open"].iloc[0])
        day_high = float(df5m["High"].max())
        day_low = float(df5m["Low"].min())
        vol_shares = float(df5m["Volume"].sum())
        vol_lots = int(vol_shares / 1000)

        # near limit (within N ticks)
        near_limit = last >= (limit_up - preset["near_limit_ticks"] * tick)

        # lock proxies
        rng = max(1e-9, day_high - day_low)
        close_pos = (last - day_low) / rng
        pullback = (day_high - last) / max(1e-9, day_high)

        # first board exclusion (yesterday was limit-ish)
        ret_1d = float(b["ret_1d"]) if pd.notna(b["ret_1d"]) else 0.0
        yday_was_limit_like = ret_1d >= (19.0 if limit_pct == 0.20 else 9.5)

        # stronger exclusions (Improvement #4)
        had_hype_10d = bool(b["had_hype_10d"])
        yday_bad = bool(b["yday_vol_spike"]) and (float(b["yday_upper_wick_ratio"]) >= 0.35) and (ret_1d >= 6.0)

        # base filters (Improvement #5)
        base_len = int(b["base_len_days"]) if pd.notna(b["base_len_days"]) else 0
        base_tight = float(b["base_tight_score"]) if pd.notna(b["base_tight_score"]) else 0.0

        # breakout filter (optional)
        break_high60 = True
        if preset["require_break_high60"]:
            high60 = float(b["high60_ex1"]) if pd.notna(b["high60_ex1"]) else 0.0
            break_high60 = limit_up >= (high60 * 0.995)

        # same-time volume profile (Improvement #3) — only if near_limit candidate
        # to reduce cost, only build profile if it's already near limit and has basic liquidity
        vol_ratio_profile = None
        if near_limit and vol_lots >= int(preset["min_cum_lots"]) and float(b["vol_ma20_shares"]) > 0:
            prof = build_intraday_volume_profile(code, lookback_days=20)
            frac = float(prof[min(53, max(0, expected_bar_idx))])
            expected_vol = float(b["vol_ma20_shares"]) * frac
            vol_ratio_profile = (vol_shares / (expected_vol + 1e-9)) if expected_vol > 0 else 0.0
        else:
            # fallback to linear (still usable)
            frac_lin = max(0.2, bars_expected_5m(now_ts) / 54.0)
            expected_vol = float(b["vol_ma20_shares"]) * frac_lin if float(b["vol_ma20_shares"]) > 0 else 0.0
            vol_ratio_profile = (vol_shares / (expected_vol + 1e-9)) if expected_vol > 0 else 0.0

        # lock quality (Improvement #6)
        open_board = compute_open_board_count(df5m, limit_up, tick)
        lastN_hit = lastN_near_limit(df5m, limit_up, tick, preset["require_lastN_near_limit"])

        # overheat / volatility filters
        ret_5d = float(b["ret_5d"]) if pd.notna(b["ret_5d"]) else 0.0
        atr20 = float(b["atr20_pct"]) if pd.notna(b["atr20_pct"]) else 999.0

        # apply main conditions
        cond = (
            near_limit
            and (close_pos >= float(preset["min_close_pos"]))
            and (pullback <= float(preset["max_pullback"]))
            and (vol_lots >= int(preset["min_cum_lots"]))
            and (vol_ratio_profile >= float(preset["min_vol_ratio_profile"]))
            and (not yday_was_limit_like)     # first board
            and (not had_hype_10d)            # not already hyped in last 10
            and (not yday_bad)                # avoid yesterday distribution
            and (ret_5d <= float(preset["max_ret_5d"]))
            and (atr20 <= float(preset["max_atr20"]))
            and (base_len >= int(preset["min_base_len"]))
            and (base_tight >= float(preset["min_base_tight"]))
            and break_high60
            and (lastN_hit >= int(preset["require_lastN_near_limit"]))
            and (open_board <= int(preset["max_open_board"]))
        )

        if not cond:
            continue

        # base/industry meta
        m = meta_map.get(code, {"name": "", "industry": ""})
        name = m.get("name", "")
        industry = m.get("industry", "")

        dist_pct = (limit_up - last) / max(1e-9, limit_up) * 100.0
        chg_pct = (last / yday_close - 1.0) * 100.0

        candidates.append({
            "代號": code,
            "名稱": name,
            "族群": industry,
            "現價": last,
            "漲停價": limit_up,
            "距離漲停(%)": dist_pct,
            "較昨收(%)": chg_pct,
            "累積量(張)": vol_lots,
            "盤中爆量倍數": float(vol_ratio_profile),
            "收在高檔(0-1)": float(close_pos),
            "回落幅度(%)": float(pullback * 100.0),
            "開板次數(5m近似)": int(open_board) if open_board != 999 else None,
            "連續貼漲停(最後N根)": int(lastN_hit),
            "基底天數": int(base_len),
            "基底緊縮分": float(base_tight),
            "ATR20(%)": float(atr20),
            "近5日漲幅(%)": float(ret_5d),
            "是否突破60日高": bool(break_high60),
        })

    if not candidates:
        return pd.DataFrame()

    out = pd.DataFrame(candidates)

    # sector resonance (Improvement #7): count candidates per industry and add bonus
    out["族群"] = out["族群"].fillna("")
    grp = out["族群"].value_counts()
    out["族群共振數"] = out["族群"].apply(lambda x: int(grp.get(x, 1)) if x else 1)
    out["族群共振加分"] = out["族群共振數"].apply(lambda n: min(15.0, max(0.0, (n - 1) * 5.0)))

    # continuation score (0..100) — lock quality + volume quality + base + resonance
    def score_row(r):
        s = 0.0
        # lock
        s += 30.0 * min(1.0, max(0.0, (float(r["收在高檔(0-1)"]) - 0.85) / 0.15))
        s += 20.0 * min(1.0, max(0.0, (0.70 - float(r["回落幅度(%)"])) / 0.70))
        s += 10.0 * min(1.0, float(r["連續貼漲停(最後N根)"]) / max(1, preset["require_lastN_near_limit"]))

        # volume
        vr = float(r["盤中爆量倍數"])
        s += 20.0 * min(1.0, max(0.0, (vr - 1.5) / 2.5))

        # base
        s += 10.0 * min(1.0, max(0.0, (float(r["基底天數"]) - 8) / 40.0))
        s += 5.0 * min(1.0, max(0.0, float(r["基底緊縮分"])))

        # resonance
        s += float(r["族群共振加分"])

        # penalty: more open board
        ob = r["開板次數(5m近似)"]
        if ob is not None:
            s -= min(10.0, float(ob) * 3.0)

        # bonus: break 60d high
        if bool(r["是否突破60日高"]):
            s += 5.0

        return float(max(0.0, min(100.0, s)))

    out["連板潛力分"] = out.apply(score_row, axis=1)

    # sort: score desc, dist asc, vol_ratio desc
    out = out.sort_values(["連板潛力分", "距離漲停(%)", "盤中爆量倍數"], ascending=[False, True, False]).reset_index(drop=True)
    out.insert(0, "排名", range(1, len(out) + 1))
    return out

# =========================
# LOGGING — Improvement #8 (event log)
# =========================
def append_log(df: pd.DataFrame, mode: str, universe: str) -> None:
    if df.empty:
        return
    ts = now_taipei()
    log = df.copy()
    log["掃描時間"] = ts.strftime("%Y-%m-%d %H:%M:%S")
    log["模式"] = mode
    log["股票池"] = universe
    cols_first = ["掃描時間", "模式", "股票池", "排名", "代號", "名稱", "族群"]
    remain = [c for c in log.columns if c not in cols_first]
    log = log[cols_first + remain]

    if os.path.exists(LOG_PATH):
        old = pd.read_csv(LOG_PATH, dtype=str)
        # append as text to avoid dtype issues
        log.to_csv(LOG_PATH, mode="a", index=False, header=False, encoding="utf-8-sig")
    else:
        log.to_csv(LOG_PATH, index=False, encoding="utf-8-sig")

def load_log() -> pd.DataFrame:
    if not os.path.exists(LOG_PATH):
        return pd.DataFrame()
    try:
        return pd.read_csv(LOG_PATH, dtype=str)
    except Exception:
        return pd.DataFrame()

# =========================
# BACKTEST — Improvement #8 (update outcomes)
# =========================
def calc_board_count_from_daily(close_series: pd.Series, limit_pct: float) -> int:
    """
    Given daily close series for consecutive days starting at signal day,
    count consecutive 'limit-up close' days using prev close & tick rounding.
    """
    if close_series is None or len(close_series) < 2:
        return 0

    closes = close_series.astype(float).values
    boards = 0

    for i in range(1, len(closes)):
        prev_close = float(closes[i - 1])
        limit_up = calc_limit_up(prev_close, limit_pct)
        tick = tw_tick(limit_up)

        if float(closes[i]) >= (limit_up - tick):  # within 1 tick
            boards += 1
        else:
            break

    return boards

def update_backtest(max_lookahead_days: int = 12) -> pd.DataFrame:
    """
    Reads signals_log.csv, and computes future boards using daily closes.
    Output:
      - day0_close_limit (是否當天收盤漲停)
      - boards_after (連板數：從隔天開始連續漲停收盤幾天)
      - total_boards (若 day0 也收漲停，則 total = 1 + boards_after，否則 0)
    """
    log = load_log()
    if log.empty:
        return pd.DataFrame()

    # normalize
    log = log.copy()
    if "掃描時間" not in log.columns or "代號" not in log.columns:
        return pd.DataFrame()

    log["signal_date"] = log["掃描時間"].astype(str).str.slice(0, 10)
    log["code"] = log["代號"].astype(str).str.strip()

    # load existing outcomes
    if os.path.exists(OUTCOME_PATH):
        try:
            out_old = pd.read_csv(OUTCOME_PATH, dtype=str)
        except Exception:
            out_old = pd.DataFrame()
    else:
        out_old = pd.DataFrame()

    done_keys = set()
    if not out_old.empty and {"signal_date", "code"}.issubset(out_old.columns):
        done_keys = set(zip(out_old["signal_date"].astype(str), out_old["code"].astype(str)))

    # tasks to compute
    tasks = []
    for _, r in log.iterrows():
        key = (str(r["signal_date"]), str(r["code"]))
        if key in done_keys:
            continue
        tasks.append(key)

    if not tasks:
        return out_old

    # compute in batches
    new_rows = []
    tasks_by_code = {}
    for d, c in tasks:
        tasks_by_code.setdefault(c, []).append(d)

    codes = sorted(tasks_by_code.keys())
    batch = 40

    for i in range(0, len(codes), batch):
        chunk = codes[i:i + batch]
        tickers = " ".join([f"{c}.TW" for c in chunk])

        # extend date range to include lookahead
        # yfinance uses UTC; we rely on daily bars anyway
        try:
            raw = yf.download(
                tickers=tickers,
                period="6mo",
                interval="1d",
                group_by="ticker",
                auto_adjust=False,
                threads=True,
                progress=False,
            )
        except Exception:
            continue

        for c in chunk:
            t = f"{c}.TW"
            try:
                if isinstance(raw.columns, pd.MultiIndex):
                    if t not in raw.columns.get_level_values(0):
                        continue
                    df = raw[t].dropna().copy()
                else:
                    df = raw.dropna().copy()

                if df.empty or "Close" not in df.columns:
                    continue

                # choose limit class from latest baseline heuristic:
                # if any day return > 10.5% in this df => 20 else 10
                close = df["Close"].astype(float)
                hist_ret = close.pct_change().dropna()
                limit_pct = 0.20 if (len(hist_ret) and float(hist_ret.max()) > 0.105) else 0.10

                # for each signal date, compute day0 limit close and subsequent boards
                df_dates = pd.to_datetime(df.index).date
                df = df.copy()
                df["date"] = df_dates
                df = df.sort_values("date")

                for d in tasks_by_code.get(c, []):
                    sd = datetime.strptime(d, "%Y-%m-%d").date()
                    # slice from signal date to signal+lookahead
                    end = sd + timedelta(days=max_lookahead_days + 2)
                    seg = df[(df["date"] >= sd) & (df["date"] <= end)].copy()
                    if seg.empty or len(seg) < 2:
                        continue

                    seg = seg.reset_index(drop=True)
                    # determine if signal day close is limit-up close (needs prev day close)
                    # We treat "signal day" as day0, but if the log was intraday, day0 close might not be available yet.
                    # day0 limit-up close check uses day-1 close as reference.
                    # Find day0 index in seg (should be 0)
                    day0_close = float(seg.loc[0, "Close"])
                    # Need prev day close:
                    prev_day = df[df["date"] < sd].tail(1)
                    if prev_day.empty:
                        continue
                    prev_close = float(prev_day["Close"].iloc[0])

                    day0_limit = calc_limit_up(prev_close, limit_pct)
                    tick = tw_tick(day0_limit)
                    day0_is_limit = (day0_close >= (day0_limit - tick))

                    # boards after: from day0 close onward, count consecutive limit-up closes starting next day
                    # We'll create a close series that starts at prev_close then day0.. to reuse function
                    series = pd.Series([prev_close] + seg["Close"].astype(float).tolist())
                    boards_after = calc_board_count_from_daily(series, limit_pct)

                    total_boards = (1 + boards_after) if day0_is_limit else 0

                    new_rows.append({
                        "signal_date": d,
                        "code": c,
                        "limit_pct_class": f"{int(limit_pct*100)}%",
                        "day0_is_limit_close": str(bool(day0_is_limit)),
                        "boards_after": str(int(boards_after)),
                        "total_boards": str(int(total_boards)),
                    })
            except Exception:
                continue

        time.sleep(0.08)

    out_new = pd.DataFrame(new_rows)
    if out_new.empty:
        return out_old

    # merge and save
    if out_old.empty:
        out_all = out_new
    else:
        out_all = pd.concat([out_old, out_new], ignore_index=True).drop_duplicates(["signal_date", "code"], keep="last")

    out_all.to_csv(OUTCOME_PATH, index=False, encoding="utf-8-sig")
    return out_all

# =========================
# PRETTY TABLE (components.html)
# =========================
def render_pretty_table(df: pd.DataFrame) -> None:
    if df.empty:
        st.info("沒有資料。")
        return

    def f2(x):
        try: return f"{float(x):,.2f}"
        except Exception: return str(x)

    def f0(x):
        try: return f"{int(float(x)):,}"
        except Exception: return str(x)

    def f3(x):
        try: return f"{float(x):,.3f}"
        except Exception: return str(x)

    rows = []
    for _, r in df.iterrows():
        rows.append(f"""
        <tr>
          <td class="center">{r.get('排名','')}</td>
          <td>{r.get('代號','')}</td>
          <td>{r.get('名稱','')}</td>
          <td>{r.get('族群','')}</td>
          <td class="num">{f2(r.get('現價',''))}</td>
          <td class="num">{f2(r.get('漲停價',''))}</td>
          <td class="num">{f2(r.get('距離漲停(%)',''))}</td>
          <td class="num">{f2(r.get('較昨收(%)',''))}</td>
          <td class="num">{f0(r.get('累積量(張)',''))}</td>
          <td class="num">{f2(r.get('盤中爆量倍數',''))}</td>
          <td class="num">{f3(r.get('收在高檔(0-1)',''))}</td>
          <td class="num">{f2(r.get('回落幅度(%)',''))}</td>
          <td class="num">{r.get('開板次數(5m近似)','')}</td>
          <td class="num">{r.get('連續貼漲停(最後N根)','')}</td>
          <td class="num">{r.get('基底天數','')}</td>
          <td class="num">{f2(r.get('近5日漲幅(%)',''))}</td>
          <td class="num">{f2(r.get('ATR20(%)',''))}</td>
          <td class="num">{f2(r.get('連板潛力分',''))}</td>
        </tr>
        """)

    html = f"""
    <!doctype html>
    <html><head><meta charset="utf-8"/>
    <style>
      :root {{
        --text:#e5e7eb; --muted:#9ca3af; --line:rgba(148,163,184,.16); --hi: rgba(148,163,184,.08);
      }}
      body {{ margin:0; background: transparent; color: var(--text);
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Noto Sans TC","PingFang TC","Microsoft JhengHei", Arial, sans-serif; }}
      .wrap {{
        max-height: 580px; overflow:auto;
        border: 1px solid var(--line); border-radius: 16px;
        background: rgba(15,17,22,.70);
      }}
      table {{ width:100%; border-collapse: separate; border-spacing: 0; font-size: 12.5px; }}
      thead th {{
        position: sticky; top: 0; z-index: 2;
        text-align: left; padding: 11px 10px;
        background: rgba(15,17,22,.98);
        color: var(--text); border-bottom: 1px solid var(--line);
        font-weight: 900; white-space: nowrap;
      }}
      tbody td {{
        padding: 10px 10px;
        border-bottom: 1px solid rgba(148,163,184,.10);
        color: var(--text); background: rgba(11,13,18,.92);
        white-space: nowrap;
      }}
      tbody tr:hover td {{ background: var(--hi); }}
      .num {{ text-align:right; font-variant-numeric: tabular-nums; }}
      .center {{ text-align:center; }}
    </style></head>
    <body>
      <div class="wrap">
        <table>
          <thead><tr>
            <th class="center">#</th><th>代號</th><th>名稱</th><th>族群</th>
            <th class="num">現價</th><th class="num">漲停價</th><th class="num">距離漲停(%)</th><th class="num">較昨收(%)</th>
            <th class="num">累積量(張)</th><th class="num">盤中爆量倍數</th>
            <th class="num">收在高檔</th><th class="num">回落(%)</th>
            <th class="num">開板次數</th><th class="num">最後N根貼板</th>
            <th class="num">基底天數</th><th class="num">近5日(%)</th><th class="num">ATR20(%)</th>
            <th class="num">連板潛力分</th>
          </tr></thead>
          <tbody>{''.join(rows)}</tbody>
        </table>
      </div>
    </body></html>
    """
    components.html(html, height=640, scrolling=False)

# =========================
# SIDEBAR (still lazy)
# =========================
st.sidebar.markdown("### 🧠 懶人設定")
mode = st.sidebar.selectbox("模式", list(PRESETS.keys()), index=1)
pool_mode = st.sidebar.selectbox("股票池", ["流動性預篩（推薦）", "全上市（很慢）"], index=0)
st.sidebar.markdown("---")
run_scan = st.sidebar.button("🧊 立即掃描", use_container_width=True)
btn_update_bt = st.sidebar.button("📈 更新回測結果", use_container_width=True)
btn_clear_cache = st.sidebar.button("🔄 清快取", use_container_width=True)

# =========================
# HEADER
# =========================
now_ts = now_taipei()
elapsed = minutes_elapsed_in_session(now_ts)

st.markdown(f"""
<div class="header-wrap">
  <div>
    <h1 class="title">起漲戰情室</h1>
    <div class="subtitle">主軸：抓「第一根漲停」＋提高後續 2～7 根連板機率（瑞軒型）</div>
  </div>
  <div class="pill"><span class="dot"></span> 台北時間 <b>{now_ts.strftime('%H:%M:%S')}</b>　盤中進度 <b>{elapsed}/270</b></div>
</div>
""", unsafe_allow_html=True)

if not is_market_time(now_ts):
    st.info("目前非盤中：掃描會使用『最後可取得的 5m 盤中資料快照』，準確度會比盤中低。")

# =========================
# LOAD META
# =========================
try:
    meta = fetch_listed_stocks_mops()
except Exception as e:
    st.error(f"抓上市清單失敗：{e}")
    st.stop()

all_codes = meta["code"].tolist()

if btn_clear_cache:
    fetch_listed_stocks_mops.clear()
    build_daily_baseline.clear()
    fetch_intraday_bars_5m.clear()
    build_intraday_volume_profile.clear()
    st.success("已清除快取（上市清單 / 日線基準 / 盤中5m / 量能曲線）。")

# =========================
# UPDATE BACKTEST
# =========================
if btn_update_bt:
    with st.spinner("更新回測結果（會讀取 scan_data/signals_log.csv 並補齊連板數）..."):
        out_bt = update_backtest(max_lookahead_days=12)

    if out_bt is None or out_bt.empty:
        st.warning("目前沒有可回測的紀錄（先掃描幾次才會有 log）。")
    else:
        # show quick summary
        dfb = out_bt.copy()
        dfb["total_boards"] = dfb["total_boards"].astype(int)
        dist = dfb["total_boards"].value_counts().sort_index()
        st.markdown("<div class='hr'></div>", unsafe_allow_html=True)
        st.success("✅ 回測更新完成（資料寫入 scan_data/signals_outcome.csv）")
        st.write("📊 連板數分佈（total_boards）")
        st.dataframe(dist.rename("count").to_frame(), use_container_width=True)

# =========================
# UNIVERSE + BASELINE
# =========================
preset = PRESETS[mode]

with st.spinner("建立日線基準（10%/20%判斷、基底、排雷）..."):
    base = build_daily_baseline(all_codes)

if base is None or base.empty:
    st.error("日線基準抓不到（yfinance 可能被限流/網路限制）。")
    st.stop()

base = base.set_index("code", drop=False)

# prefilter
codes_to_scan = all_codes
universe_label = "全上市"

if pool_mode.startswith("流動性預篩"):
    # 20日均量>=500張/日
    liq_thr = 500_000
    if "vol_ma20_shares" in base.columns:
        kept = base[base["vol_ma20_shares"].astype(float) >= liq_thr]["code"].tolist()
        if len(kept) >= 80:
            codes_to_scan = kept
            universe_label = f"流動性預篩（{len(codes_to_scan)} 檔）"
        else:
            universe_label = "全上市（預篩資料不足→降級）"

st.markdown(f"""
<div class="grid">
  <div class="card"><div class="k">股票池</div><div class="v">{universe_label}</div></div>
  <div class="card"><div class="k">掃描目標</div><div class="v">第一根漲停</div></div>
  <div class="card"><div class="k">核心濾網</div><div class="v">貼板品質 + 基底收斂 + 排雷</div></div>
  <div class="card"><div class="k">模式</div><div class="v">{mode.split('（')[0]}<small>（內建參數）</small></div></div>
</div>
""", unsafe_allow_html=True)

st.markdown("""
<div class="banner">
<b>你要的「瑞軒型」我用 8 層過濾做掉：</b>
①貼近漲停( tick ) ②回落小 ③收在高檔 ④同時間量能曲線爆量 ⑤排除昨日先派發 ⑥排除近10日已嗨過
⑦基底長＋緊縮 ⑧族群共振加權＋回測自動驗證
</div>
""", unsafe_allow_html=True)

# =========================
# RUN SCAN
# =========================
if run_scan:
    with st.spinner("抓取盤中 5m（分批）..."):
        bars_today = fetch_intraday_bars_5m(codes_to_scan, batch_size=60)
# =========================
# 🧭 族群共振 Radar（獨立掃描區塊，不用搜尋）
# =========================
st.markdown("<div class='hr'></div>", unsafe_allow_html=True)
st.subheader("🧭 族群共振 Radar（獨立掃描）")

# 你可以依模式自動調整門檻（保守更嚴格）
heat_thr = 70.0 if "保守" in mode else (55.0 if "積極" in mode else 62.0)

with st.spinner("掃描族群共振（整個股票池：看誰在一起熱、一起貼板）..."):
    sector_rank_df, sector_members = scan_sector_resonance_radar(
        bars_today=bars_today,
        base_df=base,     # 你日線基準那個 DataFrame（已 set_index 也OK）
        meta_df=meta,     # MOPS 的 code/name/industry
        now_ts=now_ts,
        heat_threshold=heat_thr,
        near_ticks=2,
        top_sectors=10,
        top_members=12
    )

if sector_rank_df.empty:
    st.info("目前掃不到族群共振（可能盤中資料不足或資料源被限制）。")
else:
    # Top 族群卡片
    cols = st.columns(4)
    for i, (_, r) in enumerate(sector_rank_df.head(8).iterrows(), start=1):
        with cols[(i - 1) % 4]:
            st.markdown(f"""
<div class="card">
  <div class="k">#{int(r['排名'])} 族群共振</div>
  <div class="v">{html.escape(str(r['族群名稱']))}</div>
  <div class="hr"></div>
  <div class="small-note">共振分：{float(r['共振分']):.1f} ｜ 熱檔：{int(r['熱檔數'])} ｜ 貼板：{int(r['貼板數'])}</div>
  <div class="small-note">掃描檔數：{int(r['掃描檔數'])} ｜ 平均熱度：{float(r['平均熱度']):.1f}</div>
</div>
""", unsafe_allow_html=True)

    # 族群排行榜（表格）
    with st.expander("📋 族群共振排行榜（Top 10）", expanded=True):
        show = sector_rank_df.copy()
        st.dataframe(show, use_container_width=True)

    # 每個族群 Top 成員
    st.markdown("<div class='hr'></div>", unsafe_allow_html=True)
    st.subheader("🔥 各族群 Top 成員（熱度最高）")

    for sec in sector_rank_df["族群名稱"].tolist():
        sub = sector_members.get(sec, pd.DataFrame())
        if sub.empty:
            continue
        with st.expander(f"📌 {sec}（Top {len(sub)}）", expanded=False):
            cols_show = ["排名","代號","名稱","熱度分","貼板","距離漲停(%)","較昨收(%)","盤中爆量倍數(快)","累積量(張)","回落(%)"]
            st.dataframe(sub[cols_show], use_container_width=True, height=420)
    if not bars_today:
        st.error("盤中 5m 抓不到資料（yfinance intraday 可能被限制）。")
        st.stop()

    with st.spinner("鎖定「第一根漲停」＋計算連板潛力分（含同時間量能曲線 / 開板次數 / 族群共振）..."):
        result = scan_first_limitup_continuation(
            bars_today=bars_today,
            base_df=base,
            stock_meta=meta,
            preset=preset,
            now_ts=now_ts
        )

    st.markdown("<div class='hr'></div>", unsafe_allow_html=True)

    found = 0 if result is None or result.empty else len(result)

    st.markdown(f"""
<div class="grid">
  <div class="card"><div class="k">資料來源</div><div class="v">yfinance 5m + 日線</div></div>
  <div class="card"><div class="k">掃描檔數</div><div class="v">{len(bars_today):,}</div></div>
  <div class="card"><div class="k">第一根漲停候選</div><div class="v">{found:,}</div></div>
  <div class="card"><div class="k">輸出</div><div class="v">TOP卡片 + 美化表格 + log</div></div>
</div>
""", unsafe_allow_html=True)

    if found == 0:
        st.warning("目前沒有符合『第一根漲停 + 連板體質濾網』的候選。你可以切到「積極」放寬。")
    else:
        # Save log (Improvement #8)
        append_log(result, mode=mode, universe=universe_label)

        st.success(f"🧊 掃到 {found} 檔「第一根漲停」候選（已按連板潛力分排序），並已寫入 scan_data/signals_log.csv")

        topn = result.head(12).copy()
        q75 = float(topn["連板潛力分"].quantile(0.75))
        cols = st.columns(4)

        for i, (_, r) in enumerate(topn.iterrows(), start=1):
            with cols[(i - 1) % 4]:
                score = float(r["連板潛力分"])
                tag = "🔒 幾乎鎖死" if score >= q75 else "👀 候選"

                code = str(r["代號"])
                name = str(r["名稱"])
                price = float(r["現價"])
                lim = float(r["漲停價"])
                dist = float(r["距離漲停(%)"])
                chg = float(r["較昨收(%)"])
                lots = int(float(r["累積量(張)"]))
                volx = float(r["盤中爆量倍數"])
                ob = r.get("開板次數(5m近似)", None)

                st.markdown(f"""
<div class="card">
  <div class="metric">
    <div class="left">
      <div class="label">#{i} <span class="tag">{tag}</span></div>
      <div class="code">{code}</div>
      <div class="name">{name}</div>
    </div>
    <div style="text-align:right">
      <div class="price">{price:.2f}</div>
      <div class="chg">漲停 {lim:.2f}｜距離 {dist:.2f}%</div>
    </div>
  </div>
  <div class="hr"></div>
  <div class="small-note">較昨收：{chg:.2f}% ｜ 累積量：{lots:,} 張 ｜ 爆量：{volx:.2f}x</div>
  <div class="small-note">開板(5m近似)：{ob} ｜ 連板潛力分：{score:.1f}</div>
</div>
""", unsafe_allow_html=True)

        st.markdown("<div class='hr'></div>", unsafe_allow_html=True)

        # Watchlist export (simple)
        watch = ",".join(result["代號"].astype(str).tolist())
        st.text_input("一鍵複製（代號清單）", value=watch)

        with st.expander("📋 看完整榜單（美化表格）", expanded=True):
            render_pretty_table(result)

st.caption("註：未接券商五檔/封單時，『鎖死』用價格型態近似（tick/回落/收高/開板次數/最後N根貼板）。回測請按右側「更新回測結果」。")

def _sf(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default

def scan_sector_resonance_radar(
    bars_today: dict,
    base_df: pd.DataFrame,
    meta_df: pd.DataFrame,
    now_ts: datetime,
    *,
    heat_threshold: float = 65.0,     # 熱度門檻（越高越嚴格）
    near_ticks: int = 2,              # 距離漲停幾個 tick 內算「貼板」
    top_sectors: int = 10,
    top_members: int = 12,
) -> tuple[pd.DataFrame, dict]:
    """
    回傳：
      1) sector_rank_df：族群共振排行榜
      2) sector_members：dict[族群] -> DataFrame(該族群Top成員)
    """
    if not bars_today or base_df is None or base_df.empty:
        return pd.DataFrame(), {}

    # base index
    base = base_df
    if "code" in base.columns:
        base = base.set_index("code", drop=False)

    # meta maps
    meta = meta_df.copy()
    meta["industry"] = meta["industry"].fillna("").replace("", "未分類")
    code_to_name = dict(zip(meta["code"], meta["name"]))
    code_to_ind = dict(zip(meta["code"], meta["industry"]))

    # time progress (for rough expected volume; radar要快，不做profile)
    bar_n = bars_expected_5m(now_ts)
    frac_lin = max(0.2, bar_n / 54.0)

    rows = []
    for code, df5m in bars_today.items():
        if code not in base.index:
            continue

        b = base.loc[code]
        yday_close = b.get("yday_close", None)
        if pd.isna(yday_close):
            continue
        yday_close = float(yday_close)

        limit_pct = b.get("limit_class_pct", 0.10)
        try:
            limit_pct = float(limit_pct)
        except Exception:
            limit_pct = 0.10

        limit_up = calc_limit_up(yday_close, limit_pct)
        tick = tw_tick(limit_up)

        last = _sf(df5m["Close"].iloc[-1])
        day_high = _sf(df5m["High"].max())
        day_low = _sf(df5m["Low"].min())
        vol_shares = _sf(df5m["Volume"].sum())
        vol_lots = int(vol_shares / 1000)

        # 貼板/距離
        dist_to_limit_pct = (limit_up - last) / max(1e-9, limit_up) * 100.0
        near_limit = last >= (limit_up - near_ticks * tick)

        # 收高與回落（鎖板品質）
        rng = max(1e-9, day_high - day_low)
        close_pos = (last - day_low) / rng
        pullback = (day_high - last) / max(1e-9, day_high)  # 0~1

        # 爆量（快速版）
        vol_ma20 = _sf(b.get("vol_ma20_shares", 0.0))
        exp_vol = vol_ma20 * frac_lin if vol_ma20 > 0 else 0.0
        vol_ratio = (vol_shares / (exp_vol + 1e-9)) if exp_vol > 0 else 0.0

        # 漲幅
        chg_pct = (last / yday_close - 1.0) * 100.0

        # 熱度分 0~100（族群雷達用：抓“集體升溫”）
        heat = 0.0
        # 越接近漲停越高（貼板直接加滿）
        heat += 35.0 if near_limit else max(0.0, 35.0 * (1.0 - dist_to_limit_pct / 5.0))  # 5%內有分
        # 漲幅靠近漲停
        heat += 20.0 * min(1.0, max(0.0, chg_pct / (limit_pct * 100.0)))
        # 爆量品質
        heat += 20.0 * min(1.0, max(0.0, (vol_ratio - 1.0) / 2.5))
        # 收在高檔
        heat += 15.0 * min(1.0, max(0.0, (close_pos - 0.55) / 0.45))
        # 回落懲罰（>1%回落扣爆）
        heat -= 20.0 * min(1.0, max(0.0, pullback / 0.01))

        heat = float(max(0.0, min(100.0, heat)))

        industry = code_to_ind.get(code, "未分類") or "未分類"

        rows.append({
            "代號": code,
            "名稱": code_to_name.get(code, ""),
            "族群": industry,
            "熱度分": heat,
            "貼板": bool(near_limit),
            "距離漲停(%)": float(dist_to_limit_pct),
            "較昨收(%)": float(chg_pct),
            "盤中爆量倍數(快)": float(vol_ratio),
            "累積量(張)": int(vol_lots),
            "收在高檔(0-1)": float(close_pos),
            "回落(%)": float(pullback * 100.0),
        })

    if not rows:
        return pd.DataFrame(), {}

    stock_df = pd.DataFrame(rows)
    stock_df["族群"] = stock_df["族群"].fillna("未分類")
    stock_df["熱"] = stock_df["熱度分"] >= float(heat_threshold)

    # 族群共振分數：熱檔數 + 貼板數 + 平均熱度 + 最高熱度（你要的“共振”）
    g = stock_df.groupby("族群", dropna=False)
    sector = g.agg(
        掃描檔數=("代號", "count"),
        熱檔數=("熱", "sum"),
        貼板數=("貼板", "sum"),
        平均熱度=("熱度分", "mean"),
        最高熱度=("熱度分", "max"),
        平均爆量=("盤中爆量倍數(快)", "mean"),
    ).reset_index().rename(columns={"族群": "族群名稱"})

    # 共振分（0~100 近似）：熱檔+貼板權重最大
    sector["共振分"] = (
        sector["熱檔數"] * 18.0 +
        sector["貼板數"] * 10.0 +
        sector["平均熱度"] * 0.35 +
        sector["最高熱度"] * 0.25 +
        sector["平均爆量"] * 2.0
    )
    # normalization (clip)
    sector["共振分"] = sector["共振分"].clip(0, 100)

    sector = sector.sort_values(["共振分", "熱檔數", "貼板數", "最高熱度"], ascending=False).head(int(top_sectors))
    sector.insert(0, "排名", range(1, len(sector) + 1))

    # 各族群 top 成員
    sector_members = {}
    for sec in sector["族群名稱"].tolist():
        sub = stock_df[stock_df["族群"] == sec].copy()
        sub = sub.sort_values(["熱度分", "貼板", "距離漲停(%)"], ascending=[False, False, True]).head(int(top_members))
        sub.insert(0, "排名", range(1, len(sub) + 1))
        sector_members[sec] = sub

    return sector, sector_members

