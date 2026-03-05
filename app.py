# app.py — 第一根漲停 + 連板潛力（1～8 全改）｜冷酷黑灰｜懶人版｜卡片 + 美化表格｜可記錄/回測｜族群共振 Radar(可搜尋)
# pip install -U streamlit pandas yfinance requests lxml urllib3

import os
import math
import time
import html
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
    m = minutes_elapsed_in_session(ts)
    return max(1, min(54, int(math.ceil(m / 5.0))))

# =========================
# TICK / LIMIT-UP
# =========================
def tw_tick(price: float) -> float:
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
# DAILY BASELINE
# =========================
BASE_COLS = [
    "code","yday_close","prev2_close","limit_class_pct","vol_ma20_shares",
    "high20_ex1","high60_ex1","low60","range20_pct","range60_pct","atr20_pct",
    "ret_1d","ret_3d","ret_5d","max_ret_10d","had_hype_10d","yday_upper_wick_ratio",
    "yday_vol_spike","base_len_days","base_tight_score",
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
    today = now_taipei().date()
    start = (now_taipei() - timedelta(days=380)).date().isoformat()

    batch = 60
    rows = []

    for i in range(0, len(codes), batch):
        chunk = codes[i:i + batch]
        tickers = " ".join([f"{c}.TW" for c in chunk])

        try:
            raw = yf.download(
                tickers=tickers, start=start, interval="1d",
                group_by="ticker", auto_adjust=False,
                threads=True, progress=False,
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

                ret_1d = (yday_close / prev2_close - 1.0) if prev2_close else None
                ret_3d = (yday_close / float(close.iloc[-4]) - 1.0) if len(close) >= 4 else None
                ret_5d = (yday_close / float(close.iloc[-6]) - 1.0) if len(close) >= 6 else None

                high20_ex1 = float(high.rolling(20).max().shift(1).iloc[-1])
                high60_ex1 = float(high.rolling(60).max().shift(1).iloc[-1])
                low60 = float(low.rolling(60).min().iloc[-1])

                range20_pct = float((high.rolling(20).max().iloc[-1] - low.rolling(20).min().iloc[-1]) / yday_close)
                range60_pct = float((high.rolling(60).max().iloc[-1] - low.rolling(60).min().iloc[-1]) / yday_close)

                prev_close = close.shift(1)
                tr1 = (high - low).abs()
                tr2 = (high - prev_close).abs()
                tr3 = (low - prev_close).abs()
                tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
                atr20 = float(tr.rolling(20).mean().iloc[-1])
                atr20_pct = float(atr20 / yday_close) if yday_close else None

                hist_ret = close.pct_change().dropna()
                max_hist_ret = float(hist_ret.tail(260).max()) if len(hist_ret) > 10 else 0.0
                limit_class_pct = 0.20 if max_hist_ret > 0.105 else 0.10

                thr_hype = 0.19 if limit_class_pct == 0.20 else 0.095
                max_ret_10d = float(hist_ret.tail(10).max()) if len(hist_ret) >= 10 else float(hist_ret.max() if len(hist_ret) else 0.0)
                had_hype_10d = (max_ret_10d >= thr_hype)

                y_open = float(df["Open"].iloc[-1])
                y_high = float(df["High"].iloc[-1])
                y_low = float(df["Low"].iloc[-1])
                y_close = float(df["Close"].iloc[-1])
                y_range = max(1e-9, y_high - y_low)
                y_upper_wick_ratio = float((y_high - max(y_open, y_close)) / y_range)

                vol_ma20 = float(vol.rolling(20).mean().iloc[-1])
                y_vol = float(vol.iloc[-1])
                yday_vol_spike = (y_vol >= 2.0 * vol_ma20)

                ma20 = close.rolling(20).mean()
                near_ma20 = ((close / (ma20 + 1e-9) - 1.0).abs() <= 0.04)
                base_len_days = int(near_ma20.tail(60).sum())

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
# INTRADAY (5m bars)
# =========================
def _normalize_intraday_index(df: pd.DataFrame) -> pd.DataFrame:
    idx = df.index
    try:
        if getattr(idx, "tz", None) is not None:
            idx = idx.tz_convert(TZ_NAME).tz_localize(None)
        else:
            idx = idx.tz_localize(None)
    except Exception:
        try:
            idx = pd.to_datetime(idx).tz_localize(None)
        except Exception:
            pass
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
# INTRADAY PROFILE (same-time vol curve)
# =========================
@st.cache_data(ttl=24 * 3600, show_spinner=False)
def build_intraday_volume_profile(code: str, lookback_days: int = 20) -> list:
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
        return [min(1.0, (i + 1) / 54.0) for i in range(54)]

    if raw is None or raw.empty:
        return [min(1.0, (i + 1) / 54.0) for i in range(54)]

    if isinstance(raw.columns, pd.MultiIndex):
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
    for _, g in df.groupby("date"):
        g = g.sort_index()
        if len(g) < 30:
            continue
        vol = g["Volume"].astype(float).values
        total = float(vol.sum())
        if total <= 0:
            continue
        cum = vol.cumsum() / total
        arr = [float(cum[min(i, len(cum) - 1)]) for i in range(54)]
        sessions.append(arr)

    if not sessions:
        return [min(1.0, (i + 1) / 54.0) for i in range(54)]

    sessions = sessions[-lookback_days:]
    prof = [float(sum(s[i] for s in sessions) / len(sessions)) for i in range(54)]
    for i in range(1, 54):
        prof[i] = max(prof[i], prof[i - 1])
    prof[-1] = 1.0
    return prof

# =========================
# SCAN
# =========================
PRESETS = {
    "保守（只抓幾乎鎖死、連板體質強）": dict(
        near_limit_ticks=1, min_close_pos=0.93, max_pullback=0.0025,
        min_vol_ratio_profile=2.6, min_cum_lots=1500,
        require_break_high60=True, max_ret_5d=8.0, max_atr20=4.8,
        min_base_len=28, min_base_tight=0.55,
        require_lastN_near_limit=3, max_open_board=1,
    ),
    "標準（平衡：第一根漲停 + 連板機率）": dict(
        near_limit_ticks=1, min_close_pos=0.90, max_pullback=0.0038,
        min_vol_ratio_profile=2.1, min_cum_lots=1200,
        require_break_high60=False, max_ret_5d=12.0, max_atr20=6.5,
        min_base_len=18, min_base_tight=0.45,
        require_lastN_near_limit=2, max_open_board=2,
    ),
    "積極（多抓：允許盤中較不穩）": dict(
        near_limit_ticks=2, min_close_pos=0.86, max_pullback=0.0060,
        min_vol_ratio_profile=1.6, min_cum_lots=800,
        require_break_high60=False, max_ret_5d=18.0, max_atr20=8.5,
        min_base_len=10, min_base_tight=0.35,
        require_lastN_near_limit=1, max_open_board=4,
    ),
}

def compute_open_board_count(df5m: pd.DataFrame, limit_up: float, tick: float) -> int:
    if df5m is None or df5m.empty:
        return 999
    close = df5m["Close"].astype(float).values
    high = df5m["High"].astype(float).values

    touch = high >= (limit_up - tick)
    if not touch.any():
        return 999

    first_idx = int(touch.argmax())
    opened = 0
    in_limit_state = True
    for i in range(first_idx + 1, len(close)):
        if in_limit_state:
            if close[i] < (limit_up - 2.0 * tick):
                opened += 1
                in_limit_state = False
        else:
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
    if base_df is None or base_df.empty or not bars_today:
        return pd.DataFrame()

    meta_map = stock_meta.set_index("code")[["name", "industry"]].to_dict(orient="index")
    expected_bar_idx = bars_expected_5m(now_ts) - 1
    candidates = []

    for code, df5m in bars_today.items():
        if code not in base_df.index:
            continue

        b = base_df.loc[code]
        exp_min = max(10, int(0.5 * bars_expected_5m(now_ts)))
        if len(df5m) < exp_min:
            continue

        yday_close = float(b["yday_close"])
        limit_pct = float(b["limit_class_pct"])
        limit_up = calc_limit_up(yday_close, limit_pct)
        tick = tw_tick(limit_up)

        last = float(df5m["Close"].iloc[-1])
        day_high = float(df5m["High"].max())
        day_low = float(df5m["Low"].min())
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

        # same-time volume profile
        if near_limit and vol_lots >= int(preset["min_cum_lots"]) and float(b["vol_ma20_shares"]) > 0:
            prof = build_intraday_volume_profile(code, lookback_days=20)
            frac = float(prof[min(53, max(0, expected_bar_idx))])
            expected_vol = float(b["vol_ma20_shares"]) * frac
            vol_ratio_profile = (vol_shares / (expected_vol + 1e-9)) if expected_vol > 0 else 0.0
        else:
            frac_lin = max(0.2, bars_expected_5m(now_ts) / 54.0)
            expected_vol = float(b["vol_ma20_shares"]) * frac_lin if float(b["vol_ma20_shares"]) > 0 else 0.0
            vol_ratio_profile = (vol_shares / (expected_vol + 1e-9)) if expected_vol > 0 else 0.0

        open_board = compute_open_board_count(df5m, limit_up, tick)
        lastN_hit = lastN_near_limit(df5m, limit_up, tick, preset["require_lastN_near_limit"])

        ret_5d = float(b["ret_5d"]) if pd.notna(b["ret_5d"]) else 0.0
        atr20 = float(b["atr20_pct"]) if pd.notna(b["atr20_pct"]) else 999.0

        cond = (
            near_limit
            and (close_pos >= float(preset["min_close_pos"]))
            and (pullback <= float(preset["max_pullback"]))
            and (vol_lots >= int(preset["min_cum_lots"]))
            and (vol_ratio_profile >= float(preset["min_vol_ratio_profile"]))
            and (not yday_was_limit_like)
            and (not had_hype_10d)
            and (not yday_bad)
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

        m = meta_map.get(code, {"name": "", "industry": ""})
        name = m.get("name", "")
        industry = m.get("industry", "") or "未分類"

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
    out["族群"] = out["族群"].fillna("未分類")

    # sector resonance
    grp = out["族群"].value_counts()
    out["族群共振數"] = out["族群"].apply(lambda x: int(grp.get(x, 1)))
    out["族群共振加分"] = out["族群共振數"].apply(lambda n: min(15.0, max(0.0, (n - 1) * 5.0)))

    def score_row(r):
        s = 0.0
        s += 30.0 * min(1.0, max(0.0, (float(r["收在高檔(0-1)"]) - 0.85) / 0.15))
        s += 20.0 * min(1.0, max(0.0, (0.70 - float(r["回落幅度(%)"])) / 0.70))
        s += 10.0 * min(1.0, float(r["連續貼漲停(最後N根)"]) / max(1, preset["require_lastN_near_limit"]))

        vr = float(r["盤中爆量倍數"])
        s += 20.0 * min(1.0, max(0.0, (vr - 1.5) / 2.5))

        s += 10.0 * min(1.0, max(0.0, (float(r["基底天數"]) - 8) / 40.0))
        s += 5.0 * min(1.0, max(0.0, float(r["基底緊縮分"])))

        s += float(r["族群共振加分"])

        ob = r["開板次數(5m近似)"]
        if ob is not None:
            s -= min(10.0, float(ob) * 3.0)

        if bool(r["是否突破60日高"]):
            s += 5.0

        return float(max(0.0, min(100.0, s)))

    out["連板潛力分"] = out.apply(score_row, axis=1)
    out = out.sort_values(["連板潛力分", "距離漲停(%)", "盤中爆量倍數"], ascending=[False, True, False]).reset_index(drop=True)
    out.insert(0, "排名", range(1, len(out) + 1))
    return out

# =========================
# LOGGING / BACKTEST (unchanged)
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

def calc_board_count_from_daily(close_series: pd.Series, limit_pct: float) -> int:
    if close_series is None or len(close_series) < 2:
        return 0
    closes = close_series.astype(float).values
    boards = 0
    for i in range(1, len(closes)):
        prev_close = float(closes[i - 1])
        limit_up = calc_limit_up(prev_close, limit_pct)
        tick = tw_tick(limit_up)
        if float(closes[i]) >= (limit_up - tick):
            boards += 1
        else:
            break
    return boards

def update_backtest(max_lookahead_days: int = 12) -> pd.DataFrame:
    log = load_log()
    if log.empty:
        return pd.DataFrame()

    if "掃描時間" not in log.columns or "代號" not in log.columns:
        return pd.DataFrame()

    log = log.copy()
    log["signal_date"] = log["掃描時間"].astype(str).str.slice(0, 10)
    log["code"] = log["代號"].astype(str).str.strip()

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

    tasks = []
    for _, r in log.iterrows():
        key = (str(r["signal_date"]), str(r["code"]))
        if key in done_keys:
            continue
        tasks.append(key)

    if not tasks:
        return out_old

    new_rows = []
    tasks_by_code = {}
    for d, c in tasks:
        tasks_by_code.setdefault(c, []).
