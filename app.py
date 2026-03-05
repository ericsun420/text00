# app.py — 起漲戰情室（第一根漲停＋連板潛力）＋🧭族群共振 Radar（獨立掃描）
# ✅ 冷酷黑灰｜懶人模式｜卡片＋美化表格｜不依賴 TWSE/MOPS 清單（避免 THE PAGE CANNOT BE ACCESSED）
#
# 安裝：
#   pip install -U streamlit pandas yfinance requests urllib3
# （強烈建議）pip install -U twstock   # 讓清單來源完全不碰 TWSE/MOPS 網域
#
# 執行：
#   streamlit run app.py

import math
import time
import html
import csv
import io
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dtime
from typing import Dict, List, Tuple

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
[data-testid="stAppViewContainer"]{ background: var(--bg) !important; color: var(--text) !important; }
.main{ background: var(--bg) !important; }
.block-container{ padding-top: 1.05rem; padding-bottom: 2.0rem; }
[data-testid="stHeader"]{ background: rgba(7,8,11,.80) !important; border-bottom: 1px solid var(--line) !important; }
[data-testid="stToolbar"]{ background: transparent !important; }
[data-testid="stSidebar"]{ background: var(--panel) !important; border-right: 1px solid var(--line) !important; }
[data-testid="stSidebar"] *{ color: var(--text) !important; }
[data-testid="stSidebar"] label,[data-testid="stSidebar"] p,[data-testid="stSidebar"] span{ color: var(--muted) !important; }
.header-wrap{ display:flex; align-items:flex-end; justify-content:space-between; gap:18px; padding: 6px 4px 2px 4px; }
.title{
  font-size: 42px; font-weight: 900; letter-spacing: .4px;
  background: linear-gradient(90deg, #f3f4f6, #9ca3af);
  -webkit-background-clip:text; -webkit-text-fill-color: transparent; margin:0;
}
.subtitle{ margin:6px 0 0 2px; color: var(--muted); font-size: 14px; }
.pill{
  display:inline-flex; align-items:center; gap:8px;
  padding: 8px 12px; border:1px solid var(--line);
  border-radius: 999px; color: var(--text);
  background: rgba(15,17,22,.85);
  font-size: 13px; box-shadow: var(--shadow);
}
.pill .dot{ width:8px; height:8px; border-radius:999px; background:#9ca3af; display:inline-block; }
.grid{ display:grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin: 12px 0 6px 0; }
.card{
  background: linear-gradient(180deg, rgba(15,17,22,.94), rgba(11,13,18,.94));
  border:1px solid var(--line); border-radius: 16px;
  padding: 14px 14px 12px 14px; box-shadow: var(--shadow);
}
.k{ color: var(--muted); font-size: 12px; margin-bottom: 6px; }
.v{ color: var(--text); font-size: 20px; font-weight: 800; }
.v small{ color: var(--muted); font-weight: 600; font-size: 12px; margin-left: 6px;}
.hr{ height:1px; background: var(--line); margin: 12px 0; }
.banner{
  background: rgba(148,163,184,.08);
  border: 1px solid rgba(148,163,184,.22);
  color: var(--text);
  border-radius: 16px; padding: 12px 14px; margin: 10px 0 10px 0;
}
.banner b{ color:#fff; }
.metric{ display:flex; justify-content:space-between; align-items:flex-end; gap:10px; }
.metric .left{ display:flex; flex-direction:column; gap:2px; }
.metric .label{ color: var(--muted); font-size: 12px; display:flex; gap:8px; align-items:center; }
.metric .code{ color: var(--text); font-size: 16px; font-weight: 900; line-height:1.1; }
.metric .name{ color: var(--muted); font-size: 12px; margin-top: 2px; }
.metric .tag{ font-size: 12px; padding: 4px 8px; border-radius: 999px; border:1px solid var(--line); color: var(--text); background: rgba(15,17,22,.8); }
.metric .price{ font-size: 22px; font-weight: 900; color: var(--text); line-height: 1; }
.metric .chg{ font-size: 12px; color: var(--muted); }
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
[data-testid="stExpander"]{
  border: 1px solid var(--line) !important;
  border-radius: 16px !important;
  background: rgba(15,17,22,.55) !important;
}
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
# Universe source (NO TWSE/MOPS)
# =========================
@dataclass
class StockMeta:
    code: str
    name: str
    market: str        # 上市 / 上櫃
    industry: str      # group or fallback
    yf_symbol: str     # 2330.TW / 3081.TWO

def _http_get_text(url: str, timeout: int = 45) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/plain,text/csv;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        "Connection": "close",
    }
    r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    text = r.text
    # normalize newlines (some huge csv come with \r only)
    return text.replace("\r\n", "\n").replace("\r", "\n")

def _robust_read_csv(text: str) -> pd.DataFrame:
    sample = text[:4096]
    delim = ","
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";"])
        delim = dialect.delimiter
    except Exception:
        delim = ","

    df = pd.read_csv(
        io.StringIO(text),
        sep=delim,
        dtype=str,
        engine="python",
        on_bad_lines="skip",
        quoting=csv.QUOTE_MINIMAL,
        skipinitialspace=True,
    )
    df.columns = [str(c).strip() for c in df.columns]
    return df

def _parse_twstock_codes_df(df: pd.DataFrame, default_market: str) -> pd.DataFrame:
    # expected fields per twstock docs: type, code, name, ISIN, start, market, group, CFI :contentReference[oaicite:2]{index=2}
    if "code" not in df.columns:
        # maybe no header
        df2 = pd.read_csv(io.StringIO(df.to_csv(index=False, header=False)), header=None, dtype=str, engine="python")
        df = df2

    if "code" not in df.columns:
        # try treat as raw CSV with no header
        try:
            df = pd.read_csv(io.StringIO(df.to_csv(index=False, header=False)), header=None, dtype=str, engine="python")
        except Exception:
            pass

    # if still no header, map by position
    if "code" not in df.columns:
        if df.shape[1] >= 8:
            df.columns = ["type", "code", "name", "ISIN", "start", "market", "group", "CFI"] + [f"x{i}" for i in range(df.shape[1]-8)]
        elif df.shape[1] >= 3:
            df.columns = ["code", "name", "market"] + [f"x{i}" for i in range(df.shape[1]-3)]
        else:
            raise ValueError("twstock codes csv 欄位不足，無法解析。")

    # fill common fields
    if "name" not in df.columns:
        # guess 2nd column
        df["name"] = df.iloc[:, 2] if df.shape[1] >= 3 else ""
    if "market" not in df.columns:
        df["market"] = default_market
    if "group" not in df.columns:
        df["group"] = ""

    df["code"] = df["code"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()
    df["market"] = df["market"].astype(str).str.strip().replace("", default_market)
    df["industry"] = df["group"].astype(str).str.strip().replace("", "未分類")

    df = df[df["code"].str.match(r"^\d{4,6}$")].drop_duplicates("code")
    return df[["code", "name", "market", "industry"]].copy()

@st.cache_data(ttl=24*3600, show_spinner=False)
def fetch_universe(include_tpex: bool) -> pd.DataFrame:
    """
    Returns columns: code, name, market, industry, yf_symbol
    Priority:
      1) if twstock installed -> local codes dict (no network to TWSE/MOPS)
      2) GitHub raw twstock csv
    """
    # 1) local twstock
    try:
        import twstock  # type: ignore
        # twstock.codes: dict[code] = StockCodeInfo(type, code, name, ISIN, start, market, group, CFI) :contentReference[oaicite:3]{index=3}
        rows = []
        for code, info in twstock.codes.items():
            try:
                c = str(info.code).strip()
                n = str(info.name).strip()
                m = str(info.market).strip() if hasattr(info, "market") else ""
                g = str(info.group).strip() if hasattr(info, "group") else ""
                if not c.isdigit():
                    continue
                if (m == "上市") or (m == "TWSE"):
                    market = "上市"
                    yf_symbol = f"{c}.TW"
                elif (m == "上櫃") or (m == "TPEX"):
                    market = "上櫃"
                    yf_symbol = f"{c}.TWO"
                else:
                    # unknown market: skip unless include_tpex and looks like stock
                    market = "未分類"
                    yf_symbol = f"{c}.TW"
                if (market == "上櫃") and (not include_tpex):
                    continue
                rows.append({
                    "code": c,
                    "name": n,
                    "market": market,
                    "industry": g if g else "未分類",
                    "yf_symbol": yf_symbol,
                })
            except Exception:
                continue
        df = pd.DataFrame(rows).drop_duplicates("code")
        if not df.empty and df["code"].nunique() > 300:
            # good enough
            return df.sort_values(["market", "code"]).reset_index(drop=True)
    except Exception:
        pass

    # 2) GitHub raw (twstock repo)
    # TWSE listed
    twse_raw = "https://raw.githubusercontent.com/mlouielu/twstock/refs/heads/master/twstock/codes/twse_equities.csv"
    tpex_raw = "https://raw.githubusercontent.com/mlouielu/twstock/refs/heads/master/twstock/codes/tpex_equities.csv"

    rows_all = []

    # TWSE
    text = _http_get_text(twse_raw)
    df_twse = _robust_read_csv(text)
    df_twse = _parse_twstock_codes_df(df_twse, default_market="上市")
    df_twse["market"] = "上市"
    df_twse["yf_symbol"] = df_twse["code"].astype(str) + ".TW"
    rows_all.append(df_twse)

    if include_tpex:
        text2 = _http_get_text(tpex_raw)
        df_tpex = _robust_read_csv(text2)
        df_tpex = _parse_twstock_codes_df(df_tpex, default_market="上櫃")
        df_tpex["market"] = "上櫃"
        df_tpex["yf_symbol"] = df_tpex["code"].astype(str) + ".TWO"
        rows_all.append(df_tpex)

    out = pd.concat(rows_all, ignore_index=True).drop_duplicates("code")
    return out[["code", "name", "market", "industry", "yf_symbol"]].sort_values(["market", "code"]).reset_index(drop=True)

# =========================
# DAILY BASELINE (works with yf_symbol)
# =========================
BASE_COLS = [
    "code","yf_symbol","yday_close","prev2_close","limit_class_pct","vol_ma20_shares",
    "high60_ex1","atr20_pct","ret_1d","ret_5d",
    "had_hype_10d","yday_upper_wick_ratio","yday_vol_spike",
    "base_len_days","base_tight_score",
]

def _drop_today_bar_if_exists(df: pd.DataFrame, today_date) -> pd.DataFrame:
    if df.empty: return df
    last_date = pd.Timestamp(df.index[-1]).date()
    if last_date == today_date:
        return df.iloc[:-1].copy()
    return df

@st.cache_data(ttl=6*3600, show_spinner=False)
def build_daily_baseline(symbol_rows: List[Tuple[str, str]]) -> pd.DataFrame:
    """
    symbol_rows: [(code, yf_symbol), ...]
    Return baseline indexed by code, includes vol_ma20_shares for liquidity filter.
    """
    today = now_taipei().date()
    start = (now_taipei() - timedelta(days=380)).date().isoformat()

    # map symbol -> code
    sym2code = {sym: code for code, sym in symbol_rows}
    symbols = [sym for _, sym in symbol_rows]

    batch = 60
    rows = []

    for i in range(0, len(symbols), batch):
        chunk_syms = symbols[i:i+batch]
        tickers = " ".join(chunk_syms)

        try:
            raw = yf.download(
                tickers=tickers, start=start, interval="1d",
                group_by="ticker", auto_adjust=False,
                threads=True, progress=False,
            )
        except Exception:
            continue

        for sym in chunk_syms:
            code = sym2code.get(sym, "")
            try:
                if isinstance(raw.columns, pd.MultiIndex):
                    if sym not in raw.columns.get_level_values(0):
                        continue
                    df = raw[sym].dropna().copy()
                else:
                    # single ticker mode (should not happen here) but keep safe
                    df = raw.dropna().copy()

                df = _drop_today_bar_if_exists(df, today)
                if df.empty or len(df) < 120:
                    continue

                close = df["Close"].astype(float)
                high  = df["High"].astype(float)
                low   = df["Low"].astype(float)
                vol   = df["Volume"].astype(float)

                yday_close = float(close.iloc[-1])
                prev2_close = float(close.iloc[-2])

                ret_1d = (yday_close / prev2_close - 1.0) if prev2_close else None
                ret_5d = (yday_close / float(close.iloc[-6]) - 1.0) if len(close) >= 6 else None

                # ATR20%
                prev_close = close.shift(1)
                tr1 = (high - low).abs()
                tr2 = (high - prev_close).abs()
                tr3 = (low - prev_close).abs()
                tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
                atr20 = float(tr.rolling(20).mean().iloc[-1])
                atr20_pct = float(atr20 / yday_close) if yday_close else None

                vol_ma20 = float(vol.rolling(20).mean().iloc[-1])

                # 10%/20% class (history-based heuristic)
                hist_ret = close.pct_change().dropna()
                max_hist_ret = float(hist_ret.tail(260).max()) if len(hist_ret) > 10 else 0.0
                limit_class_pct = 0.20 if max_hist_ret > 0.105 else 0.10

                thr_hype = 0.19 if limit_class_pct == 0.20 else 0.095
                max_ret_10d = float(hist_ret.tail(10).max()) if len(hist_ret) >= 10 else float(hist_ret.max() if len(hist_ret) else 0.0)
                had_hype_10d = (max_ret_10d >= thr_hype)

                # yesterday wick & vol spike
                y_open = float(df["Open"].iloc[-1])
                y_high = float(df["High"].iloc[-1])
                y_low  = float(df["Low"].iloc[-1])
                y_close = float(df["Close"].iloc[-1])
                y_range = max(1e-9, y_high - y_low)
                y_upper_wick_ratio = float((y_high - max(y_open, y_close)) / y_range)

                y_vol = float(vol.iloc[-1])
                yday_vol_spike = (y_vol >= 2.0 * vol_ma20)

                # base length (near MA20)
                ma20 = close.rolling(20).mean()
                near_ma20 = ((close / (ma20 + 1e-9) - 1.0).abs() <= 0.04)
                base_len_days = int(near_ma20.tail(60).sum())

                # base tight score (0..1)
                range20 = float(high.rolling(20).max().iloc[-1] - low.rolling(20).min().iloc[-1])
                range60 = float(high.rolling(60).max().iloc[-1] - low.rolling(60).min().iloc[-1])
                range20_pct = float(range20 / yday_close) if yday_close else 1.0
                range60_pct = float(range60 / yday_close) if yday_close else 1.0
                base_tight_score = float(
                    (1.0 - min(1.0, range20_pct / (range60_pct + 1e-9))) * 0.6
                    + (1.0 - min(1.0, (atr20_pct or 1.0) / 0.08)) * 0.4
                )

                high60_ex1 = float(high.rolling(60).max().shift(1).iloc[-1])

                rows.append({
                    "code": code,
                    "yf_symbol": sym,
                    "yday_close": yday_close,
                    "prev2_close": prev2_close,
                    "limit_class_pct": limit_class_pct,
                    "vol_ma20_shares": vol_ma20,
                    "high60_ex1": high60_ex1,
                    "atr20_pct": atr20_pct * 100.0 if atr20_pct is not None else None,
                    "ret_1d": ret_1d * 100.0 if ret_1d is not None else None,
                    "ret_5d": ret_5d * 100.0 if ret_5d is not None else None,
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
# INTRADAY (5m bars today) — supports .TW / .TWO
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
def fetch_intraday_bars_5m(symbol_rows: List[Tuple[str, str]], batch_size: int = 60) -> Dict[str, pd.DataFrame]:
    """
    Return dict: code -> df5m
    """
    bars = {}
    today = now_taipei().date()
    sym2code = {sym: code for code, sym in symbol_rows}
    symbols = [sym for _, sym in symbol_rows]

    for i in range(0, len(symbols), batch_size):
        chunk_syms = symbols[i:i + batch_size]
        tickers = " ".join(chunk_syms)

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

        for sym in chunk_syms:
            code = sym2code.get(sym, "")
            try:
                if isinstance(raw.columns, pd.MultiIndex):
                    if sym not in raw.columns.get_level_values(0):
                        continue
                    df = raw[sym].dropna().copy()
                else:
                    df = raw.dropna().copy()

                if df.empty:
                    continue

                df = _normalize_intraday_index(df)
                df = df[df.index.date == today].copy()
                if df.empty:
                    continue

                bars[code] = df
            except Exception:
                continue

        time.sleep(0.12)

    return bars

# =========================
# INTRADAY VOLUME PROFILE (same-time curve)
# =========================
@st.cache_data(ttl=24*3600, show_spinner=False)
def build_intraday_volume_profile(yf_symbol: str, lookback_days: int = 20) -> List[float]:
    try:
        raw = yf.download(
            tickers=yf_symbol,
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
        if yf_symbol in raw.columns.get_level_values(0):
            df = raw[yf_symbol].dropna().copy()
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
# SCAN params
# =========================
PRESETS = {
    "保守（只抓幾乎鎖死、連板體質強）": dict(
        near_limit_ticks=1, min_close_pos=0.93, max_pullback=0.0025,
        min_vol_ratio_profile=2.6, min_cum_lots=1500,
        max_ret_5d=8.0, max_atr20=4.8,
        min_base_len=28, min_base_tight=0.55,
        require_lastN_near_limit=3, max_open_board=1,
        require_break_high60=True,
    ),
    "標準（平衡：第一根漲停 + 連板機率）": dict(
        near_limit_ticks=1, min_close_pos=0.90, max_pullback=0.0038,
        min_vol_ratio_profile=2.1, min_cum_lots=1200,
        max_ret_5d=12.0, max_atr20=6.5,
        min_base_len=18, min_base_tight=0.45,
        require_lastN_near_limit=2, max_open_board=2,
        require_break_high60=False,
    ),
    "積極（多抓：允許盤中較不穩）": dict(
        near_limit_ticks=2, min_close_pos=0.86, max_pullback=0.0060,
        min_vol_ratio_profile=1.6, min_cum_lots=800,
        max_ret_5d=18.0, max_atr20=8.5,
        min_base_len=10, min_base_tight=0.35,
        require_lastN_near_limit=1, max_open_board=4,
        require_break_high60=False,
    ),
}

def compute_open_board_count(df5m: pd.DataFrame, limit_up: float, tick: float) -> int:
    if df5m is None or df5m.empty:
        return 999
    close = df5m["Close"].astype(float).values
    high  = df5m["High"].astype(float).values
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
    bars_today: Dict[str, pd.DataFrame],
    base_idx: pd.DataFrame,
    meta_map: Dict[str, StockMeta],
    preset: dict,
    now_ts: datetime
) -> pd.DataFrame:
    if base_idx is None or base_idx.empty or not bars_today:
        return pd.DataFrame()

    expected_bar_idx = bars_expected_5m(now_ts) - 1
    out_rows = []

    for code, df5m in bars_today.items():
        if code not in base_idx.index:
            continue
        b = base_idx.loc[code]

        yday_close = b.get("yday_close", None)
        if pd.isna(yday_close):
            continue
        yday_close = float(yday_close)

        exp_min = max(10, int(0.5 * bars_expected_5m(now_ts)))
        if len(df5m) < exp_min:
            continue

        limit_pct = float(b.get("limit_class_pct", 0.10)) if pd.notna(b.get("limit_class_pct", None)) else 0.10
        limit_up = calc_limit_up(yday_close, limit_pct)
        tick = tw_tick(limit_up)

        last = float(df5m["Close"].iloc[-1])
        day_high = float(df5m["High"].max())
        day_low  = float(df5m["Low"].min())
        vol_shares = float(df5m["Volume"].sum())
        vol_lots = int(vol_shares / 1000)

        near_limit = last >= (limit_up - preset["near_limit_ticks"] * tick)
        if not near_limit:
            continue

        rng = max(1e-9, day_high - day_low)
        close_pos = (last - day_low) / rng
        pullback = (day_high - last) / max(1e-9, day_high)

        # exclusions
        ret_1d = float(b.get("ret_1d", 0.0)) if pd.notna(b.get("ret_1d", None)) else 0.0
        yday_was_limit_like = ret_1d >= (19.0 if limit_pct == 0.20 else 9.5)
        had_hype_10d = bool(b.get("had_hype_10d", False))
        yday_bad = bool(b.get("yday_vol_spike", False)) and (float(b.get("yday_upper_wick_ratio", 0.0)) >= 0.35) and (ret_1d >= 6.0)

        base_len = int(b.get("base_len_days", 0)) if pd.notna(b.get("base_len_days", None)) else 0
        base_tight = float(b.get("base_tight_score", 0.0)) if pd.notna(b.get("base_tight_score", None)) else 0.0

        ret_5d = float(b.get("ret_5d", 0.0)) if pd.notna(b.get("ret_5d", None)) else 0.0
        atr20  = float(b.get("atr20_pct", 999.0)) if pd.notna(b.get("atr20_pct", None)) else 999.0

        break_high60 = True
        if preset.get("require_break_high60", False):
            high60 = float(b.get("high60_ex1", 0.0)) if pd.notna(b.get("high60_ex1", None)) else 0.0
            break_high60 = limit_up >= (high60 * 0.995)

        # same-time profile vol ratio
        vol_ma20 = float(b.get("vol_ma20_shares", 0.0)) if pd.notna(b.get("vol_ma20_shares", None)) else 0.0
        if vol_ma20 > 0:
            yf_symbol = str(b.get("yf_symbol", "")).strip()
            prof = build_intraday_volume_profile(yf_symbol, lookback_days=20)
            frac = float(prof[min(53, max(0, expected_bar_idx))])
            expected_vol = vol_ma20 * frac
            vol_ratio_profile = vol_shares / (expected_vol + 1e-9)
        else:
            vol_ratio_profile = 0.0

        # lock quality
        open_board = compute_open_board_count(df5m, limit_up, tick)
        lastN_hit  = lastN_near_limit(df5m, limit_up, tick, preset["require_lastN_near_limit"])

        dist_pct = (limit_up - last) / max(1e-9, limit_up) * 100.0
        chg_pct  = (last / yday_close - 1.0) * 100.0

        cond = (
            (close_pos >= float(preset["min_close_pos"])) and
            (pullback <= float(preset["max_pullback"])) and
            (vol_lots >= int(preset["min_cum_lots"])) and
            (vol_ratio_profile >= float(preset["min_vol_ratio_profile"])) and
            (not yday_was_limit_like) and
            (not had_hype_10d) and
            (not yday_bad) and
            (ret_5d <= float(preset["max_ret_5d"])) and
            (atr20 <= float(preset["max_atr20"])) and
            (base_len >= int(preset["min_base_len"])) and
            (base_tight >= float(preset["min_base_tight"])) and
            break_high60 and
            (lastN_hit >= int(preset["require_lastN_near_limit"])) and
            (open_board <= int(preset["max_open_board"]))
        )
        if not cond:
            continue

        m = meta_map.get(code, StockMeta(code=code, name="", market="未分類", industry="未分類", yf_symbol=""))
        # score (0..100)
        score = 0.0
        score += 35.0 * min(1.0, max(0.0, (close_pos - 0.85) / 0.15))
        score += 25.0 * min(1.0, max(0.0, (0.008 - pullback) / 0.008))
        score += 25.0 * min(1.0, max(0.0, (vol_ratio_profile - 1.5) / 3.0))
        score += 10.0 * min(1.0, max(0.0, (float(base_len) - 10) / 40.0))
        score -= min(10.0, (0 if open_board == 999 else open_board) * 3.0)
        if break_high60:
            score += 5.0
        score = float(max(0.0, min(100.0, score)))

        out_rows.append({
            "排名": 0,
            "代號": code,
            "名稱": m.name,
            "市場": m.market,
            "族群": m.industry,
            "現價": last,
            "漲停價": limit_up,
            "距離漲停(%)": dist_pct,
            "較昨收(%)": chg_pct,
            "累積量(張)": vol_lots,
            "盤中爆量倍數": float(vol_ratio_profile),
            "收在高檔(0-1)": float(close_pos),
            "回落(%)": float(pullback * 100.0),
            "開板次數(5m近似)": None if open_board == 999 else int(open_board),
            "最後N根貼板": int(lastN_hit),
            "基底天數": int(base_len),
            "基底緊縮分": float(base_tight),
            "ATR20(%)": float(atr20),
            "近5日漲幅(%)": float(ret_5d),
            "連板潛力分": float(score),
        })

    if not out_rows:
        return pd.DataFrame()

    out = pd.DataFrame(out_rows).sort_values(
        ["連板潛力分", "距離漲停(%)", "盤中爆量倍數"],
        ascending=[False, True, False]
    ).reset_index(drop=True)
    out["排名"] = range(1, len(out) + 1)
    return out

def scan_sector_resonance_radar(
    bars_today: Dict[str, pd.DataFrame],
    base_idx: pd.DataFrame,
    meta_map: Dict[str, StockMeta],
    now_ts: datetime,
    heat_threshold: float = 62.0,
    near_ticks: int = 2,
    top_sectors: int = 10,
    top_members: int = 12
):
    if not bars_today or base_idx is None or base_idx.empty:
        return pd.DataFrame(), {}

    bar_n = bars_expected_5m(now_ts)
    frac_lin = max(0.2, bar_n / 54.0)

    rows = []
    for code, df5m in bars_today.items():
        if code not in base_idx.index:
            continue

        b = base_idx.loc[code]
        yday_close = b.get("yday_close", None)
        if pd.isna(yday_close):
            continue
        yday_close = float(yday_close)

        limit_pct = float(b.get("limit_class_pct", 0.10)) if pd.notna(b.get("limit_class_pct", None)) else 0.10
        limit_up = calc_limit_up(yday_close, limit_pct)
        tick = tw_tick(limit_up)

        last = float(df5m["Close"].iloc[-1])
        day_high = float(df5m["High"].max())
        day_low  = float(df5m["Low"].min())
        vol_shares = float(df5m["Volume"].sum())
        vol_lots = int(vol_shares / 1000)

        dist_to_limit_pct = (limit_up - last) / max(1e-9, limit_up) * 100.0
        near_limit = last >= (limit_up - near_ticks * tick)

        rng = max(1e-9, day_high - day_low)
        close_pos = (last - day_low) / rng
        pullback = (day_high - last) / max(1e-9, day_high)

        vol_ma20 = float(b.get("vol_ma20_shares", 0.0)) if pd.notna(b.get("vol_ma20_shares", None)) else 0.0
        exp_vol = vol_ma20 * frac_lin if vol_ma20 > 0 else 0.0
        vol_ratio = (vol_shares / (exp_vol + 1e-9)) if exp_vol > 0 else 0.0

        chg_pct = (last / yday_close - 1.0) * 100.0

        heat = 0.0
        heat += 35.0 if near_limit else max(0.0, 35.0 * (1.0 - dist_to_limit_pct / 5.0))
        heat += 20.0 * min(1.0, max(0.0, chg_pct / (limit_pct * 100.0)))
        heat += 20.0 * min(1.0, max(0.0, (vol_ratio - 1.0) / 2.5))
        heat += 15.0 * min(1.0, max(0.0, (close_pos - 0.55) / 0.45))
        heat -= 20.0 * min(1.0, max(0.0, pullback / 0.01))
        heat = float(max(0.0, min(100.0, heat)))

        m = meta_map.get(code, StockMeta(code=code, name="", market="未分類", industry="未分類", yf_symbol=""))
        rows.append({
            "代號": code,
            "名稱": m.name,
            "市場": m.market,
            "族群": m.industry if m.industry else "未分類",
            "熱度分": heat,
            "熱": heat >= float(heat_threshold),
            "貼板": bool(near_limit),
            "距離漲停(%)": float(dist_to_limit_pct),
            "較昨收(%)": float(chg_pct),
            "盤中爆量倍數(快)": float(vol_ratio),
            "累積量(張)": int(vol_lots),
        })

    if not rows:
        return pd.DataFrame(), {}

    stock_df = pd.DataFrame(rows)
    g = stock_df.groupby("族群", dropna=False)

    sector = g.agg(
        掃描檔數=("代號", "count"),
        熱檔數=("熱", "sum"),
        貼板數=("貼板", "sum"),
        平均熱度=("熱度分", "mean"),
        最高熱度=("熱度分", "max"),
        平均爆量=("盤中爆量倍數(快)", "mean"),
    ).reset_index().rename(columns={"族群": "族群名稱"})

    sector["共振分"] = (
        sector["熱檔數"] * 18.0 +
        sector["貼板數"] * 10.0 +
        sector["平均熱度"] * 0.35 +
        sector["最高熱度"] * 0.25 +
        sector["平均爆量"] * 2.0
    ).clip(0, 100)

    sector = sector.sort_values(["共振分", "熱檔數", "貼板數", "最高熱度"], ascending=False).head(int(top_sectors)).reset_index(drop=True)
    sector.insert(0, "排名", range(1, len(sector) + 1))

    members = {}
    for sec in sector["族群名稱"].tolist():
        sub = stock_df[stock_df["族群名稱"] == sec] if "族群名稱" in stock_df.columns else stock_df[stock_df["族群"] == sec]
        sub = sub.copy()
        sub = sub.sort_values(["熱度分", "貼板", "距離漲停(%)"], ascending=[False, False, True]).head(int(top_members)).reset_index(drop=True)
        sub.insert(0, "排名", range(1, len(sub) + 1))
        members[sec] = sub

    return sector, members

# =========================
# Pretty HTML table
# =========================
def render_table_html(title: str, df: pd.DataFrame, columns: list[str], height: int = 560) -> None:
    if df is None or df.empty:
        st.info("沒有資料。")
        return

    def fmt(v):
        if v is None: return ""
        if isinstance(v, float) and math.isnan(v): return ""
        if isinstance(v, bool): return "✓" if v else ""
        if isinstance(v, int): return f"{v:,}"
        if isinstance(v, float): return f"{v:,.2f}"
        return html.escape(str(v))

    head = "".join([f"<th>{html.escape(c)}</th>" for c in columns])
    rows = []
    for _, r in df.iterrows():
        tds = [f"<td>{fmt(r.get(c,''))}</td>" for c in columns]
        rows.append("<tr>" + "".join(tds) + "</tr>")

    html_doc = f"""
    <!doctype html><html><head><meta charset="utf-8"/>
    <style>
      :root {{ --text:#e5e7eb; --line:rgba(148,163,184,.16); --hi: rgba(148,163,184,.08); }}
      body {{ margin:0; background: transparent; color: var(--text);
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Noto Sans TC","PingFang TC","Microsoft JhengHei", Arial, sans-serif; }}
      .title {{ padding: 0 0 8px 4px; font-weight: 900; color: #e5e7eb; }}
      .wrap {{ max-height:{height}px; overflow:auto; border: 1px solid var(--line);
        border-radius: 16px; background: rgba(15,17,22,.70); }}
      table {{ width:100%; border-collapse: separate; border-spacing:0; font-size: 12.5px; }}
      thead th {{
        position: sticky; top:0; z-index:2;
        text-align:left; padding: 11px 10px;
        background: rgba(15,17,22,.98); border-bottom: 1px solid var(--line);
        white-space: nowrap; font-weight: 900;
      }}
      tbody td {{
        padding: 10px 10px; border-bottom: 1px solid rgba(148,163,184,.10);
        background: rgba(11,13,18,.92); white-space: nowrap;
      }}
      tbody tr:hover td {{ background: var(--hi); }}
    </style></head>
    <body>
      <div class="title">{html.escape(title)}</div>
      <div class="wrap">
        <table>
          <thead><tr>{head}</tr></thead>
          <tbody>{''.join(rows)}</tbody>
        </table>
      </div>
    </body></html>
    """
    components.html(html_doc, height=height + 70, scrolling=False)

# =========================
# SIDEBAR
# =========================
st.sidebar.markdown("### 🧠 懶人設定")
mode = st.sidebar.selectbox("模式", list(PRESETS.keys()), index=1)
market_mode = st.sidebar.selectbox("市場", ["只掃上市（TWSE）", "上市 + 上櫃（TWSE+TPEX）"], index=0)
pool_mode = st.sidebar.selectbox("股票池", ["流動性預篩（推薦）", "全量（較慢）"], index=0)
st.sidebar.markdown("---")
run_scan = st.sidebar.button("🧊 立即掃描（含族群共振）", use_container_width=True)
clear_cache = st.sidebar.button("🔄 清快取", use_container_width=True)

# =========================
# HEADER
# =========================
now_ts = now_taipei()
elapsed = minutes_elapsed_in_session(now_ts)

st.markdown(f"""
<div class="header-wrap">
  <div>
    <h1 class="title">起漲戰情室</h1>
    <div class="subtitle">主軸：抓「第一根漲停」＋看「族群共振」找瑞軒型（後續 2～7 根機率更高）</div>
  </div>
  <div class="pill"><span class="dot"></span> 台北時間 <b>{now_ts.strftime('%H:%M:%S')}</b>　盤中進度 <b>{elapsed}/270</b></div>
</div>
""", unsafe_allow_html=True)

if not is_market_time(now_ts):
    st.info("目前非盤中：掃描會使用『最後可取得的 5m 盤中資料快照』，準確度會比盤中低。")

if clear_cache:
    fetch_universe.clear()
    build_daily_baseline.clear()
    fetch_intraday_bars_5m.clear()
    build_intraday_volume_profile.clear()
    st.success("已清除快取（清單/日線/盤中/量能曲線）。")

# =========================
# Load universe (NO TWSE/MOPS)
# =========================
include_tpex = (market_mode == "上市 + 上櫃（TWSE+TPEX）")

try:
    uni = fetch_universe(include_tpex=include_tpex)
except Exception as e:
    st.error(f"抓股票清單失敗：{e}\n\n建議：在 requirements.txt 加上 twstock，讓清單完全從 twstock.codes 取得（不碰 TWSE/MOPS）。")
    st.stop()

# build meta map
meta_map: Dict[str, StockMeta] = {}
symbol_rows: List[Tuple[str, str]] = []
for _, r in uni.iterrows():
    code = str(r["code"])
    meta_map[code] = StockMeta(
        code=code,
        name=str(r["name"]),
        market=str(r["market"]),
        industry=str(r["industry"]) if str(r["industry"]).strip() else "未分類",
        yf_symbol=str(r["yf_symbol"]),
    )
    symbol_rows.append((code, str(r["yf_symbol"])))

# =========================
# baseline
# =========================
with st.spinner("建立日線基準（10%/20%判斷、基底、排雷）..."):
    base_df = build_daily_baseline(symbol_rows)

if base_df is None or base_df.empty:
    st.error("日線基準抓不到（yfinance 可能被限流/網路限制）。")
    st.stop()

base_idx = base_df.set_index("code", drop=False)

# liquidity prefilter
symbol_rows_to_scan = symbol_rows
universe_label = f"全量（{len(symbol_rows):,} 檔）"

if pool_mode.startswith("流動性預篩"):
    liq_thr = 500_000  # 500張/日（shares）
    try:
        kept_codes = base_idx[base_idx["vol_ma20_shares"].astype(float) >= liq_thr]["code"].tolist()
        if len(kept_codes) >= 80:
            symbol_rows_to_scan = [(c, meta_map[c].yf_symbol) for c in kept_codes if c in meta_map]
            universe_label = f"流動性預篩（{len(symbol_rows_to_scan):,} 檔）"
    except Exception:
        pass

preset = PRESETS[mode]
heat_thr = 70.0 if "保守" in mode else (55.0 if "積極" in mode else 62.0)

st.markdown(f"""
<div class="grid">
  <div class="card"><div class="k">市場</div><div class="v">{html.escape(market_mode)}</div></div>
  <div class="card"><div class="k">股票池</div><div class="v">{html.escape(universe_label)}</div></div>
  <div class="card"><div class="k">掃描目標</div><div class="v">第一根漲停</div></div>
  <div class="card"><div class="k">模式</div><div class="v">{mode.split('（')[0]}<small>（內建參數）</small></div></div>
</div>
""", unsafe_allow_html=True)

st.markdown("""
<div class="banner">
<b>按一次「立即掃描」會得到兩份輸出：</b><br>
① 第一根漲停候選（按連板潛力分排序）<br>
② 族群共振 Radar（今天哪個族群集體升溫/貼板）<br>
</div>
""", unsafe_allow_html=True)

# =========================
# RUN SCAN
# =========================
if run_scan:
    with st.spinner("抓取盤中 5m（分批）..."):
        bars_today = fetch_intraday_bars_5m(symbol_rows_to_scan, batch_size=60)

    if not bars_today:
        st.error("盤中 5m 抓不到資料（yfinance intraday 可能被限制/延遲）。")
        st.stop()

    with st.spinner("掃描第一根漲停候選（含同時間量能曲線/開板近似/基底排雷）..."):
        result_limit = scan_first_limitup_continuation(
            bars_today=bars_today,
            base_idx=base_idx,
            meta_map=meta_map,
            preset=preset,
            now_ts=now_ts
        )

    st.markdown("<div class='hr'></div>", unsafe_allow_html=True)
    st.subheader("🚀 第一根漲停候選（連板潛力）")

    if result_limit is None or result_limit.empty:
        st.warning("目前沒有符合『第一根漲停＋體質濾網』的候選。")
    else:
        found = len(result_limit)
        st.success(f"掃到 {found} 檔候選（已按連板潛力分排序）")
        topn = result_limit.head(12).copy()
        q75 = float(topn["連板潛力分"].quantile(0.75)) if len(topn) >= 4 else 80.0
        cols = st.columns(4)

        for i, (_, r) in enumerate(topn.iterrows(), start=1):
            with cols[(i - 1) % 4]:
                score = float(r["連板潛力分"])
                tag = "🔒 幾乎鎖死" if score >= q75 else "👀 候選"
                st.markdown(f"""
<div class="card">
  <div class="metric">
    <div class="left">
      <div class="label">#{i} <span class="tag">{tag}</span></div>
      <div class="code">{html.escape(str(r['代號']))}</div>
      <div class="name">{html.escape(str(r['名稱']))}</div>
    </div>
    <div style="text-align:right">
      <div class="price">{float(r['現價']):.2f}</div>
      <div class="chg">漲停 {float(r['漲停價']):.2f}｜距離 {float(r['距離漲停(%)']):.2f}%</div>
    </div>
  </div>
  <div class="hr"></div>
  <div class="small-note">市場：{html.escape(str(r['市場']))}｜族群：{html.escape(str(r['族群']))}</div>
  <div class="small-note">較昨收：{float(r['較昨收(%)']):.2f}% ｜ 爆量：{float(r['盤中爆量倍數']):.2f}x</div>
  <div class="small-note">量：{int(r['累積量(張)']):,} 張 ｜ 分數：{score:.1f}</div>
</div>
""", unsafe_allow_html=True)

        with st.expander("📋 完整榜單（美化表格）", expanded=True):
            cols_show = [
                "排名","代號","名稱","市場","族群","現價","漲停價","距離漲停(%)","較昨收(%)",
                "累積量(張)","盤中爆量倍數","收在高檔(0-1)","回落(%)",
                "開板次數(5m近似)","最後N根貼板","基底天數","近5日漲幅(%)","ATR20(%)","連板潛力分"
            ]
            render_table_html("第一根漲停候選", result_limit, cols_show, height=580)

    st.markdown("<div class='hr'></div>", unsafe_allow_html=True)
    st.subheader("🧭 族群共振 Radar（獨立掃描）")

    with st.spinner("掃描族群共振（整個股票池：誰在一起熱、一起貼板）..."):
        sector_rank, sector_members = scan_sector_resonance_radar(
            bars_today=bars_today,
            base_idx=base_idx,
            meta_map=meta_map,
            now_ts=now_ts,
            heat_threshold=heat_thr,
            near_ticks=2,
            top_sectors=10,
            top_members=12,
        )

    if sector_rank is None or sector_rank.empty:
        st.info("目前掃不到族群共振（可能盤中資料不足或資料源被限制）。")
    else:
        cols = st.columns(4)
        for i, (_, r) in enumerate(sector_rank.head(8).iterrows(), start=1):
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

        with st.expander("📋 族群共振排行榜（Top 10）", expanded=True):
            render_table_html(
                "族群共振排行榜",
                sector_rank,
                ["排名","族群名稱","共振分","熱檔數","貼板數","掃描檔數","平均熱度","最高熱度","平均爆量"],
                height=420
            )

        st.markdown("<div class='hr'></div>", unsafe_allow_html=True)
        st.subheader("🔥 各族群 Top 成員（熱度最高）")
        for sec in sector_rank["族群名稱"].tolist():
            sub = sector_members.get(sec, pd.DataFrame())
            if sub is None or sub.empty:
                continue
            with st.expander(f"📌 {sec}（Top {len(sub)}）", expanded=False):
                render_table_html(
                    f"{sec} Top 成員",
                    sub,
                    ["排名","代號","名稱","市場","熱度分","熱","貼板","距離漲停(%)","較昨收(%)","盤中爆量倍數(快)","累積量(張)"],
                    height=420
                )

st.caption("清單來源已避開 TWSE/MOPS 網域（防止 THE PAGE CANNOT BE ACCESSED）。若你在 Streamlit Cloud，建議 requirements.txt 加上 twstock，清單就能完全本機取得。")
