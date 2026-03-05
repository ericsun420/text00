# app.py — 起漲戰情室（第一根漲停 + 連板潛力）＋🧭族群共振 Radar（獨立掃描）
# ✅ 盤中用 TWSE MIS 即時報價（避開 yfinance intraday 限流）
# ✅ 日線基準只對「盤中接近漲停 + 量大」的候選少數檔才去抓（避免卡住）
#
# 安裝：
#   pip install -U streamlit pandas yfinance requests urllib3
# （建議）pip install -U twstock   # 有裝會更穩：清單不需要連任何 TWSE/MOPS 網域
#
# 執行：
#   streamlit run app.py

import io
import math
import time
import html
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dtime
from typing import Dict, List, Tuple, Optional

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
.stSelectbox>div>div, .stTextInput>div>div, .stNumberInput>div>div{
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

def session_fraction(ts: datetime) -> float:
    # 盤中量能粗略比例（不用 5m profile，先用線性近似，夠快）
    m = minutes_elapsed_in_session(ts)
    return max(0.2, min(1.0, m / 270.0))

# =========================
# TICK / HELPERS
# =========================
def tw_tick(price: float) -> float:
    if price < 10: return 0.01
    if price < 50: return 0.05
    if price < 100: return 0.10
    if price < 500: return 0.50
    if price < 1000: return 1.00
    return 5.00

def fnum(x, default=None):
    try:
        if x is None: return default
        s = str(x).strip()
        if s in ("", "-", "null", "None"): return default
        return float(s)
    except Exception:
        return default

def split_underscore_nums(s: str) -> List[float]:
    if not s: return []
    parts = [p for p in str(s).split("_") if p and p != "-"]
    out = []
    for p in parts:
        v = fnum(p, None)
        if v is not None:
            out.append(v)
    return out

# =========================
# Universe
# =========================
@dataclass
class Meta:
    code: str
    name: str
    market: str      # 上市/上櫃
    industry: str    # group
    ex: str          # 'tse' or 'otc'
    yf_symbol: str   # 2330.TW / 3081.TWO

@st.cache_data(ttl=24*3600, show_spinner=False)
def load_universe(include_tpex: bool) -> Dict[str, Meta]:
    """
    優先：twstock.codes（本機）
    備援：GitHub raw twstock csv
    """
    # 1) twstock local
    try:
        import twstock  # type: ignore
        meta = {}
        for _, info in twstock.codes.items():
            c = str(getattr(info, "code", "")).strip()
            n = str(getattr(info, "name", "")).strip()
            m = str(getattr(info, "market", "")).strip()
            g = str(getattr(info, "group", "")).strip()
            if not re.match(r"^\d{4,6}$", c or ""):
                continue
            if m in ("上市", "TWSE"):
                ex = "tse"
                yf_symbol = f"{c}.TW"
                meta[c] = Meta(code=c, name=n, market="上市", industry=g or "未分類", ex=ex, yf_symbol=yf_symbol)
            elif m in ("上櫃", "TPEX"):
                if not include_tpex:
                    continue
                ex = "otc"
                yf_symbol = f"{c}.TWO"
                meta[c] = Meta(code=c, name=n, market="上櫃", industry=g or "未分類", ex=ex, yf_symbol=yf_symbol)
        if len(meta) > 300:
            return meta
    except Exception:
        pass

    # 2) GitHub raw
    def fetch_csv(url: str) -> pd.DataFrame:
        r = requests.get(url, timeout=45, allow_redirects=True, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        text = r.text.replace("\r\n", "\n").replace("\r", "\n")
        df = pd.read_csv(io.StringIO(text), dtype=str, engine="python", on_bad_lines="skip")
        df.columns = [str(c).strip() for c in df.columns]
        return df

    twse_raw = "https://raw.githubusercontent.com/mlouielu/twstock/refs/heads/master/twstock/codes/twse_equities.csv"
    tpex_raw = "https://raw.githubusercontent.com/mlouielu/twstock/refs/heads/master/twstock/codes/tpex_equities.csv"

    meta: Dict[str, Meta] = {}

    df1 = fetch_csv(twse_raw)
    # twstock csv usually has columns: type, code, name, ISIN, start, market, group, CFI
    if "code" not in df1.columns:
        df1 = pd.read_csv(io.StringIO(df1.to_csv(index=False, header=False)), header=None, dtype=str, engine="python")
        df1.columns = ["type","code","name","ISIN","start","market","group","CFI"][:df1.shape[1]]

    for _, r in df1.iterrows():
        c = str(r.get("code","")).strip()
        n = str(r.get("name","")).strip()
        g = str(r.get("group","")).strip()
        if re.match(r"^\d{4,6}$", c or ""):
            meta[c] = Meta(code=c, name=n, market="上市", industry=g or "未分類", ex="tse", yf_symbol=f"{c}.TW")

    if include_tpex:
        df2 = fetch_csv(tpex_raw)
        if "code" not in df2.columns:
            df2 = pd.read_csv(io.StringIO(df2.to_csv(index=False, header=False)), header=None, dtype=str, engine="python")
            df2.columns = ["type","code","name","ISIN","start","market","group","CFI"][:df2.shape[1]]
        for _, r in df2.iterrows():
            c = str(r.get("code","")).strip()
            n = str(r.get("name","")).strip()
            g = str(r.get("group","")).strip()
            if re.match(r"^\d{4,6}$", c or ""):
                meta[c] = Meta(code=c, name=n, market="上櫃", industry=g or "未分類", ex="otc", yf_symbol=f"{c}.TWO")

    if len(meta) < 200:
        raise ValueError("清單來源不足（twstock/GitHub 皆失敗）。建議安裝 twstock。")

    return meta

# =========================
# MIS client (realtime quote)
# =========================
class MISClient:
    def __init__(self):
        self.s = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
            "Referer": "https://mis.twse.com.tw/stock/index.jsp",
            "Connection": "keep-alive",
        }
        self.inited = False

    def init(self):
        if self.inited:
            return
        # 先打 fibest 拿 cookie（很多環境需要）
        url = "https://mis.twse.com.tw/stock/fibest.jsp?lang=zh_tw"
        self.s.get(url, headers=self.headers, timeout=20, verify=False)
        self.inited = True

    def get_quotes(self, ex_ch_list: List[str]) -> List[dict]:
        """
        ex_ch example: tse_2330.tw  or otc_3081.tw
        can join by '|'
        """
        self.init()
        if not ex_ch_list:
            return []

        ex_ch = "|".join(ex_ch_list)
        # 有些伺服器對 URL encode 很敏感，這裡強制用 %7c
        ex_ch = ex_ch.replace("|", "%7c")
        url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={ex_ch}&json=1&delay=0&_={int(time.time()*1000)}"
        r = self.s.get(url, headers=self.headers, timeout=25, verify=False)
        r.raise_for_status()
        data = r.json()
        arr = data.get("msgArray") or []
        return arr

@st.cache_data(ttl=6, show_spinner=False)
def fetch_mis_snapshot(meta_items: List[Meta], batch_size: int = 70) -> pd.DataFrame:
    """
    Returns df with fields:
      code,name,market,industry,last,prev_close,open,high,low,upper,lower,volume_shares,
      bid_p0,bid_v0,ask_p0,ask_v0,time
    """
    mis = MISClient()

    rows = []
    for i in range(0, len(meta_items), batch_size):
        chunk = meta_items[i:i+batch_size]
        ex_list = [f"{m.ex}_{m.code}.tw" for m in chunk]
        try:
            arr = mis.get_quotes(ex_list)
        except Exception:
            continue

        # map code -> meta
        mm = {m.code: m for m in chunk}
        for q in arr:
            c = str(q.get("c","")).strip()
            m = mm.get(c)
            if not m:
                continue

            # MIS fields:
            # z last, y prev close, o open, h high, l low, u limit up, w limit down, v volume
            last = fnum(q.get("z"), None)
            prev_close = fnum(q.get("y"), None)
            o = fnum(q.get("o"), None)
            h = fnum(q.get("h"), None)
            l = fnum(q.get("l"), None)
            u = fnum(q.get("u"), None)
            w = fnum(q.get("w"), None)
            v = fnum(q.get("v"), 0.0) or 0.0

            a_prices = split_underscore_nums(q.get("a",""))
            b_prices = split_underscore_nums(q.get("b",""))
            a_vols = split_underscore_nums(q.get("f",""))
            b_vols = split_underscore_nums(q.get("g",""))

            ask_p0 = a_prices[0] if len(a_prices) else None
            bid_p0 = b_prices[0] if len(b_prices) else None
            ask_v0 = a_vols[0] if len(a_vols) else None
            bid_v0 = b_vols[0] if len(b_vols) else None

            rows.append({
                "code": m.code,
                "name": m.name,
                "market": m.market,
                "industry": m.industry,
                "last": last,
                "prev_close": prev_close,
                "open": o,
                "high": h,
                "low": l,
                "upper": u,
                "lower": w,
                "volume_shares": v,
                "bid_p0": bid_p0, "bid_v0": bid_v0,
                "ask_p0": ask_p0, "ask_v0": ask_v0,
                "time": str(q.get("t","")).strip() or str(q.get("tlong","")).strip(),
            })
        time.sleep(0.05)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).drop_duplicates("code")
    # normalize lots
    df["volume_lots"] = (df["volume_shares"].fillna(0).astype(float) / 1000.0).astype(int)
    # % change
    df["chg_pct"] = (df["last"] / df["prev_close"] - 1.0) * 100.0
    df.loc[df["prev_close"].isna() | (df["prev_close"] == 0), "chg_pct"] = None
    return df

# =========================
# DAILY BASELINE (only for candidates)
# =========================
@st.cache_data(ttl=6*3600, show_spinner=False)
def build_daily_baseline_for_candidates(candidate_symbols: List[str]) -> pd.DataFrame:
    """
    candidate_symbols: ["2330.TW", "3081.TWO", ...]
    return indexed by symbol with features used for filtering.
    """
    if not candidate_symbols:
        return pd.DataFrame()

    # keep it lighter than 380d start: 260d is enough for 60d base + 20d MA + 10d hype
    period = "400d"

    batch = 60
    rows = []
    for i in range(0, len(candidate_symbols), batch):
        syms = candidate_symbols[i:i+batch]
        tickers = " ".join(syms)
        try:
            raw = yf.download(
                tickers=tickers,
                period=period,
                interval="1d",
                group_by="ticker",
                auto_adjust=False,
                threads=True,
                progress=False,
            )
        except Exception:
            continue

        for sym in syms:
            try:
                if isinstance(raw.columns, pd.MultiIndex):
                    if sym not in raw.columns.get_level_values(0):
                        continue
                    df = raw[sym].dropna().copy()
                else:
                    df = raw.dropna().copy()

                if df.empty or len(df) < 80:
                    continue

                close = df["Close"].astype(float)
                high = df["High"].astype(float)
                low  = df["Low"].astype(float)
                vol  = df["Volume"].astype(float)

                # vol MA20
                vol_ma20 = float(vol.rolling(20).mean().iloc[-1])

                # ATR20%
                prev_close = close.shift(1)
                tr1 = (high - low).abs()
                tr2 = (high - prev_close).abs()
                tr3 = (low - prev_close).abs()
                tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
                atr20 = float(tr.rolling(20).mean().iloc[-1])
                last_close = float(close.iloc[-1])
                atr20_pct = (atr20 / last_close) * 100.0 if last_close else None

                # 5d return
                ret_5d = (last_close / float(close.iloc[-6]) - 1.0) * 100.0 if len(close) >= 6 else None

                # last 10d hype
                hist_ret = close.pct_change().dropna()
                max_ret_10d = float(hist_ret.tail(10).max()) * 100.0 if len(hist_ret) >= 10 else None

                # yesterday distribution proxy
                y_open = float(df["Open"].iloc[-1])
                y_high = float(df["High"].iloc[-1])
                y_low  = float(df["Low"].iloc[-1])
                y_close = float(df["Close"].iloc[-1])
                y_range = max(1e-9, y_high - y_low)
                y_upper_wick_ratio = float((y_high - max(y_open, y_close)) / y_range)
                yday_vol_spike = bool(float(vol.iloc[-1]) >= 2.0 * vol_ma20) if vol_ma20 else False

                # base length (near MA20)
                ma20 = close.rolling(20).mean()
                near_ma20 = ((close / (ma20 + 1e-9) - 1.0).abs() <= 0.04)
                base_len_days = int(near_ma20.tail(60).sum())

                # base tight score
                range20 = float(high.rolling(20).max().iloc[-1] - low.rolling(20).min().iloc[-1])
                range60 = float(high.rolling(60).max().iloc[-1] - low.rolling(60).min().iloc[-1])
                range20_pct = (range20 / last_close) if last_close else 1.0
                range60_pct = (range60 / last_close) if last_close else 1.0
                base_tight_score = float(
                    (1.0 - min(1.0, range20_pct / (range60_pct + 1e-9))) * 0.6
                    + (1.0 - min(1.0, (atr20_pct or 999.0) / 8.0)) * 0.4
                )

                rows.append({
                    "yf_symbol": sym,
                    "vol_ma20_shares": vol_ma20,
                    "atr20_pct": atr20_pct,
                    "ret_5d": ret_5d,
                    "max_ret_10d": max_ret_10d,
                    "yday_upper_wick_ratio": y_upper_wick_ratio,
                    "yday_vol_spike": yday_vol_spike,
                    "base_len_days": base_len_days,
                    "base_tight_score": base_tight_score,
                })
            except Exception:
                continue
        time.sleep(0.05)

    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows).drop_duplicates("yf_symbol").set_index("yf_symbol")
    return out

# =========================
# FIRST LIMIT-UP + SECTOR RADAR
# =========================
PRESETS = {
    "保守（只抓幾乎鎖死、連板體質強）": dict(
        near_ticks=1,
        min_cum_lots=1500,
        min_close_pos=0.92,
        max_pullback_pct=0.35,     # %
        max_ret_5d=8.0,
        max_atr20=4.8,
        min_base_len=28,
        min_base_tight=0.55,
        min_vol_ratio=2.3,
        min_bid_lots=200,          # 限價買單(最佳買)至少幾張
        max_ask_lots=60,           # 限價賣單(最佳賣)最多幾張
    ),
    "標準（平衡：第一根漲停 + 連板機率）": dict(
        near_ticks=1,
        min_cum_lots=1200,
        min_close_pos=0.88,
        max_pullback_pct=0.55,
        max_ret_5d=12.0,
        max_atr20=6.5,
        min_base_len=18,
        min_base_tight=0.45,
        min_vol_ratio=1.8,
        min_bid_lots=120,
        max_ask_lots=120,
    ),
    "積極（多抓：允許盤中較不穩）": dict(
        near_ticks=2,
        min_cum_lots=800,
        min_close_pos=0.82,
        max_pullback_pct=0.90,
        max_ret_5d=18.0,
        max_atr20=8.5,
        min_base_len=10,
        min_base_tight=0.35,
        min_vol_ratio=1.4,
        min_bid_lots=60,
        max_ask_lots=250,
    ),
}

def compute_lock_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    使用 MIS 的 upper + bid/ask 來估「鎖板品質」
    """
    out = df.copy()

    def _tick(row):
        u = row.get("upper")
        return tw_tick(float(u)) if pd.notna(u) and u else 0.05

    out["tick"] = out.apply(_tick, axis=1)
    out["near_upper"] = (out["last"] >= (out["upper"] - out["tick"]))

    # close position / pullback using day high/low
    rng = (out["high"] - out["low"]).replace(0, float("nan"))
    out["close_pos"] = (out["last"] - out["low"]) / rng
    out["pullback_pct"] = ((out["high"] - out["last"]) / out["high"]) * 100.0

    # bid/ask lots at best
    out["bid_lots0"] = (out["bid_v0"].fillna(0).astype(float) / 1000.0)
    out["ask_lots0"] = (out["ask_v0"].fillna(0).astype(float) / 1000.0)

    # whether best bid price is at upper
    out["bid_at_upper"] = (out["bid_p0"].fillna(0).astype(float) >= (out["upper"].fillna(0).astype(float) - out["tick"]))
    # whether best ask price equals upper
    out["ask_at_upper"] = (out["ask_p0"].fillna(0).astype(float) >= (out["upper"].fillna(0).astype(float) - out["tick"]))

    return out

def build_sector_radar(snapshot: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """
    完全不靠日線：用 MIS snapshot 做族群共振（快、穩）
    """
    if snapshot is None or snapshot.empty:
        return pd.DataFrame(), {}

    df = snapshot.copy()
    df["heat"] = 0.0
    # 熱度：接近漲停 + 漲幅 + 量
    df["heat"] += df["near_upper"].astype(int) * 40.0
    df["heat"] += df["chg_pct"].fillna(0).clip(lower=0) * 2.0
    df["heat"] += (df["volume_lots"].fillna(0) / 2000.0).clip(upper=20) * 2.0
    df["heat"] += (df["bid_lots0"].fillna(0) / 300.0).clip(upper=10) * 2.0
    df["heat"] -= (df["pullback_pct"].fillna(0)).clip(lower=0, upper=2) * 8.0
    df["heat"] = df["heat"].clip(0, 100)

    df["hot"] = df["heat"] >= 65.0

    g = df.groupby("industry", dropna=False)
    sector = g.agg(
        掃描檔數=("code","count"),
        熱檔數=("hot","sum"),
        貼板數=("near_upper","sum"),
        平均熱度=("heat","mean"),
        最高熱度=("heat","max"),
        平均量=("volume_lots","mean")
    ).reset_index().rename(columns={"industry":"族群名稱"})

    sector["共振分"] = (
        sector["熱檔數"]*20.0 +
        sector["貼板數"]*8.0 +
        sector["平均熱度"]*0.35 +
        sector["最高熱度"]*0.25 +
        (sector["平均量"]/2000.0).clip(upper=5)*3.0
    ).clip(0, 100)

    sector = sector.sort_values(["共振分","熱檔數","貼板數","最高熱度"], ascending=False).head(10).reset_index(drop=True)
    sector.insert(0, "排名", range(1, len(sector)+1))

    members = {}
    for sec in sector["族群名稱"].tolist():
        sub = df[df["industry"] == sec].copy()
        sub = sub.sort_values(["heat","near_upper","volume_lots"], ascending=[False, False, False]).head(12).reset_index(drop=True)
        sub.insert(0, "排名", range(1, len(sub)+1))
        members[sec] = sub

    return sector, members

def render_table_html(title: str, df: pd.DataFrame, columns: List[str], height: int = 560) -> None:
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
    components.html(html_doc, height=height+70, scrolling=False)

# =========================
# Sidebar (simple)
# =========================
st.sidebar.markdown("### 🧠 懶人設定")
mode = st.sidebar.selectbox("模式", list(PRESETS.keys()), index=1)
market_mode = st.sidebar.selectbox("市場", ["只掃上市（TWSE）", "上市 + 上櫃（TWSE+TPEX）"], index=0)
min_lots = st.sidebar.number_input("最低盤中量（張）", min_value=200, max_value=20000, value=1200, step=100)
candidate_dist_pct = st.sidebar.number_input("候選距離漲停(%)（越小越嚴）", min_value=0.1, max_value=5.0, value=1.0, step=0.1)
st.sidebar.markdown("---")
run_scan = st.sidebar.button("🧊 立即掃描（MIS 即時）", use_container_width=True)
clear_cache = st.sidebar.button("🔄 清快取", use_container_width=True)

# =========================
# Header
# =========================
now_ts = now_taipei()
elapsed = minutes_elapsed_in_session(now_ts)

st.markdown(f"""
<div class="header-wrap">
  <div>
    <h1 class="title">起漲戰情室</h1>
    <div class="subtitle">主軸：抓「第一根漲停」＋「族群共振」找瑞軒型（後續 2～7 根機率更高）</div>
  </div>
  <div class="pill"><span class="dot"></span> 台北時間 <b>{now_ts.strftime('%H:%M:%S')}</b>　盤中進度 <b>{elapsed}/270</b></div>
</div>
""", unsafe_allow_html=True)

if not is_market_time(now_ts):
    st.info("目前非盤中：MIS 即時仍可取到，但邏輯更適合盤中 09:05~13:25。")

if clear_cache:
    load_universe.clear()
    fetch_mis_snapshot.clear()
    build_daily_baseline_for_candidates.clear()
    st.success("已清快取。")

# =========================
# Load universe
# =========================
include_tpex = (market_mode == "上市 + 上櫃（TWSE+TPEX）")
universe = load_universe(include_tpex)
meta_items = list(universe.values())

preset = PRESETS[mode]
st.markdown(f"""
<div class="grid">
  <div class="card"><div class="k">市場</div><div class="v">{html.escape(market_mode)}</div></div>
  <div class="card"><div class="k">模式</div><div class="v">{mode.split('（')[0]}<small>（內建）</small></div></div>
  <div class="card"><div class="k">最低量</div><div class="v">{int(min_lots):,}<small> 張</small></div></div>
  <div class="card"><div class="k">候選距離漲停</div><div class="v">{float(candidate_dist_pct):.1f}<small>%</small></div></div>
</div>
""", unsafe_allow_html=True)

st.markdown("""
<div class="banner">
<b>這版不再卡住：</b>盤中先用 MIS 一次抓全市場快照 → 先篩出「接近漲停 + 量大」的少數檔 → 才去抓日線基準（避免 yfinance 對全市場日線/盤中限流）。
</div>
""", unsafe_allow_html=True)

# =========================
# Run scan
# =========================
if run_scan:
    # 1) MIS snapshot for all (fast)
    with st.spinner("MIS：抓盤中即時快照（大量檔一次抓）..."):
        snap = fetch_mis_snapshot(meta_items, batch_size=70)

    if snap is None or snap.empty:
        st.error("MIS 即時資料抓不到（可能網路/環境限制）。")
        st.stop()

    snap = compute_lock_metrics(snap)

    # 2) sector radar (no daily baseline needed)
    st.markdown("<div class='hr'></div>", unsafe_allow_html=True)
    st.subheader("🧭 族群共振 Radar（獨立掃描）")

    sector_rank, sector_members = build_sector_radar(snap)
    if sector_rank.empty:
        st.info("目前沒有族群共振結果（資料不足）。")
    else:
        cols = st.columns(4)
        for i, (_, r) in enumerate(sector_rank.head(8).iterrows(), start=1):
            with cols[(i-1) % 4]:
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
                ["排名","族群名稱","共振分","熱檔數","貼板數","掃描檔數","平均熱度","最高熱度","平均量"],
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
                    ["排名","code","name","market","heat","hot","near_upper","chg_pct","volume_lots","bid_lots0","ask_lots0","pullback_pct"],
                    height=420
                )

    # 3) candidate prefilter (near limit + volume)
    st.markdown("<div class='hr'></div>", unsafe_allow_html=True)
    st.subheader("🚀 第一根漲停候選（先用 MIS 快篩，再抓日線基準）")

    # distance to upper (%)
    snap["dist_upper_pct"] = ((snap["upper"] - snap["last"]) / snap["upper"]) * 100.0
    snap.loc[snap["upper"].isna() | (snap["upper"] == 0), "dist_upper_pct"] = None

    pre = snap[
        (snap["volume_lots"] >= int(min_lots)) &
        (snap["dist_upper_pct"].fillna(999) <= float(candidate_dist_pct))
    ].copy()

    # fallback: if too few, relax a bit automatically
    if len(pre) < 15:
        pre = snap[
            (snap["volume_lots"] >= int(min_lots)) &
            (snap["dist_upper_pct"].fillna(999) <= max(2.0, float(candidate_dist_pct)))
        ].copy()

    if pre.empty:
        st.warning("目前沒有符合『量大 + 接近漲停』的候選（可以放寬距離漲停%或降低最低量）。")
        st.stop()

    # 4) build daily baseline ONLY for these few candidates
    # map code -> yf symbol from universe
    pre["yf_symbol"] = pre["code"].apply(lambda c: universe.get(str(c), Meta("", "", "", "", "tse", f"{c}.TW")).yf_symbol)
    cand_syms = pre["yf_symbol"].dropna().astype(str).unique().tolist()

    with st.spinner(f"抓日線基準（只算候選 {len(cand_syms)} 檔，不卡住）..."):
        base = build_daily_baseline_for_candidates(cand_syms)

    if base is None or base.empty:
        st.error("候選的日線基準抓不到（yfinance 仍可能被限流）。可以稍後再按一次，或把候選檔數再縮小。")
        st.stop()

    # 5) apply full filters (first-limit-up + continuation bias)
    frac = session_fraction(now_ts)
    rows = []
    for _, r in pre.iterrows():
        sym = str(r["yf_symbol"])
        b = base.loc[sym] if sym in base.index else None
        if b is None:
            continue

        # expected volume (linear)
        vol_ma20 = float(b.get("vol_ma20_shares", 0.0) or 0.0)
        vol_ratio = (float(r["volume_shares"]) / (vol_ma20 * frac + 1e-9)) if vol_ma20 > 0 else 0.0

        # exclude "not first": last 10d had big day
        # use limit pct from MIS upper/prev_close
        prev_close = float(r["prev_close"]) if pd.notna(r["prev_close"]) and r["prev_close"] else None
        upper = float(r["upper"]) if pd.notna(r["upper"]) and r["upper"] else None
        if not prev_close or not upper:
            continue
        limit_pct = (upper / prev_close - 1.0) * 100.0
        hype_thr = 19.0 if limit_pct > 15 else 9.5
        max_ret_10d = float(b.get("max_ret_10d", 0.0) or 0.0)
        had_hype_10d = (max_ret_10d >= hype_thr)

        # yesterday distribution proxy
        yday_bad = bool(b.get("yday_vol_spike", False)) and (float(b.get("yday_upper_wick_ratio", 0.0) or 0.0) >= 0.35) and (max_ret_10d >= 6.0)

        # lock by orderbook
        bid_lots0 = float(r.get("bid_lots0", 0.0) or 0.0)
        ask_lots0 = float(r.get("ask_lots0", 0.0) or 0.0)
        bid_at_upper = bool(r.get("bid_at_upper", False))
        # 如果 ask price 就在 upper 且 ask lots 很大，視為不夠鎖
        lock_quality_ok = bid_at_upper and (bid_lots0 >= preset["min_bid_lots"]) and (ask_lots0 <= preset["max_ask_lots"])

        # base & overheat
        ret_5d = float(b.get("ret_5d", 0.0) or 0.0)
        atr20 = float(b.get("atr20_pct", 999.0) or 999.0)
        base_len = int(b.get("base_len_days", 0) or 0)
        base_tight = float(b.get("base_tight_score", 0.0) or 0.0)

        close_pos = float(r.get("close_pos", 0.0) or 0.0)
        pullback_pct = float(r.get("pullback_pct", 999.0) or 999.0)

        # final cond
        cond = (
            bool(r.get("near_upper", False)) and
            (int(r["volume_lots"]) >= preset["min_cum_lots"]) and
            (close_pos >= preset["min_close_pos"]) and
            (pullback_pct <= preset["max_pullback_pct"]) and
            (not had_hype_10d) and
            (not yday_bad) and
            (ret_5d <= preset["max_ret_5d"]) and
            (atr20 <= preset["max_atr20"]) and
            (base_len >= preset["min_base_len"]) and
            (base_tight >= preset["min_base_tight"]) and
            (vol_ratio >= preset["min_vol_ratio"]) and
            lock_quality_ok
        )
        if not cond:
            continue

        # score
        score = 0.0
        score += 35.0 * min(1.0, max(0.0, (close_pos - 0.80) / 0.20))
        score += 20.0 * min(1.0, max(0.0, (preset["max_pullback_pct"] - pullback_pct) / max(0.1, preset["max_pullback_pct"])))
        score += 25.0 * min(1.0, max(0.0, (vol_ratio - 1.2) / 2.5))
        score += 10.0 * min(1.0, max(0.0, (base_len - 8) / 40.0))
        score += 10.0 * min(1.0, max(0.0, (bid_lots0 - preset["min_bid_lots"]) / max(1.0, preset["min_bid_lots"])))
        score = float(max(0.0, min(100.0, score)))

        rows.append({
            "排名": 0,
            "代號": r["code"],
            "名稱": r["name"],
            "市場": r["market"],
            "族群": r["industry"],
            "現價": r["last"],
            "漲停價": r["upper"],
            "距離漲停(%)": r["dist_upper_pct"],
            "較昨收(%)": r["chg_pct"],
            "累積量(張)": int(r["volume_lots"]),
            "盤中爆量倍數(線性)": float(vol_ratio),
            "收在高檔(0-1)": float(close_pos),
            "回落(%)": float(pullback_pct),
            "買一掛單(張)": float(bid_lots0),
            "賣一掛單(張)": float(ask_lots0),
            "基底天數": int(base_len),
            "基底緊縮分": float(base_tight),
            "ATR20(%)": float(atr20),
            "近5日漲幅(%)": float(ret_5d),
            "連板潛力分": float(score),
        })

    if not rows:
        st.warning("快篩候選有，但『完整濾網』沒有通過（你可以切到「積極」或放寬距離漲停% / 降低最低量）。")
        st.stop()

    res = pd.DataFrame(rows).sort_values(["連板潛力分","距離漲停(%)","盤中爆量倍數(線性)"], ascending=[False, True, False]).reset_index(drop=True)
    res["排名"] = range(1, len(res)+1)

    st.success(f"✅ 鎖到 {len(res)} 檔『第一根漲停』候選（已排序）")

    topn = res.head(12).copy()
    q75 = float(topn["連板潛力分"].quantile(0.75)) if len(topn) >= 4 else 80.0
    cols = st.columns(4)
    for i, (_, rr) in enumerate(topn.iterrows(), start=1):
        with cols[(i-1) % 4]:
            score = float(rr["連板潛力分"])
            tag = "🔒 幾乎鎖死" if score >= q75 else "👀 候選"
            st.markdown(f"""
<div class="card">
  <div class="metric">
    <div class="left">
      <div class="label">#{i} <span class="tag">{tag}</span></div>
      <div class="code">{html.escape(str(rr['代號']))}</div>
      <div class="name">{html.escape(str(rr['名稱']))}</div>
    </div>
    <div style="text-align:right">
      <div class="price">{float(rr['現價']):.2f}</div>
      <div class="chg">漲停 {float(rr['漲停價']):.2f}｜距離 {float(rr['距離漲停(%)']):.2f}%</div>
    </div>
  </div>
  <div class="hr"></div>
  <div class="small-note">買一：{float(rr['買一掛單(張)']):.0f} 張｜賣一：{float(rr['賣一掛單(張)']):.0f} 張</div>
  <div class="small-note">爆量：{float(rr['盤中爆量倍數(線性)']):.2f}x｜分數：{score:.1f}</div>
</div>
""", unsafe_allow_html=True)

    with st.expander("📋 完整榜單（美化表格）", expanded=True):
        cols_show = [
            "排名","代號","名稱","市場","族群","現價","漲停價","距離漲停(%)","較昨收(%)",
            "累積量(張)","盤中爆量倍數(線性)","收在高檔(0-1)","回落(%)",
            "買一掛單(張)","賣一掛單(張)",
            "基底天數","近5日漲幅(%)","ATR20(%)","基底緊縮分","連板潛力分"
        ]
        render_table_html("第一根漲停候選", res, cols_show, height=580)

st.caption("這版盤中不靠 yfinance 5m（避免限流），用 MIS 即時報價做全市場快照；日線只針對少量候選才抓，所以不會卡在『建立日線基準』。")


