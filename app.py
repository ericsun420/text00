# app.py  —  起漲戰情室（第一根漲停｜連板潛力｜冷酷黑灰｜卡片+美化表格｜不顯示 Running...）
# 直接整份複製貼上覆蓋 app.py 即可
# 需要套件：streamlit pandas yfinance requests lxml urllib3

import math
import time
import re
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
# Page & Theme
# =========================
st.set_page_config(page_title="起漲戰情室", page_icon="🧊", layout="wide")

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
.block-container{ padding-top: 1.2rem; padding-bottom: 2.2rem; }

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
  gap:18px; padding: 10px 4px 6px 4px;
}
.title{
  font-size: 44px; font-weight: 900; letter-spacing: .5px;
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
  margin: 14px 0 6px 0;
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

/* TOP signal card content */
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
.stSelectbox>div>div{
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
  font-weight: 800 !important;
}

.small-note{ color: var(--muted); font-size: 12px; }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

# =========================
# Helpers
# =========================
def now_taipei() -> datetime:
    return datetime.utcnow() + timedelta(hours=8)

def minutes_elapsed_in_session(ts: datetime) -> int:
    start = datetime.combine(ts.date(), dtime(9, 0))
    end = datetime.combine(ts.date(), dtime(13, 30))
    if ts < start:
        return 0
    if ts > end:
        return 270
    return int((ts - start).total_seconds() // 60)

def is_market_time(ts: datetime) -> bool:
    t = ts.time()
    return dtime(9, 0) <= t <= dtime(13, 30)

# --- Taiwan tick size (approx, good enough with 1-tick tolerance) ---
def tw_tick(price: float) -> float:
    if price < 10:
        return 0.01
    if price < 50:
        return 0.05
    if price < 100:
        return 0.1
    if price < 500:
        return 0.5
    if price < 1000:
        return 1.0
    return 5.0

def round_to_tick(x: float, tick: float) -> float:
    # round to nearest tick
    return round(round(x / tick) * tick, 2 if tick < 0.1 else 1 if tick < 1 else 0)

# =========================
# MOPS list (robust decode)
# =========================
def http_get_bytes(url: str, timeout: int = 40) -> bytes:
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        r.raise_for_status()
        return r.content
    except requests.exceptions.SSLError:
        r = requests.get(
            url.replace("http://", "https://"),
            headers=headers,
            timeout=timeout,
            allow_redirects=True,
            verify=False,
        )
        r.raise_for_status()
        return r.content

def decode_csv_bytes(b: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp950", "big5", "big5hkscs"):
        try:
            text = b.decode(enc)
        except Exception:
            continue
        if ("公司代號" in text) and ("公司簡稱" in text or "公司名稱" in text):
            return text
    return b.decode("cp950", errors="ignore")

@st.cache_data(ttl=24 * 3600, show_spinner=False)
def fetch_all_twse_listed_stocks() -> pd.DataFrame:
    url = "http://mopsfin.twse.com.tw/opendata/t187ap03_L.csv"
    b = http_get_bytes(url)
    csv_text = decode_csv_bytes(b)

    df = pd.read_csv(StringIO(csv_text), dtype=str, engine="python")
    df.columns = [str(c).strip() for c in df.columns]

    name_col = "公司簡稱" if "公司簡稱" in df.columns else ("公司名稱" if "公司名稱" in df.columns else None)
    if name_col is None or "公司代號" not in df.columns:
        raise ValueError(f"欄位異常：{list(df.columns)[:30]}")

    out = df[["公司代號", name_col]].rename(columns={"公司代號": "code", name_col: "name"}).copy()
    out["code"] = out["code"].astype(str).str.strip()
    out["name"] = out["name"].astype(str).str.strip()
    out = out[out["code"].str.match(r"^\d{4,6}$")].drop_duplicates("code").sort_values("code").reset_index(drop=True)
    return out

# =========================
# Daily baseline (always return expected columns)
# =========================
EXPECTED_BASE_COLS = [
    "code",
    "vol_ma20_shares",
    "high20",
    "high60",
    "ma60",
    "yday_close",
    "prev2_close",
    "yday_ret",
    "ret_5d",
    "atr20_pct",
]

def _drop_today_bar_if_exists(df: pd.DataFrame, today_date) -> pd.DataFrame:
    if df.empty:
        return df
    last_date = pd.Timestamp(df.index[-1]).date()
    if last_date == today_date:
        return df.iloc[:-1].copy()
    return df

@st.cache_data(ttl=6 * 3600, show_spinner=False)
def build_daily_baselines(codes: list[str]) -> pd.DataFrame:
    end_date = now_taipei().date()
    start = (now_taipei() - timedelta(days=260)).date().isoformat()

    batch = 60
    records = []

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

                df = _drop_today_bar_if_exists(df, end_date)
                if df.empty or len(df) < 90:
                    continue

                yday_close = float(df["Close"].iloc[-1])
                prev2_close = float(df["Close"].iloc[-2])

                # rolling highs (exclude yesterday to represent "breakout from base")
                high20 = df["High"].rolling(20).max().shift(1).iloc[-1]
                high60 = df["High"].rolling(60).max().shift(1).iloc[-1]

                ma60 = df["Close"].rolling(60).mean().iloc[-1]
                vol_ma20 = df["Volume"].rolling(20).mean().iloc[-1]

                yday_ret = (yday_close / prev2_close - 1.0) if prev2_close else None
                ret_5d = (yday_close / float(df["Close"].iloc[-6]) - 1.0) if len(df) >= 6 else None

                # ATR20%
                prev_close = df["Close"].shift(1)
                tr1 = (df["High"] - df["Low"]).abs()
                tr2 = (df["High"] - prev_close).abs()
                tr3 = (df["Low"] - prev_close).abs()
                tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
                atr20 = tr.rolling(20).mean().iloc[-1]
                atr20_pct = (float(atr20) / yday_close * 100.0) if yday_close else None

                records.append({
                    "code": c,
                    "vol_ma20_shares": float(vol_ma20),
                    "high20": float(high20) if pd.notna(high20) else None,
                    "high60": float(high60) if pd.notna(high60) else None,
                    "ma60": float(ma60) if pd.notna(ma60) else None,
                    "yday_close": yday_close,
                    "prev2_close": prev2_close,
                    "yday_ret": float(yday_ret) if yday_ret is not None else None,
                    "ret_5d": float(ret_5d) if ret_5d is not None else None,
                    "atr20_pct": float(atr20_pct) if atr20_pct is not None else None,
                })
            except Exception:
                continue

        time.sleep(0.05)

    if not records:
        return pd.DataFrame(columns=EXPECTED_BASE_COLS)

    out = pd.DataFrame(records).drop_duplicates("code")
    for c in EXPECTED_BASE_COLS:
        if c not in out.columns:
            out[c] = pd.NA
    return out[EXPECTED_BASE_COLS].copy()

# =========================
# Intraday snapshot (yfinance, 5m) — no ugly Running...
# =========================
@st.cache_data(ttl=20, show_spinner=False)
def fetch_intraday_snapshot_yf(codes: list[str], interval: str = "5m", batch_size: int = 30) -> pd.DataFrame:
    out = []
    for i in range(0, len(codes), batch_size):
        chunk = codes[i:i + batch_size]
        tickers = " ".join([f"{c}.TW" for c in chunk])

        try:
            raw = yf.download(
                tickers=tickers,
                period="1d",
                interval=interval,
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

                out.append({
                    "code": c,
                    "last": float(df["Close"].iloc[-1]),
                    "open": float(df["Open"].iloc[0]),
                    "day_high": float(df["High"].max()),
                    "day_low": float(df["Low"].min()),
                    "vol_lots": int(float(df["Volume"].sum()) / 1000),
                })
            except Exception:
                continue

        time.sleep(0.10)

    df = pd.DataFrame(out)
    if df.empty:
        return df
    return df.drop_duplicates("code")

# =========================
# Presets (for FIRST LIMIT-UP + continuation potential)
# =========================
PRESETS = {
    "保守（只抓幾乎鎖死、連板體質強）": dict(
        min_vol_ratio=2.8,
        min_cum_lots=1500,
        min_close_pos=0.92,
        max_pullback=0.0025,     # 0.25%
        max_ret_5d=0.08,         # 8%
        max_atr20_pct=4.5,       # 4.5% ATR
        require_break_high60=True,
    ),
    "標準（平衡：第一根漲停 + 連板機率）": dict(
        min_vol_ratio=2.2,
        min_cum_lots=1200,
        min_close_pos=0.90,
        max_pullback=0.0035,     # 0.35%
        max_ret_5d=0.12,         # 12%
        max_atr20_pct=6.0,
        require_break_high60=False,
    ),
    "積極（多抓：允許盤中較不穩）": dict(
        min_vol_ratio=1.6,
        min_cum_lots=800,
        min_close_pos=0.86,
        max_pullback=0.0060,     # 0.6%
        max_ret_5d=0.18,
        max_atr20_pct=8.0,
        require_break_high60=False,
    ),
}

def scan_first_limitup_with_continuation(
    quotes: pd.DataFrame,
    base: pd.DataFrame,
    now_ts: datetime,
    preset: dict,
) -> pd.DataFrame:
    if base is None or base.empty or quotes is None or quotes.empty:
        return pd.DataFrame()

    df = quotes.merge(base, on="code", how="inner").copy()
    if df.empty:
        return pd.DataFrame()

    # expected volume ratio (still linear, but good enough; can be improved to intraday profile later)
    elapsed = minutes_elapsed_in_session(now_ts)
    frac = max(0.2, max(1, min(270, elapsed)) / 270.0)
    df["expected_vol_shares_now"] = df["vol_ma20_shares"].astype(float) * frac
    df["cum_vol_shares"] = df["vol_lots"].astype(float) * 1000.0
    df["vol_ratio_now"] = df["cum_vol_shares"] / (df["expected_vol_shares_now"] + 1e-9)

    # limit-up percent: default 10%; if already >11.5% assume 20% rule
    df["ret_now"] = (df["last"] / df["yday_close"] - 1.0)
    df["limit_pct"] = df["ret_now"].apply(lambda x: 0.20 if pd.notna(x) and x > 0.115 else 0.10)

    # limit-up price with tick rounding (1-tick tolerance later)
    def _limit_up(row):
        prev = float(row["yday_close"])
        pct = float(row["limit_pct"])
        raw = prev * (1.0 + pct)
        tick = tw_tick(raw)
        return round_to_tick(raw, tick)

    df["limit_up"] = df.apply(_limit_up, axis=1)
    df["tick"] = df["limit_up"].apply(tw_tick)

    # near-limit / lock quality proxies
    rng = (df["day_high"] - df["day_low"]).replace(0, 1e-9)
    df["close_pos"] = (df["last"] - df["day_low"]) / rng
    df["pullback_from_high"] = (df["day_high"] - df["last"]) / (df["day_high"] + 1e-9)

    df["dist_to_limit_pct"] = (df["limit_up"] - df["last"]) / (df["limit_up"] + 1e-9) * 100.0
    df["near_limit"] = df["last"] >= (df["limit_up"] - df["tick"] * 1.0)  # within 1 tick

    # "first board" exclusions: yesterday not limit-up
    # use 10% as baseline for yesterday check; 1% tolerance
    df["yday_was_limit"] = df["yday_ret"].apply(lambda x: True if pd.notna(x) and x >= 0.095 else False)

    # base/tightness / overheat filters
    df["ret_5d"] = df["ret_5d"].fillna(0.0)
    df["atr20_pct"] = df["atr20_pct"].fillna(999.0)

    # breakout to reduce overhead supply (optional)
    df["break_high60"] = True
    if "high60" in df.columns:
        df["break_high60"] = df["limit_up"] >= (df["high60"].fillna(0) * 0.995)

    # MAIN FILTER: FIRST LIMIT-UP candidate (price-based)
    cond = (
        df["near_limit"]
        & (df["vol_ratio_now"] >= float(preset["min_vol_ratio"]))
        & (df["vol_lots"] >= int(preset["min_cum_lots"]))
        & (df["close_pos"] >= float(preset["min_close_pos"]))
        & (df["pullback_from_high"] <= float(preset["max_pullback"]))
        & (~df["yday_was_limit"])  # ✅ first board
        & (df["ret_5d"] <= float(preset["max_ret_5d"]))
        & (df["atr20_pct"] <= float(preset["max_atr20_pct"]))
    )
    if preset.get("require_break_high60", False):
        cond = cond & (df["break_high60"])

    out = df[cond].copy()
    if out.empty:
        return pd.DataFrame()

    # Continuation score (0-100): lock quality + volume quality + base quality + breakout
    # (all internal; user still only selects preset)
    def _score(r):
        s = 0.0

        # lock/ # lock/near limit
        s += 35.0 * min(1.0, max(0.0, (float(r["close_pos"]) - 0.85) / 0.15))
        s += 25.0 * min(1.0, max(0.0, (0.006 - float(r["pullback_from_high"])) / 0.006))

        # volume quality
        vr = float(r["vol_ratio_now"])
        s += 25.0 * min(1.0, max(0.0, (vr - 1.5) / 3.0))

        # base tightness / not overextended
        atr = float(r["atr20_pct"])
        s += 10.0 * min(1.0, max(0.0, (8.0 - atr) / 6.0))

        r5 = float(r["ret_5d"])
        s += 10.0 * min(1.0, max(0.0, (0.18 - r5) / 0.18))

        # breakout bonus
        if bool(r.get("break_high60", True)):
            s += 5.0

        return max(0.0, min(100.0, s))

    out["連板潛力分"] = out.apply(_score, axis=1)
    out = out.sort_values(["連板潛力分", "dist_to_limit_pct", "vol_ratio_now"], ascending=[False, True, False])

    show = out[[
        "code",
        "last",
        "limit_up",
        "dist_to_limit_pct",
        "vol_lots",
        "vol_ratio_now",
        "ret_now",
        "ret_5d",
        "atr20_pct",
        "連板潛力分",
    ]].copy()

    show.rename(columns={
        "code": "代號",
        "last": "現價",
        "limit_up": "漲停價",
        "dist_to_limit_pct": "距離漲停(%)",
        "vol_lots": "累積量(張)",
        "vol_ratio_now": "盤中爆量倍數",
        "ret_now": "較昨收(%)",
        "ret_5d": "近5日漲幅(%)",
        "atr20_pct": "ATR20(%)",
    }, inplace=True)

    show["較昨收(%)"] = show["較昨收(%)"] * 100.0
    show["近5日漲幅(%)"] = show["近5日漲幅(%)"] * 100.0
    return show.reset_index(drop=True)

# =========================
# Beautiful table via components.html
# =========================
def render_pretty_table(df: pd.DataFrame) -> None:
    if df.empty:
        st.info("沒有資料。")
        return

    def f2(x):
        try:
            return f"{float(x):,.2f}"
        except Exception:
            return str(x)

    def f0(x):
        try:
            return f"{int(float(x)):,}"
        except Exception:
            return str(x)

    rows = []
    for _, r in df.iterrows():
        rows.append(f"""
        <tr>
          <td class="center">{r.get('排名','')}</td>
          <td>{r.get('代號','')}</td>
          <td>{r.get('名稱','')}</td>
          <td class="num">{f2(r.get('現價',''))}</td>
          <td class="num">{f2(r.get('漲停價',''))}</td>
          <td class="num">{f2(r.get('距離漲停(%)',''))}</td>
          <td class="num">{f2(r.get('較昨收(%)',''))}</td>
          <td class="num">{f0(r.get('累積量(張)',''))}</td>
          <td class="num">{f2(r.get('盤中爆量倍數',''))}</td>
          <td class="num">{f2(r.get('近5日漲幅(%)',''))}</td>
          <td class="num">{f2(r.get('ATR20(%)',''))}</td>
          <td class="num">{f2(r.get('連板潛力分',''))}</td>
        </tr>
        """)

    html = f"""
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8"/>
      <style>
        :root {{
          --text:#e5e7eb;
          --muted:#9ca3af;
          --line:rgba(148,163,184,.16);
          --hi: rgba(148,163,184,.08);
        }}
        body {{
          margin:0;
          background: transparent;
          color: var(--text);
          font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Noto Sans TC", "PingFang TC", "Microsoft JhengHei", Arial, sans-serif;
        }}
        .wrap {{
          max-height: 560px;
          overflow:auto;
          border: 1px solid var(--line);
          border-radius: 16px;
          background: rgba(15,17,22,.70);
        }}
        table {{
          width:100%;
          border-collapse: separate;
          border-spacing: 0;
          font-size: 13px;
        }}
        thead th {{
          position: sticky;
          top: 0;
          z-index: 2;
          text-align: left;
          padding: 12px 12px;
          background: rgba(15,17,22,.98);
          color: var(--text);
          border-bottom: 1px solid var(--line);
          font-weight: 900;
          white-space: nowrap;
        }}
        tbody td {{
          padding: 11px 12px;
          border-bottom: 1px solid rgba(148,163,184,.10);
          color: var(--text);
          background: rgba(11,13,18,.92);
          white-space: nowrap;
        }}
        tbody tr:hover td {{ background: var(--hi); }}
        .num {{ text-align:right; font-variant-numeric: tabular-nums; }}
        .center {{ text-align:center; }}
      </style>
    </head>
    <body>
      <div class="wrap">
        <table>
          <thead>
            <tr>
              <th class="center">#</th>
              <th>代號</th>
              <th>名稱</th>
              <th class="num">現價</th>
              <th class="num">漲停價</th>
              <th class="num">距離漲停(%)</th>
              <th class="num">較昨收(%)</th>
              <th class="num">累積量(張)</th>
              <th class="num">盤中爆量倍數</th>
              <th class="num">近5日漲幅(%)</th>
              <th class="num">ATR20(%)</th>
              <th class="num">連板潛力分</th>
            </tr>
          </thead>
          <tbody>
            {''.join(rows)}
          </tbody>
        </table>
      </div>
    </body>
    </html>
    """
    components.html(html, height=610, scrolling=False)

# =========================
# Sidebar (still lazy)
# =========================
st.sidebar.markdown("### 🧠 懶人設定")
preset_name = st.sidebar.selectbox("模式", list(PRESETS.keys()), index=1)
pool_mode = st.sidebar.selectbox("股票池", ["流動性預篩（推薦）", "全上市（很慢）"], index=0)
st.sidebar.markdown("---")
run_scan = st.sidebar.button("🧊 立即掃描", use_container_width=True)
refresh_cache = st.sidebar.button("🔄 清快取", use_container_width=True)

# =========================
# Header
# =========================
now_ts = now_taipei()
elapsed = minutes_elapsed_in_session(now_ts)

st.markdown(f"""
<div class="header-wrap">
  <div>
    <h1 class="title">起漲戰情室</h1>
    <div class="subtitle">主軸：抓「第一根漲停」並提高 2～7 根連板機率（瑞軒型）</div>
  </div>
  <div class="pill"><span class="dot"></span> 台北時間 <b>{now_ts.strftime('%H:%M:%S')}</b>　盤中進度 <b>{elapsed}/270</b></div>
</div>
""", unsafe_allow_html=True)

if not is_market_time(now_ts):
    st.info("目前非盤中：掃描會使用『最後可取得的盤中資料快照』，準確度會比 09:00～13:30 低。")

# =========================
# Load stock list
# =========================
try:
    stock_df = fetch_all_twse_listed_stocks()
except Exception as e:
    st.error(f"抓上市清單失敗：{e}")
    st.stop()

name_map = dict(zip(stock_df["code"].tolist(), stock_df["name"].tolist()))
all_codes = stock_df["code"].tolist()

if refresh_cache:
    fetch_all_twse_listed_stocks.clear()
    build_daily_baselines.clear()
    fetch_intraday_snapshot_yf.clear()
    st.success("已清除快取。")

# =========================
# Universe (safe)
# =========================
base_df = None
codes_to_scan = all_codes
universe_label = "全上市"

if pool_mode.startswith("流動性預篩"):
    with st.spinner("建立日線基準（用於預篩）..."):
        base_df = build_daily_baselines(all_codes)

    if base_df is None or base_df.empty or "vol_ma20_shares" not in base_df.columns or "code" not in base_df.columns:
        st.warning("日線基準抓不到（可能被限流/網路問題），已自動改用『全上市』模式。")
        base_df = None
        codes_to_scan = all_codes
        universe_label = "全上市（預篩失敗→自動降級）"
    else:
        # 20日均量>=500張/日
        liq_threshold_shares = 500_000
        kept = base_df[base_df["vol_ma20_shares"].astype(float) >= liq_threshold_shares]["code"].tolist()
        if len(kept) < 50:
            st.warning("預篩資料太少（可能不完整），已自動改用『全上市』模式。")
            base_df = None
            codes_to_scan = all_codes
            universe_label = "全上市（預篩資料不足→自動降級）"
        else:
            codes_to_scan = kept
            universe_label = f"流動性預篩（{len(codes_to_scan)} 檔）"

preset = PRESETS[preset_name]

# Pre cards
st.markdown(f"""
<div class="grid">
  <div class="card"><div class="k">股票池</div><div class="v">{universe_label}</div></div>
  <div class="card"><div class="k">掃描目標</div><div class="v">第一根漲停</div></div>
  <div class="card"><div class="k">策略重點</div><div class="v">幾乎鎖死 + 體質濾網</div></div>
  <div class="card"><div class="k">模式</div><div class="v">{preset_name.split('（')[0]}<small>（內建參數）</small></div></div>
</div>
""", unsafe_allow_html=True)

st.markdown("""
<div class="banner">
<b>判斷方式（你不用調參數）：</b>
接近漲停（1 tick 內）＋回落很小＋收在高檔＋盤中爆量品質＋排除昨日已漲停 → 再用「連板潛力分」排序。
</div>
""", unsafe_allow_html=True)

# =========================
# Scan
# =========================
if run_scan:
    with st.spinner("取得日線基準（高點/均量/ATR/昨日漲幅/近5日）..."):
        if base_df is None:
            base_df = build_daily_baselines(codes_to_scan)

    if base_df is None or base_df.empty:
        st.error("日線基準抓不到（yfinance 可能被限流）。請稍後再試或換網路。")
        st.stop()

    with st.spinner("抓取盤中快照（yfinance intraday 5m）..."):
        quotes_df = fetch_intraday_snapshot_yf(codes_to_scan, interval="5m", batch_size=30)

    if quotes_df.empty:
        st.error("盤中快照抓不到（yfinance intraday 可能被限制）。")
        st.stop()

    with st.spinner("鎖定第一根漲停 + 計算連板潛力分..."):
        result = scan_first_limitup_with_continuation(quotes_df, base_df, now_ts, preset)

    st.markdown("<div class='hr'></div>", unsafe_allow_html=True)

    found = 0 if result is None or len(result) == 0 else len(result)

    st.markdown(f"""
<div class="grid">
  <div class="card"><div class="k">資料來源</div><div class="v">yfinance (5m)</div></div>
  <div class="card"><div class="k">掃描檔數</div><div class="v">{len(quotes_df):,}</div></div>
  <div class="card"><div class="k">第一根漲停候選</div><div class="v">{found:,}</div></div>
  <div class="card"><div class="k">模式</div><div class="v">{preset_name.split('（')[0]}</div></div>
</div>
""", unsafe_allow_html=True)

    if found == 0:
        st.warning("目前沒掃到符合『第一根漲停』且體質過濾通過的標的。你可以切到「積極」放寬條件再掃一次。")
    else:
        result = result.copy()
        result["名稱"] = result["代號"].map(name_map).fillna("")
        result.insert(0, "排名", range(1, len(result) + 1))

        st.success(f"🧊 掃到 {found} 檔「第一根漲停」候選（已按連板潛力分排序）")

        topn = result.head(12).copy()
        q75 = float(topn["連板潛力分"].quantile(0.75))
        cols = st.columns(4)

        for i, (_, r) in enumerate(topn.iterrows(), start=1):
            with cols[(i - 1) % 4]:
                score = float(r["連板潛力分"])
                tag = "🔒 幾乎鎖死" if score >= q75 else "👀 候選"

                code = str(r["代號"])
                name = str(r["名稱"]) if pd.notna(r["名稱"]) else ""
                price = float(r["現價"])
                lim = float(r["漲停價"])
                dist = float(r["距離漲停(%)"])
                chg = float(r["較昨收(%)"])
                lots = int(float(r["累積量(張)"]))
                volx = float(r["盤中爆量倍數"])

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
  <div class="small-note">連板潛力分：{score:.1f}</div>
</div>
""", unsafe_allow_html=True)

        st.markdown("<div class='hr'></div>", unsafe_allow_html=True)

        with st.expander("📋 看完整榜單（美化表格）", expanded=True):
            render_pretty_table(result)

st.caption("提醒：此工具用「價格型態」近似鎖死，沒有封單資料；連板只是機率提升，不是保證。")
