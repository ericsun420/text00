import io
import math
import time
import html
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dtime
from typing import Dict, List, Tuple

import requests
import urllib3
import pandas as pd
import yfinance as yf
import streamlit as st
import streamlit.components.v1 as components

# 關閉 SSL 憑證警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================
# UI / THEME
# =========================
st.set_page_config(page_title="起漲戰情室｜第一根漲停", page_icon="🧊", layout="wide")

CSS = """
<style>
:root{ --bg:#07080b; --panel:#0b0d12; --text:#e5e7eb; --muted:#9ca3af; --line:rgba(148,163,184,.16); --shadow:0 16px 40px rgba(0,0,0,.35); --hi:rgba(148,163,184,.08);}
[data-testid="stAppViewContainer"]{ background: var(--bg) !important; color: var(--text) !important; }
.main{ background: var(--bg) !important; }
.block-container{ padding-top: 1.05rem; padding-bottom: 2.0rem; }
[data-testid="stHeader"]{ background: rgba(7,8,11,.80) !important; border-bottom: 1px solid var(--line) !important; }
[data-testid="stToolbar"]{ background: transparent !important; }
[data-testid="stSidebar"]{ background: var(--panel) !important; border-right: 1px solid var(--line) !important; }
[data-testid="stSidebar"] *{ color: var(--text) !important; }
[data-testid="stSidebar"] label,[data-testid="stSidebar"] p,[data-testid="stSidebar"] span{ color: var(--muted) !important; }
.header-wrap{ display:flex; align-items:flex-end; justify-content:space-between; gap:18px; padding: 6px 4px 2px 4px; }
.title{ font-size: 42px; font-weight: 900; letter-spacing: .4px; background: linear-gradient(90deg,#f3f4f6,#9ca3af); -webkit-background-clip:text; -webkit-text-fill-color: transparent; margin:0; }
.subtitle{ margin:6px 0 0 2px; color: var(--muted); font-size: 14px; }
.pill{ display:inline-flex; align-items:center; gap:8px; padding:8px 12px; border:1px solid var(--line); border-radius:999px; background: rgba(15,17,22,.85); font-size:13px; box-shadow: var(--shadow); }
.pill .dot{ width:8px; height:8px; border-radius:999px; background:#9ca3af; display:inline-block; }
.grid{ display:grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin: 12px 0 6px 0; }
.card{ background: linear-gradient(180deg, rgba(15,17,22,.94), rgba(11,13,18,.94)); border:1px solid var(--line); border-radius:16px; padding:14px 14px 12px 14px; box-shadow: var(--shadow); }
.k{ color: var(--muted); font-size: 12px; margin-bottom: 6px; }
.v{ color: var(--text); font-size: 20px; font-weight: 800; }
.v small{ color: var(--muted); font-weight: 600; font-size: 12px; margin-left: 6px;}
.hr{ height:1px; background: var(--line); margin: 12px 0; }
.banner{ background: rgba(148,163,184,.08); border: 1px solid rgba(148,163,184,.22); border-radius:16px; padding: 12px 14px; margin: 10px 0 10px 0; }
.banner b{ color:#fff; }
.metric{ display:flex; justify-content:space-between; align-items:flex-end; gap:10px; }
.metric .left{ display:flex; flex-direction:column; gap:2px; }
.metric .label{ color: var(--muted); font-size: 12px; display:flex; gap:8px; align-items:center; }
.metric .code{ color: var(--text); font-size: 16px; font-weight: 900; line-height:1.1; }
.metric .name{ color: var(--muted); font-size: 12px; margin-top: 2px; }
.metric .tag{ font-size: 12px; padding: 4px 8px; border-radius: 999px; border:1px solid var(--line); background: rgba(15,17,22,.8); }
.metric .price{ font-size: 22px; font-weight: 900; line-height: 1; }
.metric .chg{ font-size: 12px; color: var(--muted); }
.stButton>button{ border-radius: 14px !important; border: 1px solid rgba(203,213,225,.26) !important; background: linear-gradient(90deg, rgba(148,163,184,.16), rgba(107,114,128,.10)) !important; color: var(--text) !important; font-weight: 800 !important; padding: 10px 14px !important;}
.stSelectbox>div>div, .stNumberInput>div>div{ border-radius: 14px !important; border: 1px solid rgba(148,163,184,.22) !important; background: rgba(15,17,22,.88) !important; color: var(--text) !important;}
[data-testid="stExpander"]{ border: 1px solid var(--line) !important; border-radius: 16px !important; background: rgba(15,17,22,.55) !important;}
[data-testid="stExpander"] summary{ font-weight: 900 !important; }
.small-note{ color: var(--muted); font-size: 12px; }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

# =========================
# TIME
# =========================
def now_taipei() -> datetime:
    return datetime.utcnow() + timedelta(hours=8)

def minutes_elapsed_in_session(ts: datetime) -> int:
    start = datetime.combine(ts.date(), dtime(9, 0))
    end = datetime.combine(ts.date(), dtime(13, 30))
    if ts < start: return 0
    if ts > end: return 270
    return int((ts - start).total_seconds() // 60)

def session_fraction(ts: datetime) -> float:
    m = minutes_elapsed_in_session(ts)
    return max(0.2, min(1.0, m / 270.0))

# =========================
# HELPERS
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
# Universe — GitHub ONLY
# =========================
@dataclass
class Meta:
    code: str
    name: str
    market: str
    industry: str
    ex: str
    yf_symbol: str

def _fetch_csv(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=45, allow_redirects=True, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    text = r.text.replace("\r\n", "\n").replace("\r", "\n")
    # 移除 on_bad_lines="skip"，確保相容性
    df = pd.read_csv(io.StringIO(text), dtype=str, engine="python")
    df.columns = [str(c).strip() for c in df.columns]
    return df

@st.cache_data(ttl=24*3600, show_spinner=False)
def load_universe_github(include_tpex: bool) -> Dict[str, Meta]:
    twse_raw = "https://raw.githubusercontent.com/mlouielu/twstock/refs/heads/master/twstock/codes/twse_equities.csv"
    tpex_raw = "https://raw.githubusercontent.com/mlouielu/twstock/refs/heads/master/twstock/codes/tpex_equities.csv"

    meta: Dict[str, Meta] = {}

    df1 = _fetch_csv(twse_raw)
    if "code" not in df1.columns:
        df1 = pd.read_csv(io.StringIO(df1.to_csv(index=False, header=False)), header=None, dtype=str, engine="python")
        df1.columns = ["type","code","name","ISIN","start","market","group","CFI"][:df1.shape[1]]

    for _, r in df1.iterrows():
        c = str(r.get("code","")).strip()
        if re.match(r"^\d{4,6}$", c or ""):
            meta[c] = Meta(
                code=c,
                name=str(r.get("name","")).strip(),
                market="上市",
                industry=str(r.get("group","")).strip() or "未分類",
                ex="tse",
                yf_symbol=f"{c}.TW",
            )

    if include_tpex:
        df2 = _fetch_csv(tpex_raw)
        if "code" not in df2.columns:
            df2 = pd.read_csv(io.StringIO(df2.to_csv(index=False, header=False)), header=None, dtype=str, engine="python")
            df2.columns = ["type","code","name","ISIN","start","market","group","CFI"][:df2.shape[1]]
        for _, r in df2.iterrows():
            c = str(r.get("code","")).strip()
            if re.match(r"^\d{4,6}$", c or ""):
                meta[c] = Meta(
                    code=c,
                    name=str(r.get("name","")).strip(),
                    market="上櫃",
                    industry=str(r.get("group","")).strip() or "未分類",
                    ex="otc",
                    yf_symbol=f"{c}.TWO",
                )

    if len(meta) < 300:
        raise ValueError("GitHub 清單取得失敗或數量異常。")
    return meta

# =========================
# MIS client
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
        self.s.get("https://mis.twse.com.tw/stock/fibest.jsp?lang=zh_tw", headers=self.headers, timeout=20, verify=False)
        self.inited = True

    def get_quotes(self, ex_ch_list: List[str]) -> List[dict]:
        self.init()
        if not ex_ch_list:
            return []
        ex_ch = "%7c".join(ex_ch_list)
        url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={ex_ch}&json=1&delay=0&_={int(time.time()*1000)}"
        r = self.s.get(url, headers=self.headers, timeout=25, verify=False)
        r.raise_for_status()
        data = r.json()
        return data.get("msgArray") or []

@st.cache_data(ttl=6, show_spinner=False)
def fetch_mis_snapshot(meta_items: List[Meta], batch_size: int = 70) -> pd.DataFrame:
    mis = MISClient()
    rows = []
    for i in range(0, len(meta_items), batch_size):
        chunk = meta_items[i:i+batch_size]
        ex_list = [f"{m.ex}_{m.code}.tw" for m in chunk]
        try:
            arr = mis.get_quotes(ex_list)
        except Exception:
            continue
        mm = {m.code: m for m in chunk}
        for q in arr:
            c = str(q.get("c","")).strip()
            m = mm.get(c)
            if not m:
                continue
            last = fnum(q.get("z"), None)
            prev_close = fnum(q.get("y"), None)
            o = fnum(q.get("o"), None)
            h = fnum(q.get("h"), None)
            l = fnum(q.get("l"), None)
            u = fnum(q.get("u"), None)
            v = fnum(q.get("v"), 0.0) or 0.0

            a_prices = split_underscore_nums(q.get("a",""))
            b_prices = split_underscore_nums(q.get("b",""))
            a_vols = split_underscore_nums(q.get("f",""))
            b_vols = split_underscore_nums(q.get("g",""))

            rows.append({
                "code": m.code, "name": m.name, "market": m.market, "industry": m.industry,
                "last": last, "prev_close": prev_close, "open": o, "high": h, "low": l,
                "upper": u, "volume_shares": v,
                "bid_p0": (b_prices[0] if b_prices else None),
                "bid_v0": (b_vols[0] if b_vols else None),
                "ask_p0": (a_prices[0] if a_prices else None),
                "ask_v0": (a_vols[0] if a_vols else None),
                "time": str(q.get("t","")).strip() or str(q.get("tlong","")).strip(),
            })
        time.sleep(0.05)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).drop_duplicates("code")
    df["volume_lots"] = (df["volume_shares"].fillna(0).astype(float) / 1000.0).astype(int)
    df["chg_pct"] = (df["last"] / df["prev_close"] - 1.0) * 100.0
    df.loc[df["prev_close"].isna() | (df["prev_close"] == 0), "chg_pct"] = None
    df["industry"] = df["industry"].fillna("未分類").replace("", "未分類")
    return df

# =========================
# Daily baseline (candidates only)
# =========================
@st.cache_data(ttl=6*3600, show_spinner=False)
def build_daily_baseline_for_candidates(candidate_symbols: List[str]) -> pd.DataFrame:
    if not candidate_symbols:
        return pd.DataFrame()
    period = "400d"
    batch = 60
    rows = []
    for i in range(0, len(candidate_symbols), batch):
        syms = candidate_symbols[i:i+batch]
        tickers = " ".join(syms)
        try:
            raw = yf.download(tickers=tickers, period=period, interval="1d", group_by="ticker",
                              auto_adjust=False, threads=True, progress=False)
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

                vol_ma20 = float(vol.rolling(20).mean().iloc[-1])

                prev_close = close.shift(1)
                tr1 = (high - low).abs()
                tr2 = (high - prev_close).abs()
                tr3 = (low - prev_close).abs()
                tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
                atr20 = float(tr.rolling(20).mean().iloc[-1])
                last_close = float(close.iloc[-1])
                atr20_pct = (atr20 / last_close) * 100.0 if last_close else None

                ret_5d = (last_close / float(close.iloc[-6]) - 1.0) * 100.0 if len(close) >= 6 else None
                hist_ret = close.pct_change().dropna()
                max_ret_10d = float(hist_ret.tail(10).max()) * 100.0 if len(hist_ret) >= 10 else None

                ma20 = close.rolling(20).mean()
                near_ma20 = ((close / (ma20 + 1e-9) - 1.0).abs() <= 0.04)
                base_len_days = int(near_ma20.tail(60).sum())

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
                    "base_len_days": base_len_days,
                    "base_tight_score": base_tight_score,
                })
            except Exception:
                continue
        time.sleep(0.05)

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).drop_duplicates("yf_symbol").set_index("yf_symbol")

# =========================
# Table renderer
# =========================
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
    components.html(html_doc, height=height + 70, scrolling=False)

# =========================
# Sidebar
# =========================
st.sidebar.markdown("### 🧠 懶人設定")
mode = st.sidebar.selectbox("模式", ["保守", "標準", "積極"], index=1)
market_mode = st.sidebar.selectbox("市場", ["只掃上市（TWSE）", "上市 + 上櫃（TWSE+TPEX）"], index=0)
min_lots = st.sidebar.number_input("最低盤中量（張）", min_value=200, max_value=20000, value=1200, step=100)
dist_upper = st.sidebar.number_input("候選距離漲停(%)", min_value=0.1, max_value=5.0, value=1.0, step=0.1)
run_scan = st.sidebar.button("🧊 立即掃描（MIS 即時）", use_container_width=True)

# =========================
# Header
# =========================
now_ts = now_taipei()
elapsed = minutes_elapsed_in_session(now_ts)
st.markdown(f"""
<div class="header-wrap">
  <div>
    <h1 class="title">起漲戰情室</h1>
    <div class="subtitle">不走本機：GitHub 清單 + MIS 盤中快照 → 少量候選再抓日線</div>
  </div>
  <div class="pill"><span class="dot"></span> 台北時間 <b>{now_ts.strftime('%H:%M:%S')}</b>　盤中進度 <b>{elapsed}/270</b></div>
</div>
""", unsafe_allow_html=True)

include_tpex = (market_mode == "上市 + 上櫃（TWSE+TPEX）")

try:
    universe = load_universe_github(include_tpex)
except Exception as e:
    st.error(f"股票清單抓不到：{e}")
    st.stop()

meta_items = list(universe.values())

st.markdown(f"""
<div class="grid">
  <div class="card"><div class="k">市場</div><div class="v">{html.escape(market_mode)}</div></div>
  <div class="card"><div class="k">清單檔數</div><div class="v">{len(meta_items):,}<small> 檔</small></div></div>
  <div class="card"><div class="k">最低量</div><div class="v">{int(min_lots):,}<small> 張</small></div></div>
  <div class="card"><div class="k">距離漲停</div><div class="v">{float(dist_upper):.1f}<small>%</small></div></div>
</div>
""", unsafe_allow_html=True)

st.markdown("""
<div class="banner">
<b>你之前卡住/抓不到 5m 的根因：</b> yfinance intraday 限流。<br>
這版盤中改用 MIS 即時報價（一次抓很多檔），並且日線只抓少量候選，速度會穩很多。
</div>
""", unsafe_allow_html=True)

# =========================
# Run scan
# =========================
if run_scan:
    with st.spinner("MIS：抓盤中即時快照（大量檔一次抓）..."):
        snap = fetch_mis_snapshot(meta_items, batch_size=70)

    if snap is None or snap.empty:
        st.error("MIS 即時資料抓不到（可能網路/環境限制）。")
        st.stop()

    snap["dist_upper_pct"] = ((snap["upper"] - snap["last"]) / snap["upper"]) * 100.0
    snap.loc[snap["upper"].isna() | (snap["upper"] == 0), "dist_upper_pct"] = None

    # 族群共振（快照版）
    st.markdown("<div class='hr'></div>", unsafe_allow_html=True)
    st.subheader("🧭 族群共振 Radar（快照版）")

    df = snap.copy()
    df["tick"] = df["upper"].fillna(0).astype(float).apply(lambda u: tw_tick(u) if u else 0.05)
    df["near_upper"] = df["last"] >= (df["upper"] - df["tick"])
    df["heat"] = 0.0
    df["heat"] += df["near_upper"].astype(int) * 40.0
    df["heat"] += df["chg_pct"].fillna(0).clip(lower=0) * 2.0
    df["heat"] += (df["volume_lots"].fillna(0) / 2000.0).clip(upper=20) * 2.0
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
    sector["共振分"] = (sector["熱檔數"]*20.0 + sector["貼板數"]*8.0 + sector["平均熱度"]*0.35 + sector["最高熱度"]*0.25).clip(0, 100)
    sector = sector.sort_values(["共振分","熱檔數","貼板數","最高熱度"], ascending=False).head(10).reset_index(drop=True)
    sector.insert(0, "排名", range(1, len(sector)+1))

    cols = st.columns(4)
    for i, (_, r) in enumerate(sector.head(8).iterrows(), start=1):
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
        render_table_html("族群共振排行榜", sector, ["排名","族群名稱","共振分","熱檔數","貼板數","掃描檔數","平均熱度","最高熱度","平均量"], height=420)

    # 第一根候選：先快篩，再抓日線
    st.markdown("<div class='hr'></div>", unsafe_allow_html=True)
    st.subheader("🚀 第一根漲停候選（少量才抓日線）")

    pre = snap[
        (snap["volume_lots"] >= int(min_lots)) &
        (snap["dist_upper_pct"].fillna(999) <= float(dist_upper))
    ].copy()

    if pre.empty:
        st.warning("目前沒有符合『量大 + 接近漲停』的候選（放寬距離漲停%或降低最低量）。")
        st.stop()

    pre["yf_symbol"] = pre["code"].apply(lambda c: universe.get(str(c)).yf_symbol if str(c) in universe else f"{c}.TW")
    cand_syms = pre["yf_symbol"].dropna().astype(str).unique().tolist()

    with st.spinner(f"抓日線基準（只算候選 {len(cand_syms)} 檔）..."):
        base = build_daily_baseline_for_candidates(cand_syms)

    if base is None or base.empty:
        st.error("候選日線抓不到（yfinance 可能被限流）。請稍後重試或收斂候選檔數。")
        st.stop()

    frac = session_fraction(now_ts)
    rows = []
    for _, r in pre.iterrows():
        sym = str(r["yf_symbol"])
        if sym not in base.index:
            continue
        b = base.loc[sym]

        vol_ma20 = float(b.get("vol_ma20_shares", 0.0) or 0.0)
        vol_ratio = (float(r["volume_shares"]) / (vol_ma20 * frac + 1e-9)) if vol_ma20 > 0 else 0.0

        prev_close = float(r["prev_close"]) if pd.notna(r["prev_close"]) and r["prev_close"] else None
        upper = float(r["upper"]) if pd.notna(r["upper"]) and r["upper"] else None
        if not prev_close or not upper:
            continue

        limit_pct = (upper / prev_close - 1.0) * 100.0
        hype_thr = 19.0 if limit_pct > 15 else 9.5
        max_ret_10d = float(b.get("max_ret_10d", 0.0) or 0.0)
        if max_ret_10d >= hype_thr:
            continue

        score = 0.0
        score += min(40.0, max(0.0, (float(r["chg_pct"] or 0) * 2.0)))
        score += min(30.0, max(0.0, (vol_ratio - 1.0) * 10.0))
        score += min(15.0, max(0.0, (int(b.get("base_len_days", 0) or 0) - 10) * 0.4))
        score += min(15.0, max(0.0, float(b.get("base_tight_score", 0.0) or 0.0) * 15.0))
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
            "基底天數": int(b.get("base_len_days", 0) or 0),
            "基底緊縮分": float(b.get("base_tight_score", 0.0) or 0.0),
            "ATR20(%)": float(b.get("atr20_pct", 999.0) or 999.0),
            "近5日漲幅(%)": float(b.get("ret_5d", 0.0) or 0.0),
            "連板潛力分": float(score),
        })

    if not rows:
        st.warning("候選有，但『第一根濾網』全部被排除（多半是近10日已嗨過）。")
        st.stop()

    res = pd.DataFrame(rows).sort_values(["連板潛力分","距離漲停(%)","盤中爆量倍數(線性)"], ascending=[False, True, False]).reset_index(drop=True)
    res["排名"] = range(1, len(res)+1)

    st.success(f"✅ 鎖到 {len(res)} 檔候選（已排序）")

    with st.expander("📋 完整榜單（美化表格）", expanded=True):
        cols_show = [
            "排名","代號","名稱","市場","族群","現價","漲停價","距離漲停(%)","較昨收(%)",
            "累積量(張)","盤中爆量倍數(線性)",
            "基底天數","近5日漲幅(%)","ATR20(%)","基底緊縮分","連板潛力分"
        ]
        render_table_html("第一根漲停候選", res, cols_show, height=580)
