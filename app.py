import time
import re
from datetime import datetime, timedelta, time as dtime
from io import StringIO

import requests
import urllib3
import pandas as pd
import yfinance as yf
import streamlit as st

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================
# 美化 CSS
# =========================
st.set_page_config(page_title="起漲戰情室", page_icon="🚀", layout="wide")

CSS = """
<style>
:root{
  --bg:#07080b;
  --panel:#0b0d12;
  --card:#0f1116;
  --card2:#0b0d12;
  --text:#e5e7eb;
  --muted:#9ca3af;
  --line:rgba(148,163,184,.16);
  --shadow: 0 16px 40px rgba(0,0,0,.35);
}

/* ✅ 1) 強制整個 App（含白底）變深色 */
[data-testid="stAppViewContainer"]{
  background: var(--bg) !important;
  color: var(--text) !important;
}
.main{
  background: var(--bg) !important;
}
.block-container{
  padding-top: 1.2rem;
  padding-bottom: 2.2rem;
}

/* ✅ 2) Header / Toolbar 變黑灰 */
[data-testid="stHeader"]{
  background: rgba(7,8,11,.80) !important;
  border-bottom: 1px solid var(--line) !important;
}
[data-testid="stToolbar"]{
  background: transparent !important;
}

/* ✅ 3) Sidebar 變黑灰（你截圖左邊白底的元兇） */
[data-testid="stSidebar"]{
  background: var(--panel) !important;
  border-right: 1px solid var(--line) !important;
}
[data-testid="stSidebar"] *{
  color: var(--text) !important;
}

/* Sidebar 裡的 label 讓它更乾淨 */
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span{
  color: var(--muted) !important;
}

/* ✅ 4) 你原本的戰情室排版（保留但改成更耐看黑灰） */
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

.metric{
  display:flex; justify-content:space-between; align-items:flex-end;
  gap:10px;
}
.metric .left{ display:flex; flex-direction:column; gap:2px; }
.metric .label{ color: var(--muted); font-size: 12px; }
.metric .code{ color: var(--text); font-size: 16px; font-weight: 800; }
.metric .tag{
  font-size: 12px; padding: 4px 8px; border-radius: 999px;
  border:1px solid var(--line); color: var(--text);
  background: rgba(15,17,22,.8);
}
.metric .price{ font-size: 22px; font-weight: 900; color: var(--text); line-height: 1; }
.metric .chg{ font-size: 12px; color: var(--muted); }

/* ✅ 5) 控件：按鈕/下拉/輸入框都改黑灰 */
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

.stSelectbox>div>div,
.stTextInput>div>div,
.stNumberInput>div>div{
  border-radius: 14px !important;
  border: 1px solid rgba(148,163,184,.22) !important;
  background: rgba(15,17,22,.88) !important;
  color: var(--text) !important;
}

/* ✅ 6) dataframe / expander 也黑灰 */
[data-testid="stDataFrame"]{
  border-radius: 16px;
  border: 1px solid var(--line);
  overflow: hidden;
  background: rgba(15,17,22,.88) !important;
}
[data-testid="stExpander"]{
  border: 1px solid var(--line) !important;
  border-radius: 16px !important;
  background: rgba(15,17,22,.70) !important;
}
.small-note{ color: var(--muted); font-size: 12px; }

/* ✅ 7) 文字（避免有些地方還是黑字） */
h1,h2,h3,h4,h5,h6, p, span, div{
  color: inherit;
}
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)
# =========================
# 時間工具
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

# =========================
# 上市清單（MOPS CSV, 自動解碼）
# =========================
def http_get_bytes(url: str, timeout: int = 40) -> bytes:
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        r.raise_for_status()
        return r.content
    except requests.exceptions.SSLError:
        r = requests.get(url.replace("http://", "https://"), headers=headers, timeout=timeout,
                         allow_redirects=True, verify=False)
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

@st.cache_data(ttl=24 * 3600)
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
# 日線基準（yfinance）
# =========================
def _drop_today_bar_if_exists(df: pd.DataFrame, today_date) -> pd.DataFrame:
    if df.empty:
        return df
    last_date = pd.Timestamp(df.index[-1]).date()
    if last_date == today_date:
        return df.iloc[:-1].copy()
    return df

@st.cache_data(ttl=6 * 3600)
def build_daily_baselines(codes: list[str]) -> pd.DataFrame:
    end_date = now_taipei().date()
    start = (now_taipei() - timedelta(days=200)).date().isoformat()
    batch = 60
    records = []
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

                df = _drop_today_bar_if_exists(df, end_date)
                if df.empty or len(df) < 80:
                    continue

                records.append({
                    "code": c,
                    "vol_ma20_shares": float(df["Volume"].rolling(20).mean().iloc[-1]),
                    "high20": float(df["High"].rolling(20).max().shift(1).iloc[-1]),
                    "ma60": float(df["Close"].rolling(60).mean().iloc[-1]),
                    "yday_vol_shares": float(df["Volume"].iloc[-1]),
                    "yday_close": float(df["Close"].iloc[-1]),
                    "change_5d": float((df["Close"].iloc[-1] / df["Close"].iloc[-6] - 1.0)) if len(df) >= 6 else None,
                })
            except Exception:
                continue
    return pd.DataFrame(records).drop_duplicates("code")

# =========================
# 盤中即時：yfinance intraday（懶人版用這個，最少外部依賴）
# =========================
@st.cache_data(ttl=20)
def fetch_intraday_snapshot_yf(codes: list[str], interval: str = "5m", batch_size: int = 30) -> pd.DataFrame:
    out = []
    for i in range(0, len(codes), batch_size):
        chunk = codes[i:i + batch_size]
        tickers = " ".join([f"{c}.TW" for c in chunk])
        try:
            raw = yf.download(
                tickers=tickers, period="1d", interval=interval,
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

                if df.empty:
                    continue

                day_open = float(df["Open"].iloc[0])
                day_high = float(df["High"].max())
                day_low = float(df["Low"].min())
                last = float(df["Close"].iloc[-1])
                vol_shares = float(df["Volume"].sum())

                out.append({
                    "code": c,
                    "last": last,
                    "open": day_open,
                    "high": day_high,
                    "low": day_low,
                    "vol_lots": int(vol_shares / 1000),
                })
            except Exception:
                continue

        time.sleep(0.12)
    df = pd.DataFrame(out)
    if df.empty:
        return df
    return df.drop_duplicates("code")

# =========================
# 懶人參數（只給新手三段）
# =========================
PRESETS = {
    "保守（少訊號、盡量避雷）": dict(
        breakout_buffer_pct=1.2,
        vol_mult=3.0,
        close_pos_min=0.75,
        upper_shadow_max=0.25,
        min_cum_lots=2500,
        require_above_ma60=True,
        avoid_yday_spike=True, yday_spike_mult=1.6,
        avoid_overheat_5d=True, overheat_5d_max=15,
        require_green_body=True, body_min_pct=3.0,
    ),
    "標準（大多數人用）": dict(
        breakout_buffer_pct=1.0,
        vol_mult=2.5,
        close_pos_min=0.70,
        upper_shadow_max=0.30,
        min_cum_lots=2000,
        require_above_ma60=True,
        avoid_yday_spike=True, yday_spike_mult=1.8,
        avoid_overheat_5d=True, overheat_5d_max=18,
        require_green_body=True, body_min_pct=2.5,
    ),
    "積極（多訊號、容忍波動）": dict(
        breakout_buffer_pct=0.6,
        vol_mult=2.0,
        close_pos_min=0.62,
        upper_shadow_max=0.38,
        min_cum_lots=1200,
        require_above_ma60=False,
        avoid_yday_spike=False, yday_spike_mult=2.2,
        avoid_overheat_5d=False, overheat_5d_max=25,
        require_green_body=False, body_min_pct=2.0,
    ),
}

def scan_intraday_breakouts(quotes: pd.DataFrame, base: pd.DataFrame, now_ts: datetime, p: dict) -> pd.DataFrame:
    df = quotes.merge(base, on="code", how="inner").copy()
    df = df.dropna(subset=["last", "open", "high", "low", "high20", "vol_ma20_shares", "yday_close", "vol_lots"])

    df["cum_vol_shares"] = df["vol_lots"].astype(float) * 1000.0

    df["breakout_level"] = df["high20"] * (1.0 + p["breakout_buffer_pct"] / 100.0)
    df["cond_breakout"] = df["last"] > df["breakout_level"]

    rng = (df["high"] - df["low"]).replace(0, 1e-9)
    df["close_pos"] = (df["last"] - df["low"]) / rng
    df["cond_close_pos"] = df["close_pos"] >= p["close_pos_min"]

    df["real_body_top"] = df[["open", "last"]].max(axis=1)
    df["upper_shadow_ratio"] = (df["high"] - df["real_body_top"]) / rng
    df["cond_shadow"] = df["upper_shadow_ratio"] <= p["upper_shadow_max"]

    df["body_return"] = (df["last"] - df["open"]) / df["open"]
    if p["require_green_body"]:
        df["cond_green_body"] = (df["last"] > df["open"]) & (df["body_return"] >= p["body_min_pct"] / 100.0)
    else:
        df["cond_green_body"] = True

    elapsed = minutes_elapsed_in_session(now_ts)
    frac = max(0.2, max(1, min(270, elapsed)) / 270.0)
    df["expected_vol_shares_now"] = df["vol_ma20_shares"] * frac
    df["vol_ratio_now"] = df["cum_vol_shares"] / (df["expected_vol_shares_now"] + 1e-9)
    df["cond_vol_burst"] = df["vol_ratio_now"] >= p["vol_mult"]

    df["cond_min_cum"] = df["vol_lots"].astype(int) >= int(p["min_cum_lots"])

    if p["require_above_ma60"]:
        df = df.dropna(subset=["ma60"])
        df["cond_above_ma60"] = df["last"] > df["ma60"]
    else:
        df["cond_above_ma60"] = True

    if p["avoid_yday_spike"]:
        df["cond_yday_ok"] = df["yday_vol_shares"] <= (df["vol_ma20_shares"] * p["yday_spike_mult"])
    else:
        df["cond_yday_ok"] = True

    if p["avoid_overheat_5d"]:
        df["cond_overheat_ok"] = df["change_5d"].fillna(0) <= p["overheat_5d_max"] / 100.0
    else:
        df["cond_overheat_ok"] = True

    cond = (
        df["cond_breakout"] & df["cond_vol_burst"] & df["cond_close_pos"] &
        df["cond_shadow"] & df["cond_min_cum"] & df["cond_above_ma60"] &
        df["cond_yday_ok"] & df["cond_overheat_ok"] & df["cond_green_body"]
    )
    out = df[cond].copy()
    if out.empty:
        return out

    out["chg_pct_vs_yday"] = (out["last"] / out["yday_close"] - 1.0) * 100.0
    out["score"] = (
        2.0 * out["vol_ratio_now"].clip(0, 10) +
        1.5 * out["close_pos"].clip(0, 1) -
        1.0 * out["upper_shadow_ratio"].clip(0, 1) +
        0.3 * out["body_return"].clip(-1, 1)
    )
    out = out.sort_values(["score", "vol_ratio_now"], ascending=False)

    show = out[["code","last","chg_pct_vs_yday","vol_lots","vol_ratio_now","high20","breakout_level","score"]].copy()
    show.rename(columns={
        "code":"代號","last":"現價","chg_pct_vs_yday":"較昨收(%)","vol_lots":"累積量(張)",
        "vol_ratio_now":"盤中爆量倍數","high20":"前20日高","breakout_level":"突破門檻","score":"綜合分數"
    }, inplace=True)
    return show

# =========================
# Sidebar：超簡單
# =========================
st.sidebar.markdown("### 🧠 懶人設定")
preset_name = st.sidebar.selectbox("風險等級", list(PRESETS.keys()), index=1)
pool_mode = st.sidebar.selectbox("掃描模式", ["流動性預篩（推薦）", "全上市（很慢）"], index=0)

st.sidebar.markdown("---")
run_scan = st.sidebar.button("🚀 立即掃描", use_container_width=True)
refresh_base = st.sidebar.button("🔄 重建日線快取", use_container_width=True)

# =========================
# Header
# =========================
now_ts = now_taipei()
elapsed = minutes_elapsed_in_session(now_ts)

st.markdown(f"""
<div class="header-wrap">
  <div>
    <h1 class="title">起漲戰情室</h1>
    <div class="subtitle">新手懶人版：只選「風險等級」→按「立即掃描」</div>
  </div>
  <div class="pill"><span class="dot"></span> 台北時間 <b>{now_ts.strftime('%H:%M:%S')}</b>　盤中進度 <b>{elapsed}/270</b></div>
</div>
""", unsafe_allow_html=True)

# =========================
# Load stock list
# =========================
try:
    stock_df = fetch_all_twse_listed_stocks()
except Exception as e:
    st.error(f"抓上市清單失敗：{e}")
    st.stop()

all_codes = stock_df["code"].tolist()

if refresh_base:
    build_daily_baselines.clear()
    st.success("已清除日線快取，下次掃描會重建。")

# =========================
# Determine universe
# =========================
base_df = None
codes_to_scan = all_codes
universe_label = "全上市"

if pool_mode.startswith("流動性預篩"):
    with st.spinner("建立日線基準（用於預篩）..."):
        base_df = build_daily_baselines(all_codes)
    liq_threshold_shares = 500_000  # 500 張/日
    codes_to_scan = base_df[base_df["vol_ma20_shares"] >= liq_threshold_shares]["code"].tolist()
    universe_label = f"流動性預篩（{len(codes_to_scan)} 檔）"

p = PRESETS[preset_name]

# =========================
# Dashboard cards (before scan)
# =========================
st.markdown(f"""
<div class="grid">
  <div class="card"><div class="k">股票池</div><div class="v">{universe_label}</div></div>
  <div class="card"><div class="k">風險等級</div><div class="v">{preset_name.split('（')[0]}<small>({preset_name})</small></div></div>
  <div class="card"><div class="k">判斷邏輯</div><div class="v">突破 + 爆量 + 收高</div></div>
  <div class="card"><div class="k">建議掃描時間</div><div class="v">13:15 – 13:25</div></div>
</div>
""", unsafe_allow_html=True)

st.markdown("""
<div class="banner">
<b>你只需要懂一句：</b>「突破 20 日高 + 盤中爆量 + 站得住（收在高檔、上影線短）」才算真正的第一根。  
</div>
""", unsafe_allow_html=True)

# =========================
# Scan
# =========================
if run_scan:
    with st.spinner("取得日線基準（MA/20日高/均量/過熱/昨日爆量）..."):
        if base_df is None:
            base_df = build_daily_baselines(codes_to_scan)

    with st.spinner("抓取盤中快照（yfinance intraday 5m）..."):
        quotes_df = fetch_intraday_snapshot_yf(codes_to_scan, interval="5m", batch_size=30)

    if quotes_df.empty:
        st.error("盤中快照抓不到資料（可能你網路限制 yfinance intraday）。")
        st.stop()

    with st.spinner("計算訊號..."):
        result = scan_intraday_breakouts(quotes_df, base_df, now_ts, p)

    # Status cards after scan
    st.markdown("<div class='hr'></div>", unsafe_allow_html=True)

    found = 0 if result is None or len(result) == 0 else len(result)

    st.markdown(f"""
<div class="grid">
  <div class="card"><div class="k">即時資料來源</div><div class="v">yfinance intraday (5m)</div></div>
  <div class="card"><div class="k">掃描檔數</div><div class="v">{len(quotes_df):,}</div></div>
  <div class="card"><div class="k">符合條件</div><div class="v">{found:,}</div></div>
  <div class="card"><div class="k">策略模式</div><div class="v">{preset_name.split('（')[0]}</div></div>
</div>
""", unsafe_allow_html=True)

    if found == 0:
        st.warning("目前沒有符合條件的第一根。你可以改成「積極」再掃一次（會更容易出訊號）。")
    else:
        st.success(f"🎯 今日此刻掃到 {found} 檔符合『第一根』的候選標的")

        # Top 12 cards
        topn = result.head(12).copy()

        cols = st.columns(4)
        for idx, row in topn.iterrows():
            c = cols[idx % 4]
            with c:
                tag = "🔥強" if row["綜合分數"] >= topn["綜合分數"].quantile(0.75) else "✅可看"
                st.markdown(f"""
<div class="card">
  <div class="metric">
    <div class="left">
      <div class="label">#{idx+1}　<span class="tag">{tag}</span></div>
      <div class="code">{row['代號']}</div>
    </div>
    <div style="text-align:right">
      <div class="price">{row['現價']:.2f}</div>
      <div class="chg">較昨收 {row['較昨收(%)']:.2f}%</div>
    </div>
  </div>
  <div class="hr"></div>
  <div class="small-note">累積量：{int(row['累積量(張)']):,} 張 ｜ 爆量：{row['盤中爆量倍數']:.2f}x</div>
  <div class="small-note">突破門檻：{row['突破門檻']:.2f} ｜ 分數：{row['綜合分數']:.2f}</div>
</div>
""", unsafe_allow_html=True)

        st.markdown("<div class='hr'></div>", unsafe_allow_html=True)

        with st.expander("📋 看完整榜單（表格）", expanded=False):
            st.dataframe(
                result.style.format({
                    "現價": "{:.2f}",
                    "較昨收(%)": "{:.2f}",
                    "累積量(張)": "{:,.0f}",
                    "盤中爆量倍數": "{:.2f}",
                    "前20日高": "{:.2f}",
                    "突破門檻": "{:.2f}",
                    "綜合分數": "{:.2f}",
                }),
                use_container_width=True,
                height=520,
            )

st.caption("懶人版設計：只留『風險等級』與『股票池』；其餘條件內建。若要專業版我也能再做。")

