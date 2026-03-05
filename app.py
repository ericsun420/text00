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
# 美化 CSS (冷酷黑灰風格)
# =========================
st.set_page_config(page_title="起漲戰情室", page_icon="🚀", layout="wide")

CSS = """
<style>
:root{
  --bg:#080808;             /* 純黑背景 */
  --card:#1a1a1a;           /* 深灰卡片 */
  --card2:#121212;          /* 次級深灰 */
  --text:#ffffff;           /* 純白文字 */
  --muted:#aaaaaa;          /* 靜音文字 */
  
  /* 霓虹藍冷色調點綴 */
  --accent:#00c2ff;          /* 主點綴色 */
  --accent2:#007bff;         /* 次點綴色 */
  
  --ok:#00c2ff;             /* 霓虹藍代替綠色 */
  --warn:#f4b400;           /* 琥珀黃 */
  --bad:#cf6679;            /* 啞光紅 */
  --line:rgba(255,255,255,.03); /* 極淡的線條 */
}

/* 隱藏預設的主選單與 footer 使畫面更乾淨 */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}

.main { background: var(--bg); }
.block-container { padding-top: 1.2rem; padding-bottom: 2.2rem; }

/* 頂部 Header */
.header-wrap{
  display:flex; align-items:flex-end; justify-content:space-between;
  gap:18px; padding: 10px 4px 6px 4px;
}
.title{
  font-size: 44px; font-weight: 900; letter-spacing: .5px;
  background: linear-gradient(90deg, var(--accent), var(--accent2));
  -webkit-background-clip:text; -webkit-text-fill-color: transparent;
  margin:0;
}
.subtitle{
  margin:6px 0 0 2px; color: var(--muted); font-size: 14px;
}

/* 膠囊 */
.pill{
  display:inline-flex; align-items:center; gap:8px;
  padding: 8px 12px; border:1px solid var(--line);
  border-radius: 999px; color: var(--text); background: var(--card);
  font-size: 13px;
}
.pill b{ color: var(--text); }
.pill .dot{ width:8px; height:8px; border-radius:999px; background: var(--warn); display:inline-block; }

/* 網格系統 */
.grid{
  display:grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 12px;
  margin: 14px 0 6px 0;
}
.card{
  background: var(--card);
  border:1px solid var(--line);
  border-radius: 16px;
  padding: 14px 14px 12px 14px;
  box-shadow: 0 4px 6px rgba(0,0,0,.2);
}
.k{ color: var(--muted); font-size: 12px; margin-bottom: 6px; }
.v{ color: var(--text); font-size: 20px; font-weight: 800; }
.v small{ color: var(--muted); font-weight: 600; font-size: 12px; margin-left: 6px;}
.hr{ height:1px; background: var(--line); margin: 12px 0; }

/* 提示 banner */
.banner{
  background: rgba(244, 180, 0, .05); /* 琥珀黃低透明度 */
  border: 1px solid rgba(244, 180, 0, .15);
  color: var(--text);
  border-radius: 16px;
  padding: 12px 14px;
  margin: 10px 0 10px 0;
}
.banner b{ color: #fff; }

/* 戰情卡片內部的 Metric */
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
  background: rgba(255,255,255,.05);
}
.metric .price{ font-size: 22px; font-weight: 900; color: var(--text); line-height: 1; }
.metric .chg{ font-size: 12px; color: var(--muted); }

/* Streamlit 預設元件美化 */
.stButton>button{
  border-radius: 14px !important;
  border: 1px solid rgba(0,194,255,.3) !important;
  background: linear-gradient(90deg, rgba(0,194,255,.15), rgba(0,123,255,.08)) !important;
  color: var(--text) !important;
  font-weight: 800 !important;
  padding: 10px 14px !important;
  transition: all .2s;
}
.stButton>button:hover{
  border-color: rgba(0,194,255,.6) !important;
  box-shadow: 0 0 10px rgba(0,194,255,.2);
}
.stSelectbox>div>div{
  border-radius: 14px !important;
}
.small-note{ color: var(--muted); font-size: 12px; }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

# =========================
# 時間工具 (維持不變)
# =========================
def now_taipei() -> datetime:
    return datetime.utcnow() + timedelta(hours=8)

def minutes_elapsed_in_session(ts: datetime) -> int:
    start = datetime.combine(ts.date(), dtime(9, 0))
    end = datetime.combine(ts.date(), dtime(13, 30))
    if ts < start: return 0
    if ts > end: return 270
    return int((ts - start).total_seconds() // 60)

# =========================
# 上市清單抓取與解碼 (維持不變)
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
# 日線基準計算 (維持不變)
# =========================
def _drop_today_bar_if_exists(df: pd.DataFrame, today_date) -> pd.DataFrame:
    if df.empty: return df
    if pd.Timestamp(df.index[-1]).date() == today_date: return df.iloc[:-1].copy()
    return df

@st.cache_data(ttl=6 * 3600)
def build_daily_baselines(codes: list[str]) -> pd.DataFrame:
    end_date = now_taipei().date()
    start = (now_taipei() - timedelta(days=200)).date().isoformat()
    records = []
    for i in range(0, len(codes), 60):
        chunk = codes[i:i + 60]
        tickers = " ".join([f"{c}.TW" for c in chunk])
        try:
            raw = yf.download(tickers=tickers, start=start, interval="1d", group_by="ticker", auto_adjust=False, threads=True, progress=False)
        except Exception: continue
        for c in chunk:
            t = f"{c}.TW"
            try:
                if isinstance(raw.columns, pd.MultiIndex):
                    if t not in raw.columns.get_level_values(0): continue
                    df = raw[t].dropna().copy()
                else: df = raw.dropna().copy()
                df = _drop_today_bar_if_exists(df, end_date)
                if df.empty or len(df) < 80: continue
                records.append({
                    "code": c,
                    "vol_ma20_shares": float(df["Volume"].rolling(20).mean().iloc[-1]),
                    "high20": float(df["High"].rolling(20).max().shift(1).iloc[-1]),
                    "ma60": float(df["Close"].rolling(60).mean().iloc[-1]),
                    "yday_vol_shares": float(df["Volume"].iloc[-1]),
                    "yday_close": float(df["Close"].iloc[-1]),
                    "change_5d": float((df["Close"].iloc[-1] / df["Close"].iloc[-6] - 1.0)) if len(df) >= 6 else None,
                })
            except Exception: continue
    return pd.DataFrame(records).drop_duplicates("code")

# =========================
# 盤中快照 (維持不變)
# =========================
@st.cache_data(ttl=20)
def fetch_intraday_snapshot_yf(codes: list[str], interval: str = "5m", batch_size: int = 30) -> pd.DataFrame:
    out = []
    for i in range(0, len(codes), batch_size):
        chunk = codes[i:i + batch_size]
        tickers = " ".join([f"{c}.TW" for c in chunk])
        try:
            raw = yf.download(tickers=tickers, period="1d", interval=interval, group_by="ticker", auto_adjust=False, threads=True, progress=False)
        except Exception: continue
        for c in chunk:
            t = f"{c}.TW"
            try:
                if isinstance(raw.columns, pd.MultiIndex):
                    if t not in raw.columns.get_level_values(0): continue
                    df = raw[t].dropna().copy()
                else: df = raw.dropna().copy()
                if df.empty: continue
                out.append({
                    "code": c, "last": float(df["Close"].iloc[-1]), "open": float(df["Open"].iloc[0]),
                    "high": float(df["High"].max()), "low": float(df["Low"].min()), "vol_lots": int(df["Volume"].sum() / 1000),
                })
            except Exception: continue
        time.sleep(0.1)
    df = pd.DataFrame(out)
    return df if df.empty else df.drop_duplicates("code")

# =========================
# 懶人預設參數
# =========================
PRESETS = {
    "保守（少訊號、盡量避雷）": dict(
        breakout_buffer_pct=1.2, vol_mult=3.0, close_pos_min=0.75, upper_shadow_max=0.25, min_cum_lots=2500, require_above_ma60=True, avoid_yday_spike=True, yday_spike_mult=1.6, avoid_overheat_5d=True, overheat_5d_max=15, require_green_body=True, body_min_pct=3.0,
    ),
    "標準（大多數人用）": dict(
        breakout_buffer_pct=1.0, vol_mult=2.5, close_pos_min=0.70, upper_shadow_max=0.30, min_cum_lots=2000, require_above_ma60=True, avoid_yday_spike=True, yday_spike_mult=1.8, avoid_overheat_5d=True, overheat_5d_max=18, require_green_body=True, body_min_pct=2.5,
    ),
    "積極（多訊號、容忍波動）": dict(
        breakout_buffer_pct=0.6, vol_mult=2.0, close_pos_min=0.62, upper_shadow_max=0.38, min_cum_lots=1200, require_above_ma60=False, avoid_yday_spike=False, yday_spike_mult=2.2, avoid_overheat_5d=False, overheat_5d_max=25, require_green_body=False, body_min_pct=2.0,
    ),
}

# =========================
# 掃描邏輯 (修正排序 Bug)
# =========================
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
    if p["require_green_body"]:
        df["cond_green_body"] = (df["last"] > df["open"]) & (df["body_return"] >= p["body_min_pct"] / 100.0)
    else: df["cond_green_body"] = True
    elapsed = minutes_elapsed_in_session(now_ts)
    frac = max(0.2, max(1, min(270, elapsed)) / 270.0)
    df["vol_ratio_now"] = df["cum_vol_shares"] / (df["vol_ma20_shares"] * frac + 1e-9)
    df["cond_vol_burst"] = df["vol_ratio_now"] >= p["vol_mult"]
    df["cond_min_cum"] = df["vol_lots"].astype(int) >= int(p["min_cum_lots"])
    if p["require_above_ma60"]:
        df = df.dropna(subset=["ma60"])
        df["cond_above_ma60"] = df["last"] > df["ma60"]
    else: df["cond_above_ma60"] = True
    if p["avoid_yday_spike"]:
        df["cond_yday_ok"] = df["yday_vol_shares"] <= (df["vol_ma20_shares"] * p["yday_spike_mult"])
    else: df["cond_yday_ok"] = True
    if p["avoid_overheat_5d"]:
        df["cond_overheat_ok"] = df["change_5d"].fillna(0) <= p["overheat_5d_max"] / 100.0
    else: df["cond_overheat_ok"] = True
    cond = (
        df["cond_breakout"] & df["cond_vol_burst"] & df["cond_close_pos"] & df["cond_shadow"] & df["cond_min_cum"] & df["cond_above_ma60"] & df["cond_yday_ok"] & df["cond_overheat_ok"] & df["cond_green_body"]
    )
    out = df[cond].copy()
    if out.empty: return out
    out["chg_pct_vs_yday"] = (out["last"] / out["yday_close"] - 1.0) * 100.0
    out["score"] = (2.0 * out["vol_ratio_now"].clip(0, 10) + 1.5 * out["close_pos"].clip(0, 1) - 1.0 * out["upper_shadow_ratio"].clip(0, 1))
    
    # 這裡修正：排序完後，重置 index，確保 idx 是 0, 1, 2... 而不是股票原本的序號
    out = out.sort_values(["score", "vol_ratio_now"], ascending=False).reset_index(drop=True)
    
    show = out[["code","last","chg_pct_vs_yday","vol_lots","vol_ratio_now","high20","breakout_level","score"]].copy()
    show.rename(columns={
        "code":"代號","last":"現價","chg_pct_vs_yday":"較昨收(%)","vol_lots":"累積量(張)","vol_ratio_now":"盤中爆量倍數","high20":"前20日高","breakout_level":"突破門檻","score":"綜合分數"
    }, inplace=True)
    return show

# =========================
# Sidebar 與 Header
# =========================
st.sidebar.markdown("### 🧠 懶人設定")
preset_name = st.sidebar.selectbox("風險等級", list(PRESETS.keys()), index=1)
pool_mode = st.sidebar.selectbox("掃描模式", ["流動性預篩（推薦）", "全上市（很慢）"], index=0)
st.sidebar.markdown("---")
run_scan = st.sidebar.button("🚀 立即掃描", use_container_width=True)
refresh_base = st.sidebar.button("🔄 重建日線快取", use_container_width=True)

now_ts = now_taipei()
elapsed = minutes_elapsed_in_session(now_ts)
st.markdown(f"""
<div class="header-wrap">
  <div>
    <h1 class="title">起漲戰情室</h1>
    <div class="subtitle">黑灰霓虹版｜新手懶人一鍵掃描</div>
  </div>
  <div class="pill"><span class="dot"></span> 台北時間 <b>{now_ts.strftime('%H:%M:%S')}</b>　盤中進度 <b>{elapsed}/270</b></div>
</div>
""", unsafe_allow_html=True)

# =========================
# 資料準備與畫面渲染
# =========================
try:
    stock_df = fetch_all_twse_listed_stocks()
except Exception as e:
    st.error(f"抓上市清單失敗：{e}"); st.stop()
all_codes = stock_df["code"].tolist()
if refresh_base: build_daily_baselines.clear(); st.success("已清除日線快取。")

base_df = None
codes_to_scan = all_codes
universe_label = "全上市"
if pool_mode.startswith("流動性預篩"):
    with st.spinner("建立日線基準..."): base_df = build_daily_baselines(all_codes)
    codes_to_scan = base_df[base_df["vol_ma20_shares"] >= 500_000]["code"].tolist()
    universe_label = f"流動性預篩（{len(codes_to_scan)} 檔）"

st.markdown(f"""
<div class="grid">
  <div class="card"><div class="k">股票池</div><div class="v">{universe_label}</div></div>
  <div class="card"><div class="k">風險等級</div><div class="v">{preset_name.split('（')[0]}</div></div>
  <div class="card"><div class="k">判斷邏輯</div><div class="v">突破+爆量+收高</div></div>
  <div class="card"><div class="k">建議時間</div><div class="v">13:15 – 13:25</div></div>
</div>
""", unsafe_allow_html=True)

st.markdown("""<div class="banner"><b>冷酷提示：</b>嚴格執行停損，只做爆量起漲的「第一根」。</div>""", unsafe_allow_html=True)

if run_scan:
    with st.spinner("取得日線基準..."):
        if base_df is None: base_df = build_daily_baselines(codes_to_scan)
    with st.spinner("抓取盤中快照..."): quotes_df = fetch_intraday_snapshot_yf(codes_to_scan)
    if quotes_df.empty: st.error("抓不到資料。"); st.stop()
    with st.spinner("計算訊號..."): result = scan_intraday_breakouts(quotes_df, base_df, now_ts, PRESETS[preset_name])
    
    st.markdown("<div class='hr'></div>", unsafe_allow_html=True)
    found = 0 if result is None or len(result) == 0 else len(result)
    
    # 訊號卡片
    cols = st.columns(4)
    if found == 0:
        st.warning("目前沒有符合條件。")
    else:
        st.success(f"🎯 今日掃到 {found} 檔標的")
        # 只顯示 Top 12 卡片，修正上一版排版問題
        topn = result.head(12).copy()
        for idx, row in topn.iterrows():
            c = cols[idx % 4]
            with c:
                tag = "🔥強" if idx < 4 else "✅可看"
                st.markdown(f"""
<div class="card">
  <div class="metric">
    <div class="left">
      <div class="label">#{idx+1} <span class="tag">{tag}</span></div>
      <div class="code">{row['代號']}</div>
    </div>
    <div style="text-align:right">
      <div class="price">{row['現價']:.2f}</div>
      <div class="chg">{row['較昨收(%)']:.2f}%</div>
    </div>
  </div>
  <div class="hr"></div>
  <div class="small-note">量：{int(row['累積量(張)']):,}張｜爆量：{row['盤中爆量倍數']:.2f}x</div>
  <div class="small-note">綜合分數：{row['綜合分數']:.2f}</div>
</div>
""", unsafe_allow_html=True)
        
        st.markdown("<div class='hr'></div>", unsafe_allow_html=True)
        with st.expander("📋 看完整榜單（表格）"):
            st.dataframe(result.style.format({
                "現價":"{:.2f}","較昨收(%)":"{:.2f}","累積量(張)":"{:,.0f}","盤中爆量倍數":"{:.2f}","前20日高":"{:.2f}","突破門檻":"{:.2f}","綜合分數":"{:.2f}"
            }).background_gradient(subset=['較昨收(%)','綜合分數'], cmap='Blues'), use_container_width=True, height=520)
