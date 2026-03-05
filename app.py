import math
import time
import html
import re
import io
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dtime
from typing import Dict, List

import requests
import urllib3
import pandas as pd
import yfinance as yf
import streamlit as st
import streamlit.components.v1 as components

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================
# UI / THEME (冷酷黑灰)
# =========================
st.set_page_config(page_title="起漲戰情室｜手機雲端版", page_icon="📱", layout="wide")

CSS = """
<style>
:root{ --bg:#07080b; --panel:#0b0d12; --card:#0f1116; --text:#e5e7eb; --muted:#9ca3af; --line:rgba(148,163,184,.16); --shadow: 0 16px 40px rgba(0,0,0,.35); --hi: rgba(148,163,184,.08); }
[data-testid="stAppViewContainer"], .main { background: var(--bg) !important; color: var(--text) !important; }
.block-container{ padding-top: 1rem; padding-bottom: 2.0rem; }
[data-testid="stSidebar"]{ background: var(--panel) !important; border-right: 1px solid var(--line) !important; }
[data-testid="stSidebar"] *{ color: var(--text) !important; }
.title{ font-size: 36px; font-weight: 900; background: linear-gradient(90deg, #f3f4f6, #9ca3af); -webkit-background-clip:text; -webkit-text-fill-color: transparent; margin:0; }
.subtitle{ color: var(--muted); font-size: 14px; margin-bottom: 15px;}
.grid{ display:grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 10px; margin: 10px 0; }
.card{ background: linear-gradient(180deg, rgba(15,17,22,.94), rgba(11,13,18,.94)); border:1px solid var(--line); border-radius: 12px; padding: 12px; box-shadow: var(--shadow); }
.k{ color: var(--muted); font-size: 12px; margin-bottom: 4px; }
.v{ color: var(--text); font-size: 18px; font-weight: 800; }
.banner{ background: rgba(148,163,184,.08); border: 1px solid rgba(148,163,184,.22); border-radius:12px; padding: 10px; margin: 10px 0; color: var(--text); font-size: 13px;}
.metric .label{ color: var(--muted); font-size: 12px; }
.metric .code{ color: var(--text); font-size: 15px; font-weight: 900; }
.metric .price{ font-size: 20px; font-weight: 900; color: #f87171; float: right;} /* 紅色代表強勢 */
.stButton>button{ border-radius: 10px !important; border: 1px solid rgba(203,213,225,.26) !important; background: linear-gradient(90deg, rgba(148,163,184,.16), rgba(107,114,128,.10)) !important; color: var(--text) !important; font-weight: 800 !important; }
.stSelectbox>div>div, .stNumberInput>div>div{ border-radius: 10px !important; border: 1px solid var(--line) !important; background: rgba(15,17,22,.88) !important; color: var(--text) !important;}
.hr { border-bottom: 1px solid var(--line); margin: 10px 0; }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

# =========================
# CORE FUNCTIONS
# =========================
def now_taipei(): return datetime.utcnow() + timedelta(hours=8)

def tw_tick(price):
    if price < 10: return 0.01
    if price < 50: return 0.05
    if price < 100: return 0.10
    if price < 500: return 0.50
    if price < 1000: return 1.00
    return 5.00

def calc_limit_up(prev_close, limit_pct):
    raw = prev_close * (1.0 + limit_pct)
    tick = tw_tick(raw)
    return round(round(raw / tick) * tick, 2 if tick < 0.1 else 1 if tick < 1 else 0)

# =========================
# 1. 股票清單 (GitHub 穩定版)
# =========================
@dataclass
class Meta:
    code: str; name: str; market: str; industry: str; ex: str; yf_symbol: str

@st.cache_data(ttl=24*3600, show_spinner=False)
def load_universe_github(include_tpex: bool):
    meta = {}
    urls = [
        ("tse", "上市", "https://raw.githubusercontent.com/mlouielu/twstock/master/twstock/codes/twse_equities.csv"),
        ("otc", "上櫃", "https://raw.githubusercontent.com/mlouielu/twstock/master/twstock/codes/tpex_equities.csv")
    ]
    for ex, market, url in urls:
        if market == "上櫃" and not include_tpex: continue
        try:
            r = requests.get(url, timeout=10, verify=False)
            df = pd.read_csv(io.StringIO(r.text.replace("\r", "")), dtype=str)
            if "code" not in df.columns:
                df = pd.read_csv(io.StringIO(r.text.replace("\r", "")), header=None, dtype=str)
                df.columns = ["type","code","name","ISIN","start","market","group","CFI"][:df.shape[1]]
            for _, row in df.iterrows():
                c = str(row.get("code","")).strip()
                if re.match(r"^\d{4,6}$", c):
                    meta[c] = Meta(c, str(row.get("name","")).strip(), market, str(row.get("group","")).strip() or "未分類", ex, f"{c}.{'TW' if ex=='tse' else 'TWO'}")
        except: pass
    return meta

# =========================
# 2. 盤中快照 (MIS)
# =========================
def fetch_mis_snapshot(meta_items, batch_size=100):
    s = requests.Session()
    s.get("https://mis.twse.com.tw/stock/fibest.jsp?lang=zh_tw", headers={"User-Agent": "Mozilla/5.0"}, timeout=5, verify=False)
    rows = []
    
    prog_bar = st.progress(0, text="📡 正在向證交所請求盤中快照...")
    total = math.ceil(len(meta_items)/batch_size)

    for i in range(0, len(meta_items), batch_size):
        chunk = meta_items[i:i+batch_size]
        ex_ch = "%7c".join([f"{m.ex}_{m.code}.tw" for m in chunk])
        url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={ex_ch}&json=1&delay=0&_={int(time.time()*1000)}"
        prog_bar.progress((i//batch_size + 1)/total, text=f"📡 MIS 掃描中 ({i//batch_size + 1}/{total})...")
        try:
            r = s.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5, verify=False)
            arr = r.json().get("msgArray", [])
            for q in arr:
                if q.get("z") == "-" or not q.get("z"): continue
                rows.append({
                    "code": q.get("c"),
                    "last": float(q.get("z")),
                    "prev_close": float(q.get("y")) if q.get("y") != "-" else 0,
                    "upper": float(q.get("u")) if q.get("u") != "-" else 0,
                    "volume_lots": int(float(q.get("v") or 0)),
                })
        except: time.sleep(0.1)
    
    prog_bar.empty()
    return pd.DataFrame(rows)

# =========================
# 3. 日線基準 (僅抓少量候選股)
# =========================
def build_daily_baseline_for_candidates(candidate_symbols):
    if not candidate_symbols: return pd.DataFrame()
    rows = []
    prog_bar = st.progress(0, text="📊 正在調閱候選股歷史日線...")
    
    try:
        raw = yf.download(tickers=" ".join(candidate_symbols), period="60d", interval="1d", group_by="ticker", auto_adjust=False, threads=False, progress=False)
        for idx, sym in enumerate(candidate_symbols):
            prog_bar.progress((idx+1)/len(candidate_symbols), text=f"📊 分析歷史籌碼 ({idx+1}/{len(candidate_symbols)})...")
            try:
                df = raw[sym].dropna() if isinstance(raw.columns, pd.MultiIndex) else raw.dropna()
                if df.empty or len(df) < 20: continue
                close = df["Close"].astype(float)
                vol = df["Volume"].astype(float)
                
                vol_ma20_lots = int(vol.rolling(20).mean().iloc[-1] / 1000)
                max_ret_10d = float(close.pct_change().tail(10).max() * 100)
                
                rows.append({
                    "yf_symbol": sym,
                    "vol_ma20_lots": vol_ma20_lots,
                    "max_ret_10d": max_ret_10d
                })
            except: continue
    except: pass
    
    prog_bar.empty()
    return pd.DataFrame(rows).set_index("yf_symbol") if rows else pd.DataFrame()

# =========================
# UI 介面與主程式
# =========================
st.markdown('<div class="title">📱 起漲戰情室</div>', unsafe_allow_html=True)
st.markdown('<div class="subtitle">雲端手機專用版：一鍵秒開，精準鎖定第一根。</div>', unsafe_allow_html=True)

with st.sidebar:
    st.markdown("### ⚙️ 戰鬥設定")
    market_mode = st.selectbox("市場", ["上市 + 上櫃", "只掃上市"])
    min_vol = st.number_input("最低盤中量(張)", min_value=100, value=800, step=100)
    dist_upper = st.number_input("距離漲停(%)", min_value=0.1, value=1.5, step=0.1)
    run_scan = st.button("🚀 立即啟動掃描", use_container_width=True)
    st.markdown("---")
    st.caption("ℹ️ 雲端版優化：平時不耗資源，按下掃描後才連線，絕不卡死。")

st.markdown(f"""
<div class="grid">
  <div class="card"><div class="k">狀態</div><div class="v">{'🟢 等待掃描' if not run_scan else '🔥 掃描中'}</div></div>
  <div class="card"><div class="k">最低量門檻</div><div class="v">{min_vol} 張</div></div>
  <div class="card"><div class="k">貼板距離</div><div class="v">{dist_upper}%</div></div>
</div>
""", unsafe_allow_html=True)

if run_scan:
    # 步驟 1: 載入清單
    meta_dict = load_universe_github(include_tpex="上櫃" in market_mode)
    if not meta_dict:
        st.error("🛑 載入清單失敗，請稍後重試。")
        st.stop()
        
    # 步驟 2: MIS 快篩全市場
    snap = fetch_mis_snapshot(list(meta_dict.values()))
    if snap.empty:
        st.error("🛑 無法連線至證交所，請確認目前是否為交易時間。")
        st.stop()
        
    # 快篩邏輯：量大 + 快漲停
    snap["dist"] = ((snap["upper"] - snap["last"]) / snap["upper"]) * 100
    pre_candidates = snap[(snap["volume_lots"] >= min_vol) & (snap["dist"] <= dist_upper)].copy()
    
    if pre_candidates.empty:
        st.info("😴 目前盤面沒有符合「量大且接近漲停」的飆股。")
        st.stop()
        
    # 步驟 3: 針對候選股要歷史日線 (排雷用)
    pre_candidates["yf_symbol"] = pre_candidates["code"].apply(lambda c: meta_dict[c].yf_symbol)
    base_df = build_daily_baseline_for_candidates(pre_candidates["yf_symbol"].tolist())
    
    # 步驟 4: 計算分數與產出
    final_rows = []
    for _, r in pre_candidates.iterrows():
        sym = r["yf_symbol"]
        if base_df is not None and sym in base_df.index:
            b = base_df.loc[sym]
            # 排雷：近10日漲幅若超過 9.5%，代表已經飆過了，不是「第一根」
            if b["max_ret_10d"] >= 9.5: continue
            vol_ma = b["vol_ma20_lots"]
        else:
            vol_ma = r["volume_lots"] / 2 # 防呆估算

        vol_ratio = r["volume_lots"] / (vol_ma + 1e-9)
        score = min(100, 50 + (vol_ratio * 10) + ((dist_upper - r["dist"]) * 20))
        
        final_rows.append({
            "代號": r["code"], "名稱": meta_dict[r["code"]].name, "族群": meta_dict[r["code"]].industry,
            "現價": r["last"], "距離漲停(%)": r["dist"], "累積量(張)": r["volume_lots"], 
            "爆量倍數": vol_ratio, "潛力分": score
        })

    if not final_rows:
        st.warning("⚠️ 有股票接近漲停，但都被『非第一根』濾網排除了 (近期已大漲過)。")
        st.stop()
        
    res = pd.DataFrame(final_rows).sort_values("潛力分", ascending=False).reset_index(drop=True)
    res.index += 1
    
    st.success(f"🎯 成功鎖定 {len(res)} 檔『第一根漲停』完美標的！")
    
    # 手機版精美卡片呈現
    cols = st.columns(min(len(res), 2)) # 手機通常排 2 列最剛好
    for i, r in res.head(6).iterrows():
        with cols[(i-1) % 2]:
            st.markdown(f"""
            <div class="card" style="margin-bottom:10px;">
                <div class="metric">
                    <div class="left"><div class="label">#{i} {r['族群']}</div><div class="code">{r['代號']} {r['名稱']}</div></div>
                    <div class="price">{r['現價']:.2f}</div>
                </div>
                <div class="hr"></div>
                <div style="display:flex; justify-content:space-between; font-size:12px; color:#9ca3af;">
                    <span>距漲停: <b style="color:#e5e7eb;">{r['距離漲停(%)']:.2f}%</b></span>
                    <span>爆量: <b style="color:#f87171;">{r['爆量倍數']:.1f}x</b></span>
                </div>
            </div>
            """, unsafe_allow_html=True)
    
    # 匯出 CSV 按鈕 (取代原本會消失的本機儲存)
    csv = res.to_csv(index=True).encode('utf-8-sig')
    st.download_button(
        label="📥 匯出今日報表 (存至手機/電腦)",
        data=csv,
        file_name=f"起漲戰情室_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
        mime="text/csv",
        use_container_width=True
    )
    
    with st.expander("📋 完整數據表"):
        st.dataframe(res.style.format({"現價":"{:.2f}", "距離漲停(%)":"{:.2f}%", "累積量(張)":"{:,}", "爆量倍數":"{:.1f}x", "潛力分":"{:.1f}"}), use_container_width=True)
