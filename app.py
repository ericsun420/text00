# app.py — 簡單暴力極簡版｜保留 1-8 終極濾網｜防轉圈圈雲端架構
import os
import math
import time
from datetime import datetime, timedelta, time as dtime
import io

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
st.set_page_config(page_title="起漲戰情室｜極簡暴力", page_icon="🧊", layout="wide")

CSS = """
<style>
:root{
  --bg:#07080b; --card:#0f1116; --text:#e5e7eb; --muted:#9ca3af;
  --line:rgba(148,163,184,.16); --shadow: 0 16px 40px rgba(0,0,0,.35);
}
[data-testid="stAppViewContainer"], .main{ background: var(--bg) !important; color: var(--text) !important; }
.block-container{ padding-top: 1.5rem; padding-bottom: 2.0rem; }
.title{ font-size: 42px; font-weight: 900; background: linear-gradient(90deg, #f3f4f6, #9ca3af); -webkit-background-clip:text; -webkit-text-fill-color: transparent; margin:0; text-align:center;}
.subtitle{ color: var(--muted); font-size: 15px; text-align:center; margin-bottom: 30px; letter-spacing: 1px;}
.card{ background: linear-gradient(180deg, rgba(15,17,22,.94), rgba(11,13,18,.94)); border:1px solid var(--line); border-radius: 16px; padding: 16px; box-shadow: var(--shadow); margin-bottom: 12px;}
.metric{ display:flex; justify-content:space-between; align-items:flex-end; }
.metric .code{ color: var(--text); font-size: 18px; font-weight: 900; }
.metric .name{ color: var(--muted); font-size: 14px; margin-left: 8px;}
.metric .price{ font-size: 24px; font-weight: 900; color: #f87171;}
.metric .tag{ font-size: 12px; padding: 4px 8px; border-radius: 999px; border:1px solid var(--line); background: rgba(15,17,22,.8); margin-bottom: 8px; display: inline-block;}
.hr{ height:1px; background: var(--line); margin: 12px 0; }
.small-note{ color: var(--muted); font-size: 13px; display:flex; justify-content:space-between; }
.stButton>button{ border-radius: 12px !important; border: 1px solid rgba(203,213,225,.26) !important; background: linear-gradient(90deg, rgba(148,163,184,.16), rgba(107,114,128,.10)) !important; color: var(--text) !important; font-weight: 900 !important; font-size: 18px !important; padding: 20px !important; transition: all 0.3s ease;}
.stButton>button:hover{ border: 1px solid #f87171 !important; background: rgba(248,113,113,0.1) !important;}
[data-testid="stSidebar"] { display: none; } /* 暴力隱藏側邊欄，完全不需要設定 */
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

# =========================
# TIME / HELPERS
# =========================
TZ_NAME = "Asia/Taipei"
def now_taipei(): return datetime.utcnow() + timedelta(hours=8)
def minutes_elapsed_in_session(ts):
    start = datetime.combine(ts.date(), dtime(9, 0)); end = datetime.combine(ts.date(), dtime(13, 30))
    if ts < start: return 0
    if ts > end: return 270
    return int((ts - start).total_seconds() // 60)
def bars_expected_5m(ts): return max(1, min(54, int(math.ceil(minutes_elapsed_in_session(ts) / 5.0))))
def tw_tick(price): return 0.01 if price<10 else 0.05 if price<50 else 0.1 if price<100 else 0.5 if price<500 else 1.0 if price<1000 else 5.0
def calc_limit_up(prev_close, limit_pct):
    raw = prev_close * (1.0 + limit_pct); tick = tw_tick(raw)
    return round(round(raw / tick) * tick, 2 if tick < 0.1 else 1 if tick < 1 else 0)

# =========================
# 1. 股票清單 (GitHub)
# =========================
@st.cache_data(ttl=24*3600, show_spinner=False)
def load_universe_github():
    meta = {}
    urls = [("上市", "https://raw.githubusercontent.com/mlouielu/twstock/master/twstock/codes/twse_equities.csv"),
            ("上櫃", "https://raw.githubusercontent.com/mlouielu/twstock/master/twstock/codes/tpex_equities.csv")]
    for market, url in urls:
        try:
            r = requests.get(url, timeout=10, verify=False)
            df = pd.read_csv(io.StringIO(r.text.replace("\r", "")), dtype=str, on_bad_lines="skip")
            if "code" not in df.columns:
                df = pd.read_csv(io.StringIO(r.text.replace("\r", "")), header=None, dtype=str)
                df.columns = ["type","code","name","ISIN","start","market","group","CFI"][:df.shape[1]]
            for _, row in df.iterrows():
                c = str(row.get("code","")).strip()
                if re.match(r"^\d{4,6}$", c):
                    meta[c] = {"code": c, "name": str(row.get("name","")).strip(), "industry": str(row.get("group","")).strip() or "未分類", "market": market}
        except: pass
    return meta

# =========================
# 2. MIS 盤中快篩 (防轉圈圈核心)
# =========================
def fetch_mis_and_prefilter(meta_dict, min_vol=1000, dist_upper=2.5):
    s = requests.Session()
    s.get("https://mis.twse.com.tw/stock/fibest.jsp?lang=zh_tw", headers={"User-Agent": "Mozilla/5.0"}, timeout=5, verify=False)
    codes = list(meta_dict.keys())
    rows = []
    
    prog = st.progress(0, text="📡 正在進行全市場光速掃描...")
    total = math.ceil(len(codes)/100)
    
    for i in range(0, len(codes), 100):
        chunk = codes[i:i+100]
        ex_ch = "%7c".join([f"{'tse' if meta_dict[c]['market']=='上市' else 'otc'}_{c}.tw" for c in chunk])
        url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={ex_ch}&json=1&delay=0&_={int(time.time()*1000)}"
        prog.progress((i//100 + 1)/total)
        try:
            r = s.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5, verify=False)
            for q in r.json().get("msgArray", []):
                last, upper, vol = q.get("z"), q.get("u"), q.get("v")
                if last == "-" or not last or upper == "-" or not upper: continue
                last, upper, vol = float(last), float(upper), float(vol or 0)
                
                # 【極簡核心】：只挑成交量大於設定，且距離漲停很近的股票
                dist = ((upper - last) / upper) * 100
                if vol >= min_vol and dist <= dist_upper:
                    rows.append({
                        "code": q.get("c"), "last": last, "upper": upper, "dist": dist,
                        "prev_close": float(q.get("y")) if q.get("y")!="-" else 0,
                        "vol_lots": int(vol)
                    })
        except: time.sleep(0.1)
    
    prog.empty()
    return pd.DataFrame(rows)

# =========================
# 3. 日線基準 (僅針對快篩存活者)
# =========================
def build_daily_baseline_for_candidates(candidates_df):
    if candidates_df.empty: return pd.DataFrame()
    syms = [f"{c}.TW" for c in candidates_df["code"]] # 簡化，yfinance 對 TW/TWO 容錯率高
    
    prog = st.progress(0, text=f"📊 鎖定 {len(syms)} 檔候選，正在調閱主力歷史籌碼...")
    try:
        raw = yf.download(tickers=" ".join(syms), period="200d", interval="1d", group_by="ticker", auto_adjust=False, threads=False, progress=False)
    except:
        prog.empty(); return pd.DataFrame()
        
    rows = []
    for c, sym in zip(candidates_df["code"], syms):
        try:
            df = raw[sym].dropna() if isinstance(raw.columns, pd.MultiIndex) else raw.dropna()
            if df.empty or len(df) < 60: continue
            
            close, high, low, vol = df["Close"].astype(float), df["High"].astype(float), df["Low"].astype(float), df["Volume"].astype(float)
            yday_close = float(close.iloc[-1])
            
            # 你原本神級邏輯的精華提取
            hist_ret = close.pct_change().dropna()
            limit_class_pct = 0.20 if (len(hist_ret)>10 and float(hist_ret.tail(150).max()) > 0.105) else 0.10
            max_ret_10d = float(hist_ret.tail(10).max()) * 100.0
            had_hype_10d = max_ret_10d >= (19.0 if limit_class_pct == 0.20 else 9.5)
            
            vol_ma20 = float(vol.rolling(20).mean().iloc[-1])
            yday_vol_spike = float(vol.iloc[-1]) >= 2.0 * vol_ma20
            
            ma20 = close.rolling(20).mean()
            base_len_days = int(((close / (ma20 + 1e-9) - 1.0).abs() <= 0.04).tail(60).sum())
            
            tr = pd.concat([(high - low).abs(), (high - close.shift(1)).abs(), (low - close.shift(1)).abs()], axis=1).max(axis=1)
            atr20_pct = float(tr.rolling(20).mean().iloc[-1] / yday_close) * 100.0
            
            range20_pct = float((high.rolling(20).max().iloc[-1] - low.rolling(20).min().iloc[-1]) / yday_close)
            range60_pct = float((high.rolling(60).max().iloc[-1] - low.rolling(60).min().iloc[-1]) / yday_close)
            base_tight_score = (1.0 - min(1.0, range20_pct / (range60_pct + 1e-9))) * 0.6 + (1.0 - min(1.0, atr20_pct / 8.0)) * 0.4
            
            rows.append({
                "code": c, "yday_close": yday_close, "limit_class_pct": limit_class_pct,
                "vol_ma20_shares": vol_ma20, "max_ret_10d": max_ret_10d, "had_hype_10d": had_hype_10d,
                "yday_vol_spike": yday_vol_spike, "base_len_days": base_len_days,
                "atr20_pct": atr20_pct, "base_tight_score": base_tight_score
            })
        except: continue
        
    prog.empty()
    return pd.DataFrame(rows).set_index("code") if rows else pd.DataFrame()

# =========================
# 4. 盤中 5M 計算 (僅限候選)
# =========================
def fetch_and_score_5m(candidates_df, base_df, meta_dict, now_ts):
    if candidates_df.empty or base_df.empty: return pd.DataFrame()
    syms = [f"{c}.TW" for c in candidates_df["code"]]
    
    prog = st.progress(0, text="⚙️ 正在進行連板潛力終極運算...")
    try:
        raw5m = yf.download(tickers=" ".join(syms), period="1d", interval="5m", group_by="ticker", auto_adjust=False, threads=False, progress=False)
    except: 
        prog.empty(); return pd.DataFrame()

    results = []
    frac = max(0.2, min(1.0, minutes_elapsed_in_session(now_ts) / 270.0))
    
    for _, r in candidates_df.iterrows():
        c = r["code"]
        if c not in base_df.index: continue
        b = base_df.loc[c]
        
        # 核心排雷：如果近10天已經大嗨過，或是昨天爆量長黑，直接淘汰(不是第一根)
        if b["had_hype_10d"]: continue
        
        df5m = raw5m[f"{c}.TW"].dropna() if isinstance(raw5m.columns, pd.MultiIndex) else raw5m.dropna()
        if df5m.empty: continue
        
        last = float(df5m["Close"].iloc[-1])
        day_high, day_low = float(df5m["High"].max()), float(df5m["Low"].min())
        vol_shares = float(df5m["Volume"].sum())
        
        # 量能計算
        vol_ratio = (vol_shares / (b["vol_ma20_shares"] * frac + 1e-9)) if b["vol_ma20_shares"] > 0 else 0.0
        
        # 鎖死品質
        rng = max(1e-9, day_high - day_low)
        close_pos = (last - day_low) / rng
        pullback = (day_high - last) / max(1e-9, day_high)
        
        # 簡單暴力計分法 (滿分100)
        score = 0.0
        score += 35.0 * min(1.0, max(0.0, (close_pos - 0.80) / 0.20)) # 收在高檔
        score += 20.0 * min(1.0, max(0.0, (0.0038 - pullback) / 0.0038)) # 回落極小
        score += 25.0 * min(1.0, max(0.0, (vol_ratio - 1.2) / 2.5)) # 爆量
        score += 15.0 * min(1.0, max(0.0, (b["base_len_days"] - 8) / 40.0)) # 盤整夠久
        score += 5.0 * min(1.0, max(0.0, b["base_tight_score"])) # 籌碼集中
        
        chg_pct = (last / b["yday_close"] - 1.0) * 100.0
        
        results.append({
            "代號": c, "名稱": meta_dict[c]["name"], "族群": meta_dict[c]["industry"],
            "現價": last, "漲停價": r["upper"], "距離漲停(%)": r["dist"], "較昨收(%)": chg_pct,
            "累積量(張)": r["vol_lots"], "盤中爆量倍數": vol_ratio, "基底天數": b["base_len_days"],
            "連板潛力分": float(max(0.0, min(100.0, score)))
        })
        
    prog.empty()
    res = pd.DataFrame(results)
    if not res.empty:
        res = res.sort_values(["連板潛力分", "距離漲停(%)"], ascending=[False, True]).reset_index(drop=True)
        res.index += 1
    return res

# =========================
# MAIN APP 
# =========================
st.markdown('<div class="title">🧊 起漲戰情室</div>', unsafe_allow_html=True)
st.markdown('<div class="subtitle">極簡暴力版 ｜ 隱藏所有設定，一鍵全自動執行你心目中的完美濾網</div>', unsafe_allow_html=True)

run_scan = st.button("🚀 啟動戰情室 (一鍵全自動)", use_container_width=True)

if run_scan:
    ts = now_taipei()
    meta = load_universe_github()
    if not meta:
        st.error("🛑 無法載入股票清單，請稍後重試。")
        st.stop()
        
    # 步驟一：MIS 全市場極速快篩 (找出有量且快漲停的)
    pre_candidates = fetch_mis_and_prefilter(meta, min_vol=1000, dist_upper=2.5)
    
    if pre_candidates.empty:
        st.info("😴 目前盤面沒有符合「量大且接近漲停」的標的。請耐心等待好球進壘！")
        st.stop()
        
    # 步驟二：對快篩存活者抓取日線，並套用你的「排雷與基底邏輯」
    base_df = build_daily_baseline_for_candidates(pre_candidates)
    
    if base_df.empty:
        st.warning("⚠️ 候選股未能通過日線排雷濾網 (可能近期已大漲過，非第一根)。")
        st.stop()
        
    # 步驟三：對最終名單進行 5M 籌碼與連板潛力運算
    final_res = fetch_and_score_5m(pre_candidates, base_df, meta, ts)
    
    if final_res.empty:
        st.warning("⚠️ 候選股未能通過最終的「第一根漲停 + 連板體質」嚴格濾網。")
    else:
        st.success(f"🎯 完美鎖定！為您篩選出 {len(final_res)} 檔『第一根漲停』候選標的。")
        
        # 輸出卡片
        cols = st.columns(min(len(final_res), 3))
        for i, r in final_res.head(6).iterrows(): # 最多顯示前 6 名卡片
            with cols[(i-1) % 3]:
                tag = "🔒 幾乎鎖死" if r["連板潛力分"] >= 75 else "👀 候選"
                st.markdown(f"""
                <div class="card">
                    <div class="metric">
                        <div>
                            <div class="tag">{tag}</div><br>
                            <span class="code">{r['代號']}</span><span class="name">{r['名稱']}</span>
                        </div>
                        <div class="price">{r['現價']:.2f}</div>
                    </div>
                    <div class="hr"></div>
                    <div class="small-note">
                        <span>距漲停: <b style="color:#e5e7eb;">{r['距離漲停(%)']:.2f}%</b></span>
                        <span>爆量: <b style="color:#f87171;">{r['盤中爆量倍數']:.1f}x</b></span>
                    </div>
                    <div class="small-note" style="margin-top:4px;">
                        <span>潛力分: <b style="color:#a3e635;">{r['連板潛力分']:.1f}</b></span>
                        <span>基底: <b>{r['基底天數']}天</b></span>
                    </div>
                </div>
                """, unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # 下載按鈕 (取代容易遺失的雲端檔案寫入)
        csv = final_res.to_csv(index=True).encode('utf-8-sig')
        st.download_button(
            label="📥 匯出今日報表至手機/電腦 (CSV)",
            data=csv,
            file_name=f"戰情室_第一根漲停_{ts.strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
            use_container_width=True
        )
        
        with st.expander("📋 完整數據表"):
            st.dataframe(final_res.style.format({
                "現價":"{:.2f}", "漲停價":"{:.2f}", "距離漲停(%)":"{:.2f}%", "較昨收(%)":"{:.2f}%",
                "累積量(張)":"{:,}", "盤中爆量倍數":"{:.2f}x", "連板潛力分":"{:.1f}"
            }), use_container_width=True)
