# app.py — 起漲先機 (1~N根通吃)｜MIS 盤中極速入口｜精準 Tick 連板計算
import io
import math
import time
import re
from datetime import datetime, timedelta, time as dtime

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
st.set_page_config(page_title="起漲戰情室｜極簡暴力", page_icon="⚡", layout="wide", initial_sidebar_state="collapsed")

CSS = """
<style>
:root{ --bg:#07080b; --card:#0f1116; --text:#e5e7eb; --muted:#9ca3af; --line:rgba(148,163,184,.16); --shadow: 0 16px 40px rgba(0,0,0,.35); }
[data-testid="stAppViewContainer"], .main{ background: var(--bg) !important; color: var(--text) !important; }
.block-container{ padding-top: 2rem; padding-bottom: 3rem; max-width: 1200px; }
[data-testid="stSidebar"] { display: none !important; }
[data-testid="stHeader"]{ background: transparent !important; }
.title{ font-size: 46px; font-weight: 900; background: linear-gradient(90deg, #f3f4f6, #9ca3af); -webkit-background-clip:text; -webkit-text-fill-color: transparent; margin:0; text-align: center; }
.subtitle{ color: var(--muted); font-size: 15px; text-align: center; margin-bottom: 30px; letter-spacing: 1px; }
.card{ background: linear-gradient(180deg, rgba(15,17,22,.94), rgba(11,13,18,.94)); border:1px solid var(--line); border-radius: 16px; padding: 18px; box-shadow: var(--shadow); margin-bottom: 12px; }
.metric{ display:flex; justify-content:space-between; align-items:flex-end; border-bottom: 1px solid var(--line); padding-bottom: 10px; margin-bottom: 10px;}
.metric .code{ color: var(--text); font-size: 20px; font-weight: 900; }
.metric .name{ color: var(--muted); font-size: 14px; margin-left: 8px;}
.metric .price{ font-size: 26px; font-weight: 900; color: var(--text); }

/* 階段標籤設計 */
.tag-stage1{ font-size: 12px; padding: 4px 8px; border-radius: 999px; border:1px solid #3b82f6; background: rgba(59,130,246,0.2); color: #93c5fd; font-weight: bold;}
.tag-stage2{ font-size: 12px; padding: 4px 8px; border-radius: 999px; border:1px solid #f97316; background: rgba(249,115,22,0.2); color: #fdba74; font-weight: bold;}
.tag-stage3{ font-size: 12px; padding: 4px 8px; border-radius: 999px; border:1px solid #ef4444; background: rgba(239,68,68,0.2); color: #fca5a5; font-weight: bold;}
.tag-stage4{ font-size: 12px; padding: 4px 8px; border-radius: 999px; border:1px solid #a855f7; background: rgba(168,85,247,0.2); color: #d8b4fe; font-weight: bold;} /* 紫色妖股標籤 */

.stButton>button{ border-radius: 16px !important; border: 1px solid rgba(255,255,255,0.2) !important; background: linear-gradient(90deg, #1f2937, #111827) !important; color: white !important; font-weight: 900 !important; font-size: 20px !important; padding: 25px !important; transition: all 0.3s ease; box-shadow: 0 4px 15px rgba(0,0,0,0.5); }
.stButton>button:hover{ border-color: #f87171 !important; transform: translateY(-2px); box-shadow: 0 6px 20px rgba(248,113,113,0.2); }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

# =========================
# MATH & TICK HELPERS
# =========================
def now_taipei(): return datetime.utcnow() + timedelta(hours=8)
def minutes_elapsed_in_session(ts):
    start, end = datetime.combine(ts.date(), dtime(9, 0)), datetime.combine(ts.date(), dtime(13, 30))
    if ts < start: return 0
    if ts > end: return 270
    return int((ts - start).total_seconds() // 60)

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

def split_nums(s):
    if not s or s == "-": return []
    return [float(x) for x in str(s).split("_") if x and x != "-"]

# =========================
# ENGINE 1: 股票清單 (嚴格剔除權證)
# =========================
@st.cache_data(ttl=24*3600, show_spinner=False)
def get_stock_list():
    meta = {}
    urls = [("tse", "https://raw.githubusercontent.com/mlouielu/twstock/master/twstock/codes/twse_equities.csv"),
            ("otc", "https://raw.githubusercontent.com/mlouielu/twstock/master/twstock/codes/tpex_equities.csv")]
    for ex, url in urls:
        try:
            r = requests.get(url, timeout=5, verify=False)
            df = pd.read_csv(io.StringIO(r.text.replace("\r", "")), on_bad_lines="skip")
            for _, row in df.iterrows():
                c = str(row.iloc[1] if len(row)>1 else "").strip()
                if len(c) == 4 and c.isdigit(): 
                    meta[c] = {"name": str(row.iloc[2]), "ind": str(row.iloc[6]) if len(row)>6 else "", "ex": ex}
        except: pass
    if not meta: raise ValueError("無法取得股票清單，請確認網路連線。")
    return meta

# =========================
# ENGINE 2: MIS 盤中極速快篩
# =========================
def fast_mis_scan(meta_dict, status_placeholder):
    s = requests.Session()
    s.get("https://mis.twse.com.tw/stock/fibest.jsp?lang=zh_tw", headers={"User-Agent": "Mozilla/5.0"}, timeout=5, verify=False)
    codes = list(meta_dict.keys())
    rows = []
    
    total_batches = math.ceil(len(codes) / 120)
    for i in range(0, len(codes), 120):
        chunk = codes[i:i+120]
        ex_ch = "%7c".join([f"{meta_dict[c]['ex']}_{c}.tw" for c in chunk])
        url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={ex_ch}&json=1&delay=0&_={int(time.time()*1000)}"
        status_placeholder.update(label=f"📡 盤中即時快篩：MIS 連線中 ({i//120 + 1}/{total_batches})...", state="running")
        
        try:
            r = s.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5, verify=False)
            for q in r.json().get("msgArray", []):
                z, u, v = q.get("z"), q.get("u"), q.get("v")
                if z == "-" or not z or u == "-" or not u: continue
                
                last, upper, vol_lots = float(z), float(u), int(float(v or 0))
                
                dist_pct = ((upper - last) / upper) * 100
                if vol_lots >= 800 and dist_pct <= 3.0:
                    b_prices = split_nums(q.get("b"))
                    b_vols = split_nums(q.get("g"))
                    rows.append({
                        "code": q.get("c"), "last": last, "upper": upper, "dist": dist_pct, 
                        "vol_lots": vol_lots, "prev_close": float(q.get("y") if q.get("y")!="-" else 0),
                        "high": float(q.get("h") if q.get("h")!="-" else last),
                        "low": float(q.get("l") if q.get("l")!="-" else last),
                        "bid_p1": b_prices[0] if b_prices else 0,
                        "bid_v1": b_vols[0] if b_vols else 0
                    })
        except: time.sleep(0.05)
        
    return pd.DataFrame(rows)

# =========================
# ENGINE 3: 日線排雷與連板計算 (解除封印版)
# =========================
def core_filter_engine(candidates_df, meta_dict, now_ts, status_placeholder):
    if candidates_df.empty: return pd.DataFrame()
    
    syms = [f"{c}.{'TW' if meta_dict[c]['ex']=='tse' else 'TWO'}" for c in candidates_df["code"]]
    status_placeholder.update(label=f"📊 鎖定 {len(syms)} 檔候選，進行日線排雷與連板數計算...", state="running")
    
    try:
        raw_daily = yf.download(tickers=" ".join(syms), period="100d", interval="1d", group_by="ticker", auto_adjust=False, threads=False, progress=False)
    except: return pd.DataFrame()

    results = []
    frac = max(0.2, min(1.0, minutes_elapsed_in_session(now_ts) / 270.0))
    today_date = now_ts.date()

    for _, r in candidates_df.iterrows():
        c = r["code"]
        sym = f"{c}.{'TW' if meta_dict[c]['ex']=='tse' else 'TWO'}"
        
        try:
            dfD = raw_daily[sym].dropna() if isinstance(raw_daily.columns, pd.MultiIndex) else raw_daily.dropna()
            if len(dfD) < 30: continue

            hist_ret = dfD["Close"].pct_change().dropna()
            limit_pct = 0.20 if (len(hist_ret)>10 and float(hist_ret.max()) > 0.105) else 0.10

            # 過去連板數計算
            has_today = dfD.index[-1].date() == today_date
            past_df = dfD.iloc[:-1].copy() if has_today else dfD.copy()
            
            past_boards = 0
            if len(past_df) >= 10:
                past_10 = past_df.tail(10)
                for i in range(len(past_10)-1, 0, -1):
                    curr_c = float(past_10["Close"].iloc[i])
                    prev_c = float(past_10["Close"].iloc[i-1])
                    lim_u = calc_limit_up(prev_c, limit_pct)
                    
                    if curr_c >= (lim_u - tw_tick(lim_u)): 
                        past_boards += 1
                    else:
                        break
            
            # 【核心修改】：移除 past_boards >= 3 就淘汰的限制，改用紫標與微幅扣分
            stage_bonus = 0.0
            if past_boards == 0: stage_label, stage_class, stage_bonus = "🚀 第一根", "tag-stage1", 10.0
            elif past_boards == 1: stage_label, stage_class, stage_bonus = "🔥 第二根", "tag-stage2", 5.0
            elif past_boards == 2: stage_label, stage_class, stage_bonus = "⚠️ 第三根", "tag-stage3", -5.0
            else: stage_label, stage_class, stage_bonus = f"💀 第{past_boards+1}根", "tag-stage4", -15.0

            # 基礎排雷
            vol_ma20 = float(dfD["Volume"].rolling(20).mean().iloc[-1])
            yday_close = float(r["prev_close"])
            high60_ex1 = float(past_df["High"].rolling(60).max().iloc[-1]) if len(past_df)>=60 else yday_close
            
            ma20 = past_df["Close"].rolling(20).mean()
            base_len = int(((past_df["Close"] / (ma20 + 1e-9) - 1.0).abs() <= 0.04).tail(60).sum())
            
            range20_pct = float((past_df["High"].rolling(20).max().iloc[-1] - past_df["Low"].rolling(20).min().iloc[-1]) / yday_close)
            range60_pct = float((past_df["High"].rolling(60).max().iloc[-1] - past_df["Low"].rolling(60).min().iloc[-1]) / yday_close)
            
            tr = pd.concat([(past_df["High"] - past_df["Low"]).abs(), (past_df["High"] - past_df["Close"].shift(1)).abs(), (past_df["Low"] - past_df["Close"].shift(1)).abs()], axis=1).max(axis=1)
            atr20_pct = float(tr.rolling(20).mean().iloc[-1] / yday_close) * 100
            base_tight = float((1.0 - min(1.0, range20_pct / (range60_pct + 1e-9))) * 0.6 + (1.0 - min(1.0, atr20_pct / 8.0)) * 0.4)

            # MIS 盤中數據即時計算
            rng = max(1e-9, r["high"] - r["low"])
            close_pos = (r["last"] - r["low"]) / rng
            pullback = (r["high"] - r["last"]) / max(1e-9, r["high"])
            
            if pullback > 0.0038: continue 

            vol_lots_daily_avg = vol_ma20 / 1000
            vol_ratio = r["vol_lots"] / (vol_lots_daily_avg * frac + 1e-9)
            if vol_ratio < 1.3: continue

            is_locked = (r["bid_p1"] >= r["upper"] - tw_tick(r["upper"]))
            lock_bonus = 15.0 if is_locked else 0.0

            # 綜合潛力計分 (0~100)
            score = 40.0 + stage_bonus + lock_bonus
            score += 15.0 * min(1.0, max(0.0, (close_pos - 0.85) / 0.15))
            score += 10.0 * min(1.0, max(0.0, (0.0038 - pullback) / 0.0038))
            score += 15.0 * min(1.0, max(0.0, (vol_ratio - 1.5) / 2.5))
            score += 10.0 * min(1.0, max(0.0, (base_len - 8) / 40.0))
            score += 5.0 * min(1.0, max(0.0, base_tight))
            if r["upper"] >= high60_ex1 * 0.995: score += 5.0

            results.append({
                "代號": c, "名稱": meta_dict[c]["name"], "族群": meta_dict[c]["ind"],
                "現價": r["last"], "漲停價": r["upper"], "距離漲停(%)": r["dist"],
                "較昨收(%)": ((r["last"] / yday_close) - 1.0) * 100, 
                "累積量(張)": r["vol_lots"], "盤中爆量倍數": vol_ratio, 
                "鎖死狀態": "鎖死" if is_locked else "未鎖", "買一掛單": r["bid_v1"],
                "基底天數": base_len, "潛力分": max(0.0, min(100.0, score)),
                "階段標籤": stage_label, "階段Class": stage_class
            })
        except: continue

    if not results: return pd.DataFrame()
    out = pd.DataFrame(results)
    
    grp = out["族群"].value_counts()
    out["潛力分"] += out["族群"].apply(lambda x: min(15.0, max(0.0, (int(grp.get(x, 1)) - 1) * 5.0)) if x and x != "未分類" else 0.0)
    out["潛力分"] = out["潛力分"].clip(0, 100)
    
    out = out.sort_values(["潛力分", "距離漲停(%)"], ascending=[False, True]).reset_index(drop=True)
    out.index += 1
    return out

# =========================
# MAIN APP (極簡暴力)
# =========================
st.markdown('<div class="title">🧊 起漲戰情室</div>', unsafe_allow_html=True)
st.markdown('<div class="subtitle">MIS 光速盤中版 ｜ 解除封印，1~N 根連板通吃</div>', unsafe_allow_html=True)

run_scan = st.button("🚀 啟動掃描 (自動鎖定強勢先機)", use_container_width=True)

if run_scan:
    with st.status("⚡ 戰情室運算中，請稍候...", expanded=True) as status:
        
        status.update(label="📦 1/3 載入台股最新清單...", state="running")
        try:
            meta = get_stock_list()
        except Exception as e:
            status.update(label="❌ 清單載入失敗", state="error"); st.error(str(e)); st.stop()
            
        pre_df = fast_mis_scan(meta, status)
        
        if pre_df.empty:
            status.update(label="✅ 掃描完畢", state="complete")
            st.info("😴 目前盤面上沒有符合「爆量且接近漲停」的標的。")
            st.stop()
            
        final_res = core_filter_engine(pre_df, meta, now_taipei(), status)
        status.update(label="✅ 掃描與計算完成！", state="complete")

    # =========================
    # 渲染結果
    # =========================
    if final_res.empty:
        st.warning("⚠️ 盤面上的快漲停股皆被『排雷濾網』剔除 (可能回落太大或量能不足)。")
    else:
        st.success(f"🎯 完美鎖定！為您篩選出 {len(final_res)} 檔『強勢候選』標的。")
        
        cols = st.columns(min(len(final_res), 4))
        for i, r in final_res.head(16).iterrows():
            with cols[(i-1) % 4]:
                st.markdown(f"""
                <div class="card">
                    <div class="metric">
                        <div>
                            <span class="{r['階段Class']}">{r['階段標籤']}</span><br>
                            <span class="code" style="display:inline-block; margin-top:8px;">{r['代號']}</span><span class="name">{r['名稱']}</span>
                        </div>
                        <div class="price">{r['現價']:.2f}</div>
                    </div>
                    <div style="display:flex; justify-content:space-between; font-size:13px; color:#9ca3af;">
                        <span>狀態: <b style="color:{'#a3e635' if r['鎖死狀態']=='鎖死' else '#fbbf24'};">{r['鎖死狀態']}</b></span>
                        <span>爆量: <b style="color:#f87171;">{r['盤中爆量倍數']:.1f}x</b></span>
                    </div>
                    <div style="display:flex; justify-content:space-between; font-size:13px; color:#9ca3af; margin-top:6px;">
                        <span>潛力分: <b style="color:#a3e635;">{r['潛力分']:.1f}</b></span>
                        <span>買一掛單: <b>{int(r['買一掛單'])}</b></span>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
        st.markdown("<div style='height:20px;'></div>", unsafe_allow_html=True)
        csv_df = final_res.drop(columns=["階段Class"])
        csv = csv_df.to_csv(index=False).encode('utf-8-sig')
        st.download_button("📥 匯出今日戰情報表 (CSV)", data=csv, file_name=f"起漲先機_{datetime.now().strftime('%Y%m%d_%H%M')}.csv", mime="text/csv", use_container_width=True)
        
        with st.expander("📋 展開完整數據表"):
            st.dataframe(csv_df.style.format({
                "現價":"{:.2f}", "漲停價":"{:.2f}", "距離漲停(%)":"{:.2f}%", "較昨收(%)":"{:.2f}%",
                "累積量(張)":"{:,}", "盤中爆量倍數":"{:.2f}x", "潛力分":"{:.1f}"
            }), use_container_width=True)
