# app.py — 起漲戰情室｜實戰狙擊手版 (1~N根)｜MIS 80 批次穩健版｜全方位 Bug 修正
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
st.set_page_config(page_title="起漲戰情室｜實戰狙擊版", page_icon="⚡", layout="wide", initial_sidebar_state="collapsed")

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
.tag-stage1{ font-size: 12px; padding: 4px 8px; border-radius: 999px; border:1px solid #3b82f6; background: rgba(59,130,246,0.2); color: #93c5fd; font-weight: bold;}
.tag-stage2{ font-size: 12px; padding: 4px 8px; border-radius: 999px; border:1px solid #f97316; background: rgba(249,115,22,0.2); color: #fdba74; font-weight: bold;}
.tag-stage3{ font-size: 12px; padding: 4px 8px; border-radius: 999px; border:1px solid #ef4444; background: rgba(239,68,68,0.2); color: #fca5a5; font-weight: bold;}
.tag-stage4{ font-size: 12px; padding: 4px 8px; border-radius: 999px; border:1px solid #a855f7; background: rgba(168,85,247,0.2); color: #d8b4fe; font-weight: bold;}
.stButton>button{ border-radius: 16px !important; border: 1px solid rgba(255,255,255,0.2) !important; background: linear-gradient(90deg, #1f2937, #111827) !important; color: white !important; font-weight: 900 !important; font-size: 20px !important; padding: 25px !important; transition: all 0.3s ease; box-shadow: 0 4px 15px rgba(0,0,0,0.5); }
.stButton>button:hover{ border-color: #f87171 !important; transform: translateY(-2px); box-shadow: 0 6px 20px rgba(248,113,113,0.2); }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

# =========================
# HELPERS
# =========================
def now_taipei(): return datetime.utcnow() + timedelta(hours=8)

# 【核心修正 7】: 分段式時間比例函數
def get_vol_frac(ts):
    m = int((datetime.combine(ts.date(), ts.time()) - datetime.combine(ts.date(), dtime(9, 0))).total_seconds() // 60)
    if m <= 30: return 0.12 # 9:30前
    elif m <= 120: return 0.12 + (0.5 - 0.12) * ((m - 30) / 90.0) # 9:30~11:00
    elif m <= 270: return min(1.0, 0.5 + (1.0 - 0.5) * ((m - 120) / 150.0)) # 11:00後
    return 1.0

def tw_tick(price):
    if price < 10: return 0.01
    if price < 50: return 0.05
    if price < 100: return 0.10
    if price < 500: return 0.50
    if price < 1000: return 1.00
    return 5.00

def calc_limit_up(prev_close, limit_pct=0.10):
    raw = prev_close * (1.0 + limit_pct)
    tick = tw_tick(raw)
    return round(round(raw / tick) * tick, 2 if tick < 0.1 else 1 if tick < 1 else 0)

# 【核心修正 2】: 強健型數字切割
def split_nums(s):
    out = []
    for x in str(s or "").split("_"):
        try:
            if x and x not in ("-", "—", ""): out.append(float(x))
        except: pass
    return out

# =========================
# ENGINE 1: 股票清單 (修復欄位偏移與權證過濾)
# =========================
@st.cache_data(ttl=24*3600, show_spinner=False)
def get_stock_list():
    meta = {}
    urls = [("tse", "https://raw.githubusercontent.com/mlouielu/twstock/master/twstock/codes/twse_equities.csv"),
            ("otc", "https://raw.githubusercontent.com/mlouielu/twstock/master/twstock/codes/tpex_equities.csv")]
    for ex, url in urls:
        try:
            r = requests.get(url, timeout=15, verify=False)
            df = pd.read_csv(io.StringIO(r.text.replace("\r", "")), dtype=str)
            # 【核心修正 1 & 3】: 自動識別欄名，且嚴格過濾「股票」類型
            col_map = {c.strip(): c for c in df.columns}
            c_col = col_map.get('code') or df.columns[1]
            n_col = col_map.get('name') or df.columns[2]
            g_col = col_map.get('group') or df.columns[6]
            t_col = col_map.get('type') or df.columns[0]

            for _, row in df.iterrows():
                code = str(row[c_col]).strip()
                # 嚴格 4 碼數字 + 排除權證/ETF字眼
                if len(code) == 4 and code.isdigit():
                    stype = str(row[t_col])
                    if "權證" in stype or "ETF" in stype: continue
                    meta[code] = {"name": str(row[n_col]), "ind": str(row[g_col]) if len(row)>6 else "未分類", "ex": ex}
        except: pass
    if not meta: raise ValueError("無法取得股票清單。")
    return meta

# =========================
# ENGINE 2: MIS 盤中極速快篩 (修正單位、Batch、檢查碼)
# =========================
def fast_mis_scan(meta_dict, status_placeholder):
    s = requests.Session()
    try: s.get("https://mis.twse.com.tw/stock/fibest.jsp?lang=zh_tw", timeout=15, verify=False)
    except: pass
    
    codes = list(meta_dict.keys())
    rows = []
    
    # 【核心修正 2】: Batch 改為 80 提升穩定度
    batch_size = 80
    total_batches = math.ceil(len(codes) / batch_size)
    
    for i in range(0, len(codes), batch_size):
        chunk = codes[i:i+batch_size]
        ex_ch = "%7c".join([f"{meta_dict[c]['ex']}_{c}.tw" for c in chunk])
        url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={ex_ch}&json=1&delay=0&_={int(time.time()*1000)}"
        status_placeholder.update(label=f"📡 盤中快篩中 ({i//batch_size + 1}/{total_batches})...", state="running")
        
        try:
            r = s.get(url, timeout=12, verify=False)
            data = r.json().get("msgArray", [])
            for q in data:
                c = q.get("c")
                # 【核心修正 7】: 安全檢查 Code
                if not c or c not in meta_dict: continue
                
                z, u, v, y = q.get("z"), q.get("u"), q.get("v"), q.get("y")
                # 【核心修正 4】: y (prev_close) 為空則跳過
                if not z or z == "-" or not u or u == "-" or not y or y == "-" or float(y) == 0: continue
                
                last, upper, prev_close = float(z), float(u), float(y)
                
                # 【核心修正 1】: MIS v 是股數，轉成張數
                vol_lots = int(float(v or 0) / 1000)
                dist_pct = ((upper - last) / upper) * 100
                
                if vol_lots >= 800 and dist_pct <= 3.1:
                    # 【核心修正 6】: 抓取買五賣五資料 (b:買價, g:買量, a:賣價, f:賣量)
                    bp, bv, ap, av = split_nums(q.get("b")), split_nums(q.get("g")), split_nums(q.get("a")), split_nums(q.get("f"))
                    rows.append({
                        "code": c, "last": last, "upper": upper, "dist": dist_pct, 
                        "vol_lots": vol_lots, "prev_close": prev_close,
                        "high": float(q.get("h") if q.get("h")!="-" else last),
                        "low": float(q.get("l") if q.get("l")!="-" else last),
                        "bid_p1": bp[0] if bp else 0, "bid_v1": bv[0] if bv else 0,
                        "ask_p1": ap[0] if ap else 0, "ask_v1": av[0] if av else 0
                    })
        except: pass
        time.sleep(0.05)
    return pd.DataFrame(rows)

# =========================
# ENGINE 3: 核心濾網 (含統計與連板修正)
# =========================
def core_filter_engine(candidates_df, meta_dict, now_ts, status_placeholder):
    if candidates_df.empty: return pd.DataFrame()
    
    # 【核心修正 8】: 建立儀表板，追蹤淘汰原因
    stats = {"Total": len(candidates_df), "YF_Fail": 0, "Hype": 0, "Pullback": 0, "VolRatio": 0, "LockFail": 0}
    
    syms = [f"{c}.{'TW' if meta_dict[c]['ex']=='tse' else 'TWO'}" for c in candidates_df["code"]]
    status_placeholder.update(label=f"📊 正在運算 {len(syms)} 檔候選股...", state="running")
    
    try:
        raw_daily = yf.download(tickers=" ".join(syms), period="100d", interval="1d", group_by="ticker", auto_adjust=False, threads=False, progress=False)
    except: return pd.DataFrame()

    results = []
    frac = get_vol_frac(now_ts)
    today_date = now_ts.date()

    for _, r in candidates_df.iterrows():
        c = r["code"]
        sym = f"{c}.{'TW' if meta_dict[c]['ex']=='tse' else 'TWO'}"
        try:
            dfD = raw_daily[sym].dropna() if isinstance(raw_daily.columns, pd.MultiIndex) else raw_daily.dropna()
            if len(dfD) < 30: 
                stats["YF_Fail"] += 1; continue

            # 連板計算修正
            hist_ret = dfD["Close"].pct_change().dropna()
            has_today = dfD.index[-1].date() == today_date
            past_df = dfD.iloc[:-1].copy() if has_today else dfD.copy()
            
            # 【核心修正 4】: 用昨日收盤推算今日漲停價，解決 10/20% 失真
            curr_limit_pct = 0.20 if (r["upper"] / r["prev_close"] > 1.11) else 0.10
            
            past_boards = 0
            if len(past_df) >= 10:
                past_10 = past_df.tail(10)
                for i in range(len(past_10)-1, 0, -1):
                    c_p, p_p = float(past_10["Close"].iloc[i]), float(past_10["Close"].iloc[i-1])
                    # 猜測當時的制度 (比單純猜 10.5% 穩)
                    daily_lim = calc_limit_up(p_p, 0.20) if (c_p/p_p > 1.11) else calc_limit_up(p_p, 0.10)
                    if c_p >= (daily_lim - tw_tick(daily_lim)): past_boards += 1
                    else: break
            
            # 【核心修正 5】: 更名為連續連板
            if past_boards == 0: stage_label, stage_class, stage_bonus = "🚀 第一根", "tag-stage1", 10.0
            elif past_boards == 1: stage_label, stage_class, stage_bonus = "🔥 第二連", "tag-stage2", 5.0
            elif past_boards == 2: stage_label, stage_class, stage_bonus = "⚠️ 第三連", "tag-stage3", -5.0
            else: stage_label, stage_class, stage_bonus = f"💀 第{past_boards+1}連", "tag-stage4", -15.0

            # 排雷：近10日大漲排除
            max_ret_10d = float(hist_ret.tail(10).max()) * 100.0
            if max_ret_10d >= (19.5 if curr_limit_pct == 0.20 else 9.6):
                stats["Hype"] += 1; continue

            # 鎖死品質 (MIS 即時)
            # 【核心修正 6】: 加入賣一量判定 (f 是賣量股數)
            bid_lots1 = int(r["bid_v1"]/1000)
            ask_lots1 = int(r["ask_v1"]/1000)
            is_locked = (r["bid_p1"] >= r["upper"] - tw_tick(r["upper"])) and (bid_lots1 >= 200)
            # 假鎖排除：賣一量太大則扣分或視為未鎖
            if is_locked and ask_lots1 > 150: is_locked = False 
            
            # 爆量倍數 (修正 frac)
            vol_ma20_lots = float(dfD["Volume"].rolling(20).mean().iloc[-1]) / 1000
            vol_ratio = r["vol_lots"] / (vol_ma20_lots * frac + 1e-9)
            if vol_ratio < 1.3: 
                stats["VolRatio"] += 1; continue

            # 回落幅度
            pullback = (r["high"] - r["last"]) / max(1e-9, r["high"])
            if pullback > 0.0039: 
                stats["Pullback"] += 1; continue

            # 綜合計分
            rng = max(1e-9, r["high"] - r["low"])
            close_pos = (r["last"] - r["low"]) / rng
            score = 40.0 + stage_bonus + (15.0 if is_locked else 0.0)
            score += 15.0 * min(1.0, max(0.0, (close_pos - 0.85) / 0.15))
            score += 15.0 * min(1.0, max(0.0, (vol_ratio - 1.5) / 2.5))
            
            results.append({
                "代號": c, "名稱": meta_dict[c]["name"], "族群": meta_dict[c]["ind"],
                "現價": r["last"], "距離(%)": r["dist"], "較昨收(%)": ((r["last"]/r["prev_close"])-1)*100, 
                "累積量": r["vol_lots"], "爆量x": vol_ratio, "狀態": "鎖死" if is_locked else "未鎖", 
                "買一": bid_lots1, "賣一": ask_lots1, "潛力分": max(0.0, min(100.0, score)),
                "階段": stage_label, "Class": stage_class
            })
        except: pass

    st.sidebar.write("📊 **掃描統計戰報**")
    st.sidebar.json(stats)
    return pd.DataFrame(results).sort_values("潛力分", ascending=False).reset_index(drop=True) if results else pd.DataFrame()

# =========================
# MAIN APP
# =========================
st.markdown('<div class="title">🧊 起漲戰情室</div>', unsafe_allow_html=True)
st.markdown('<div class="subtitle">100% 實戰優化版 ｜ 自動排除權證 ｜ MIS+YFinance 雙引擎</div>', unsafe_allow_html=True)

run_scan = st.button("🚀 啟動掃描 (自動鎖定起漲先機)", use_container_width=True)

if run_scan:
    with st.status("⚡ 戰鬥中...", expanded=True) as status:
        try:
            meta = get_stock_list()
            pre_df = fast_mis_scan(meta, status)
            if pre_df.empty:
                status.update(label="✅ 掃描完畢", state="complete"); st.info("😴 目前沒標的。"); st.stop()
            final_res = core_filter_engine(pre_df, meta, now_taipei(), status)
            status.update(label="✅ 計算完成！", state="complete")
        except Exception as e:
            st.error(f"系統崩潰：{e}"); st.stop()

    if final_res.empty:
        st.warning("⚠️ 標的皆被濾網剔除，請見側邊欄統計原因。")
    else:
        st.success(f"🎯 鎖定 {len(final_res)} 檔強勢股。")
        cols = st.columns(min(len(final_res), 4))
        for i, r in final_res.head(16).iterrows():
            with cols[i % 4]:
                st.markdown(f"""
                <div class="card">
                    <div class="metric">
                        <div><span class="{r['Class']}">{r['階段']}</span><br><span class="code" style="display:inline-block; margin-top:8px;">{r['代號']}</span><span class="name">{r['名稱']}</span></div>
                        <div class="price">{r['現價']:.2f}</div>
                    </div>
                    <div style="display:flex; justify-content:space-between; font-size:13px; color:#9ca3af;">
                        <span>狀態: <b style="color:{'#a3e635' if r['狀態']=='鎖死' else '#fbbf24'};">{r['狀態']}</b></span>
                        <span>爆量: <b style="color:#f87171;">{r['爆量x']:.1f}x</b></span>
                    </div>
                    <div style="display:flex; justify-content:space-between; font-size:13px; color:#9ca3af; margin-top:6px;">
                        <span>潛力分: <b style="color:#a3e635;">{r['潛力分']:.1f}</b></span>
                        <span>買一/賣一: <b>{r['買一']}/{r['賣一']}</b></span>
                    </div>
                </div>
                """, unsafe_allow_html=True)
        
        with st.expander("📋 數據總表"):
            st.dataframe(final_res.drop(columns=["Class"]).style.format({"現價":"{:.2f}","距離(%)":"{:.2f}%","較昨收(%)":"{:.2f}%","爆量x":"{:.1f}x","潛力分":"{:.1f}"}), use_container_width=True)
