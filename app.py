# app.py — 起漲戰情室｜戰神旗艦 3.7.1｜新增：測試模式開關
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

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================
# UI / THEME
# =========================
st.set_page_config(page_title="起漲戰情室｜測試優化版", page_icon="⚡", layout="wide", initial_sidebar_state="collapsed")

CSS = """
<style>
:root{ --bg:#07080b; --card:#0f1116; --text:#e5e7eb; --muted:#9ca3af; --line:rgba(148,163,184,.16); --shadow: 0 16px 40px rgba(0,0,0,.35); }
[data-testid="stAppViewContainer"], .main{ background: var(--bg) !important; color: var(--text) !important; }
.block-container{ padding-top: 2rem; max-width: 1200px; }
[data-testid="stSidebar"] { display: none !important; }
.title{ font-size: 46px; font-weight: 900; background: linear-gradient(90deg, #f3f4f6, #9ca3af); -webkit-background-clip:text; -webkit-text-fill-color: transparent; text-align: center; margin:0;}
.card{ background: linear-gradient(180deg, rgba(15,17,22,.94), rgba(11,13,18,.94)); border:1px solid var(--line); border-radius: 16px; padding: 18px; margin-bottom: 12px; }
.metric .code{ color: var(--text); font-size: 20px; font-weight: 900; }
.metric .price{ font-size: 26px; font-weight: 900; color: var(--text); }
.tag-stage1{ font-size: 11px; padding: 3px 7px; border-radius: 999px; border:1px solid #3b82f6; background: rgba(59,130,246,0.2); color: #93c5fd; font-weight: bold;}
.stButton>button{ border-radius: 16px !important; background: linear-gradient(90deg, #1f2937, #111827) !important; color: white !important; font-weight: 900 !important; font-size: 20px !important; padding: 25px !important; width: 100%; transition: all 0.3s ease; }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

# =========================
# HELPERS
# =========================
def now_taipei(): return datetime.utcnow() + timedelta(hours=8)

def get_vol_frac_and_dist(ts, is_test):
    m = int((datetime.combine(ts.date(), ts.time()) - datetime.combine(ts.date(), dtime(9, 0))).total_seconds() // 60)
    m = max(0, min(270, m)) 
    
    if is_test:
        return 0.5, 5.0 # 測試模式：超寬門檻
        
    if m <= 60: dist_lim = 3.1     
    elif m <= 180: dist_lim = 2.2  
    else: dist_lim = 1.5           
    
    if m <= 30: frac = 0.12
    elif m <= 120: frac = 0.12 + (0.5 - 0.12) * ((m - 30) / 90.0)
    else: frac = min(1.0, 0.5 + (1.0 - 0.5) * ((m - 120) / 150.0))
    return frac, dist_lim

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
    n = math.floor((raw + 1e-12) / tick)
    price = n * tick
    return round(price, 2 if tick < 0.1 else 1 if tick < 1 else 0)

def split_nums(s):
    out = []
    for x in str(s or "").split("_"):
        try:
            if x and x not in ("-", "—", ""): out.append(float(x))
        except: pass
    return out

# =========================
# ENGINE 1: 股票清單
# =========================
@st.cache_data(ttl=24*3600, show_spinner=False)
def get_stock_list():
    meta = {}
    urls = [("tse", "https://raw.githubusercontent.com/mlouielu/twstock/master/twstock/codes/twse_equities.csv"),
            ("otc", "https://raw.githubusercontent.com/mlouielu/twstock/master/twstock/codes/tpex_equities.csv")]
    for ex, url in urls:
        try:
            r = requests.get(url, timeout=15, verify=False)
            df = pd.read_csv(io.StringIO(r.text.replace("\r", "")), dtype=str, engine="python", on_bad_lines="skip")
            col_map = {c.strip().lower(): c for c in df.columns}
            c_col = col_map.get('code')  or df.columns[1]
            n_col = col_map.get('name')  or df.columns[2]
            g_col = col_map.get('group') or (df.columns[6] if len(df.columns)>6 else None)
            t_col = col_map.get('type')  or df.columns[0]
            for _, row in df.iterrows():
                code = str(row[c_col]).strip()
                if len(code) == 4 and code.isdigit():
                    stype = str(row[t_col]) if t_col else ""
                    if "權證" in stype or "ETF" in stype: continue
                    ind = str(row[g_col]).strip() if g_col and pd.notna(row[g_col]) else ""
                    if not ind or ind.lower() in ("nan", "none") or ind in ("-", "—"): ind = "未分類"
                    meta[code] = {"name": str(row[n_col]) if n_col else "未知", "ind": ind, "ex": ex}
        except: pass
    return meta

# =========================
# ENGINE 2: MIS 盤中快篩
# =========================
def fast_mis_scan(meta_dict, status_placeholder, now_ts, is_test):
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://mis.twse.com.tw/stock/fibest.jsp?lang=zh_tw"}
    s = requests.Session()
    try: s.get("https://mis.twse.com.tw/stock/fibest.jsp?lang=zh_tw", headers=headers, timeout=15, verify=False)
    except: pass
    
    _, dist_limit = get_vol_frac_and_dist(now_ts, is_test)
    vol_limit = 200 if is_test else 800
    
    codes = list(meta_dict.keys())
    rows, err_mis = [], 0
    batch_size = 80 
    for i in range(0, len(codes), batch_size):
        chunk = codes[i:i+batch_size]
        ex_ch = "%7c".join([f"{meta_dict[c]['ex']}_{c}.tw" for c in chunk])
        url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={ex_ch}&json=1&delay=0&_={int(time.time()*1000)}"
        status_placeholder.update(label=f"📡 快篩中 ({i//batch_size + 1}/{math.ceil(len(codes)/batch_size)})...", state="running")
        try:
            r = s.get(url, headers=headers, timeout=12, verify=False)
            data = r.json().get("msgArray", [])
            for q in data:
                c = q.get("c")
                if not c or c not in meta_dict: continue
                z, u, v, y = q.get("z"), q.get("u"), q.get("v"), q.get("y")
                if not z or z == "-" or not u or u == "-" or not y or y == "-" or float(y) == 0 or float(u) <= 0: continue
                last, upper, prev_close = float(z), float(u), float(y)
                vol_sh = float(v or 0)
                dist_pct = max(0.0, ((upper - last) / upper) * 100)
                if (vol_sh / 1000) >= vol_limit and dist_pct <= dist_limit:
                    bp = split_nums(q.get("b")); bv = split_nums(q.get("g"))
                    ap = split_nums(q.get("a")); av = split_nums(q.get("f"))
                    rows.append({"code": c, "last": last, "upper": upper, "dist": dist_pct, "vol_sh": vol_sh, "prev_close": prev_close,
                                 "high": float(q.get("h") if q.get("h")!="-" else last), "low": float(q.get("l") if q.get("l")!="-" else last),
                                 "best_bid": bp[0] if bp else 0, "bid_sh1": bv[0] if bv else 0, "best_ask": ap[0] if ap else 0, "ask_sh1": av[0] if av else 0})
        except: err_mis += 1
    return pd.DataFrame(rows), err_mis

# =========================
# ENGINE 3: 核心濾網
# =========================
def core_filter_engine(candidates_df, meta_dict, now_ts, status_placeholder, mis_err, is_test):
    if candidates_df.empty: return pd.DataFrame(), {}
    
    candidates_df = candidates_df.sort_values(["dist", "vol_sh"], ascending=[True, False]).head(80)
    stats = {"Total": len(candidates_df), "NotLocked": 0, "Hype": 0, "Pullback": 0, "VolRatio": 0, "WeakClose": 0, "Err": 0, "YF_Fail": 0, "MIS_Err": mis_err}
    syms = [f"{c}.{'TW' if meta_dict[c]['ex']=='tse' else 'TWO'}" for c in candidates_df["code"]]
    
    try:
        raw_daily = yf.download(tickers=" ".join(syms), period="100d", interval="1d", group_by="ticker", auto_adjust=False, threads=False, progress=False)
    except: return pd.DataFrame(), stats

    results, today_date = [], now_ts.date()
    frac, _ = get_vol_frac_and_dist(now_ts, is_test)
    vol_ratio_limit = 0.5 if is_test else 1.3
    pullback_limit = 0.05 if is_test else 0.0039
    close_pos_limit = 0.50 if is_test else 0.80

    for _, r in candidates_df.iterrows():
        c = r["code"]
        sym = f"{c}.{'TW' if meta_dict[c]['ex']=='tse' else 'TWO'}"
        try:
            dfD = raw_daily[sym].dropna() if isinstance(raw_daily.columns, pd.MultiIndex) else raw_daily.dropna()
            if len(dfD) < 30: stats["YF_Fail"] += 1; continue
            has_today = dfD.index[-1].date() == today_date
            past_df = dfD.iloc[:-1].copy() if has_today else dfD.copy()
            vol_ma20_sh = float(past_df["Volume"].rolling(20).mean().iloc[-1])
            if not (vol_ma20_sh > 0): stats["YF_Fail"] += 1; continue
            
            # 逼近法計算連板
            past_boards = 0
            if len(past_df) >= 10:
                past_10 = past_df.tail(10)
                for i in range(len(past_10)-1, 0, -1):
                    cp, pp, hp = float(past_10["Close"].iloc[i]), float(past_10["Close"].iloc[i-1]), float(past_10["High"].iloc[i])
                    l10, l20 = calc_limit_up(pp, 0.10), calc_limit_up(pp, 0.20)
                    d10, d20 = min(abs(cp - l10), abs(hp - l10)), min(abs(cp - l20), abs(hp - l20))
                    tol20 = max(2 * tw_tick(l20), l20 * 0.001)
                    daily_lim = l20 if (d20 < d10 and d20 <= tol20) else l10
                    if cp >= (daily_lim - tw_tick(daily_lim)): past_boards += 1
                    else: break
            
            # Hype (測試模式下跳過)
            if not is_test and past_boards == 0:
                had_limit_past = False
                for j in range(len(past_df)-1, max(1, len(past_df)-10), -1):
                    cp_j, pp_j, hp_j = float(past_df["Close"].iloc[j]), float(past_df["Close"].iloc[j-1]), float(past_df["High"].iloc[j])
                    l10_j, l20_j = calc_limit_up(pp_j, 0.10), calc_limit_up(pp_j, 0.20)
                    lim_j = l20_j if (min(abs(cp_j-l20_j), abs(hp_j-l20_j)) < min(abs(cp_j-l10_j), abs(hp_j-l10_j))) else l10_j
                    if cp_j >= (lim_j - tw_tick(lim_j)): had_limit_past = True; break
                if had_limit_past: stats["Hype"] += 1; continue

            # 鎖死判定
            is_locked = (r["best_bid"] >= r["upper"] - tw_tick(r["upper"])) and (r["bid_sh1"] >= 50000)
            if not is_locked: stats["NotLocked"] += 1

            vol_ratio = r["vol_sh"] / (vol_ma20_sh * frac + 1e-9)
            if vol_ratio < vol_ratio_limit: stats["VolRatio"] += 1; continue

            rng_raw = r["high"] - r["low"]
            close_pos = 1.0 if rng_raw <= 2 * tw_tick(r["upper"]) else (r["last"] - r["low"]) / rng_raw
            pullback = (r["high"] - r["last"]) / max(1e-9, r["high"])
            
            if pullback > pullback_limit: stats["Pullback"] += 1; continue
            if close_pos < close_pos_limit: stats["WeakClose"] += 1; continue

            results.append({"代號": c, "名稱": meta_dict[c]["name"], "現價": r["last"], "距離(%)": r["dist"], "累積量": int(r["vol_sh"]/1000), "爆量x": vol_ratio, "狀態": "鎖死" if is_locked else "未鎖", "潛力分": 50.0 + (10 if is_locked else 0), "階段": f"第{past_boards+1}連", "Class": "tag-stage1"})
        except: stats["Err"] += 1
    return pd.DataFrame(results), stats

# =========================
# MAIN APP
# =========================
st.markdown('<div class="title">🧊 起漲戰情室</div>', unsafe_allow_html=True)
is_test = st.checkbox("🔥 **測試模式 (放寬門檻，半夜也能抓標的)**", value=False)

run_scan = st.button("🚀 啟動掃描 (自動鎖定強勢先機)", use_container_width=True)

if run_scan:
    now_ts = now_taipei()
    with st.status("⚡ 狙擊中...", expanded=True) as status:
        meta = get_stock_list()
        pre_df, mis_err = fast_mis_scan(meta, status, now_ts, is_test)
        final_res, stats = core_filter_engine(pre_df, meta, now_ts, status, mis_err, is_test)
        status.update(label="✅ 計算完成！", state="complete")

    st.expander("📊 掃描統計戰報", expanded=True).json(stats)

    if final_res.empty:
        st.warning("⚠️ 門檻太硬，全軍覆沒。請開啟『測試模式』確認邏輯。")
    else:
        st.success(f"🎯 鎖定 {len(final_res)} 檔標的。")
        cols = st.columns(4)
        for i, r in final_res.head(16).iterrows():
            with cols[i % 4]:
                st.markdown(f"""<div class="card"><div class="metric"><div><span class="tag-stage1">{r['階段']}</span><br><span class="code">{r['代號']}</span> {r['名稱']}</div><div class="price">{r['現價']}</div></div>
                    <div style="font-size:13px; color:#9ca3af;">狀態: {r['狀態']} | 爆量: {r['爆量x']:.1f}x</div></div>""", unsafe_allow_html=True)
