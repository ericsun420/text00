# app.py — 起漲戰情室｜戰神 4.0｜Apple Pro 極簡美學｜1~N 根連板通吃
import io
import math
import time
from datetime import datetime, timedelta, time as dtime

import requests
import urllib3
import pandas as pd
import yfinance as yf
import streamlit as st

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================
# UI / THEME (Apple Style)
# =========================
st.set_page_config(page_title="WarRoom Pro", page_icon="⚡", layout="wide", initial_sidebar_state="collapsed")

# 注入 Apple Pro 視覺語法
st.markdown("""
<style>
    /* 全局背景：深邃黑色漸層 */
    [data-testid="stAppViewContainer"] {
        background: radial-gradient(circle at top right, #1c1c1e, #000000) !important;
        color: #f5f5f7 !important;
    }
    .block-container { padding-top: 2rem; max-width: 1200px; }
    [data-testid="stSidebar"] { display: none !important; }

    /* 標題與字體 */
    .title { font-size: 52px; font-weight: 800; letter-spacing: -1.5px; background: linear-gradient(180deg, #ffffff 0%, #a1a1a6 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; text-align: center; margin-bottom: 5px; }
    .subtitle { color: #86868b; font-size: 18px; text-align: center; margin-bottom: 40px; font-weight: 400; }

    /* Apple Pro 卡片：毛玻璃效果 */
    .pro-card {
        background: rgba(28, 28, 30, 0.7);
        backdrop-filter: blur(20px);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 24px;
        padding: 24px;
        margin-bottom: 16px;
        transition: transform 0.3s ease;
    }
    .pro-card:hover { transform: translateY(-5px); border: 1px solid rgba(0, 122, 255, 0.5); }

    /* 指標樣式 */
    .code-label { font-size: 14px; color: #0a84ff; font-weight: 600; text-transform: uppercase; margin-bottom: 4px; }
    .stock-name { font-size: 22px; font-weight: 700; color: #ffffff; }
    .price-large { font-size: 32px; font-weight: 700; color: #ffffff; font-variant-numeric: tabular-nums; }
    
    /* 階段標籤 */
    .tag-pro { padding: 4px 12px; border-radius: 8px; font-size: 12px; font-weight: 700; display: inline-block; margin-bottom: 12px; }
    .tag-rise { background: rgba(0, 122, 255, 0.15); color: #0a84ff; border: 1px solid rgba(0, 122, 255, 0.3); }

    /* 淘汰名單 Tags */
    .fail-tag {
        display: inline-block;
        padding: 4px 10px;
        background: rgba(255, 69, 58, 0.1);
        color: #ff453a;
        border-radius: 6px;
        margin: 3px;
        font-size: 12px;
        border: 1px solid rgba(255, 69, 58, 0.2);
    }

    /* 按鈕優化 */
    .stButton>button {
        border-radius: 14px !important;
        background: #ffffff !important;
        color: #000000 !important;
        font-weight: 700 !important;
        font-size: 18px !important;
        padding: 18px !important;
        border: none !important;
        width: 100% !important;
        box-shadow: 0 4px 20px rgba(255,255,255,0.15);
    }
</style>
""", unsafe_allow_html=True)

# =========================
# CORE LOGIC (精準算法繼承)
# =========================
def now_taipei(): return datetime.utcnow() + timedelta(hours=8)

def get_vol_frac_and_dist(ts, is_test):
    m = int((datetime.combine(ts.date(), ts.time()) - datetime.combine(ts.date(), dtime(9, 0))).total_seconds() // 60)
    m = max(0, min(270, m)) 
    if is_test: return 0.5, 5.0
    dist_lim = 3.1 if m <= 60 else 2.2 if m <= 180 else 1.5           
    frac = 0.12 if m <= 30 else 0.12 + (0.5 - 0.12) * ((m - 30) / 90.0) if m <= 120 else min(1.0, 0.5 + (1.0 - 0.5) * ((m - 120) / 150.0))
    return frac, dist_lim

def tw_tick(price):
    return 0.01 if price<10 else 0.05 if price<50 else 0.1 if price<100 else 0.5 if price<500 else 1.0 if price<1000 else 5.0

def calc_limit_up(prev_close, limit_pct=0.10):
    raw = prev_close * (1.0 + limit_pct)
    tick = tw_tick(raw)
    n = math.floor((raw + 1e-12) / tick)
    price = n * tick
    return round(price, 2 if tick < 0.1 else 1 if tick < 1 else 0)

def split_nums(s):
    return [float(x) for x in str(s or "").split("_") if x and x not in ("-", "—", "")]

# =========================
# ENGINE (已加固)
# =========================
@st.cache_data(ttl=3600, show_spinner=False)
def get_stock_list():
    meta = {}
    urls = [("tse", "https://raw.githubusercontent.com/mlouielu/twstock/master/twstock/codes/twse_equities.csv"),
            ("otc", "https://raw.githubusercontent.com/mlouielu/twstock/master/twstock/codes/tpex_equities.csv")]
    for ex, url in urls:
        try:
            r = requests.get(url, timeout=15, verify=False)
            df = pd.read_csv(io.StringIO(r.text.replace("\r", "")), dtype=str, engine="python", on_bad_lines="skip")
            col_map = {c.strip().lower(): c for c in df.columns}
            c_col, n_col, g_col, t_col = col_map.get('code') or df.columns[1], col_map.get('name') or df.columns[2], col_map.get('group') or (df.columns[6] if len(df.columns)>6 else None), col_map.get('type') or df.columns[0]
            for _, row in df.iterrows():
                code = str(row[c_col]).strip()
                if len(code) == 4 and code.isdigit():
                    if t_col and ("權證" in str(row[t_col]) or "ETF" in str(row[t_col])): continue
                    ind = str(row[g_col]).strip() if g_col and pd.notna(row[g_col]) else "未分類"
                    meta[code] = {"name": str(row[n_col]), "ind": ind, "ex": ex}
        except: pass
    return meta

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
        status_placeholder.update(label=f"📡 正在快篩全市場標的...", state="running")
        try:
            r = s.get(url, headers=headers, timeout=12, verify=False)
            for q in r.json().get("msgArray", []):
                c, z, u, v, y = q.get("c"), q.get("z"), q.get("u"), q.get("v"), q.get("y")
                if not c or c not in meta_dict or not z or z == "-" or not u or u == "-" or not y or y == "-" or float(y) == 0: continue
                last, upper, prev_close, vol_sh = float(z), float(u), float(y), float(v or 0)
                dist_pct = max(0.0, ((upper - last) / upper) * 100)
                if (vol_sh / 1000) >= vol_limit and dist_pct <= dist_limit:
                    bp, bv, ap, av = split_nums(q.get("b")), split_nums(q.get("g")), split_nums(q.get("a")), split_nums(q.get("f"))
                    rows.append({"code": c, "last": last, "upper": upper, "dist": dist_pct, "vol_sh": vol_sh, "prev_close": prev_close,
                                 "high": float(q.get("h") if q.get("h")!="-" else last), "low": float(q.get("l") if q.get("l")!="-" else last),
                                 "best_bid": bp[0] if bp else 0, "bid_sh1": bv[0] if bv else 0, "best_ask": ap[0] if ap else 0, "ask_sh1": av[0] if av else 0})
        except: err_mis += 1
    return pd.DataFrame(rows), err_mis

def core_filter_engine(candidates_df, meta_dict, now_ts, status_placeholder, mis_err, is_test):
    stats = {
        "Total": 0, "爆量不足": [], "回落過大": [], 
        "收盤太弱": [], "過熱排除": [], "未鎖死": [],
        "YF連線失敗": 0, "MIS_Err": mis_err
    }
    if candidates_df.empty: return pd.DataFrame(), stats
    
    candidates_df = candidates_df.sort_values(["dist", "vol_sh"], ascending=[True, False]).head(80)
    stats["Total"] = len(candidates_df)
    
    syms = [f"{c}.{'TW' if meta_dict[c]['ex']=='tse' else 'TWO'}" for c in candidates_df["code"]]
    try:
        raw_daily = yf.download(tickers=" ".join(syms), period="100d", interval="1d", group_by="ticker", auto_adjust=False, threads=False, progress=False)
    except: return pd.DataFrame(), stats

    results, today_date = [], now_ts.date()
    frac, _ = get_vol_frac_and_dist(now_ts, is_test)
    v_ratio_lim, pb_lim, cp_lim = (0.5, 0.05, 0.50) if is_test else (1.3, 0.0039, 0.80)

    for _, r in candidates_df.iterrows():
        c, name = r["code"], meta_dict[r["code"]]["name"]
        label = f"{c} {name}"
        sym = f"{c}.{'TW' if meta_dict[c]['ex']=='tse' else 'TWO'}"
        try:
            dfD = raw_daily[sym].dropna() if isinstance(raw_daily.columns, pd.MultiIndex) else raw_daily.dropna()
            if len(dfD) < 30: stats["YF連線失敗"] += 1; continue
            has_today = dfD.index[-1].date() == today_date
            past_df = dfD.iloc[:-1].copy() if has_today else dfD.copy()
            vol_ma20_sh = float(past_df["Volume"].rolling(20).mean().iloc[-1])
            
            past_boards = 0
            past_10 = past_df.tail(10)
            for i in range(len(past_10)-1, 0, -1):
                cp, pp, hp = float(past_10["Close"].iloc[i]), float(past_10["Close"].iloc[i-1]), float(past_10["High"].iloc[i])
                l10, l20 = calc_limit_up(pp, 0.10), calc_limit_up(pp, 0.20)
                daily_lim = l20 if (abs(cp-l20)<abs(cp-l10) and abs(cp-l20)<=max(2*tw_tick(l20),l20*0.001)) else l10
                if cp >= (daily_lim - tw_tick(daily_lim)): past_boards += 1
                else: break

            # 鎖死
            min_bid = 80_000 if r["last"] < 50 else 120_000 if r["last"] < 100 else 200_000
            is_locked = (r["best_bid"] >= r["upper"] - tw_tick(r["upper"])) and (r["bid_sh1"] >= min_bid)
            if not is_locked: stats["未鎖死"].append(label)

            # 爆量
            vol_ratio = r["vol_sh"] / (vol_ma20_sh * frac + 1e-9)
            if vol_ratio < v_ratio_lim: stats["爆量不足"].append(label); continue

            # 一致性
            rng_raw = r["high"] - r["low"]
            close_pos = 1.0 if rng_raw <= 2 * tw_tick(r["upper"]) else (r["last"] - r["low"]) / rng_raw
            pullback = (r["high"] - r["last"]) / max(1e-9, r["high"])
            if pullback > pb_lim: stats["回落過大"].append(label); continue
            if close_pos < cp_lim: stats["收盤太弱"].append(label); continue

            results.append({"代號": c, "名稱": name, "現價": r["last"], "距離(%)": r["dist"], "張數": int(r["vol_sh"]/1000), "爆量x": vol_ratio, "狀態": "🔒 已鎖" if is_locked else "⚡ 發動", "階段": f"連續 {past_boards+1} 板"})
        except: pass
    return pd.DataFrame(results), stats

# =========================
# MAIN APP
# =========================
st.markdown('<div class="title">WarRoom Pro</div>', unsafe_allow_html=True)
st.markdown('<div class="subtitle">起漲戰情室 ｜ 精準、極簡、秒級追蹤 </div>', unsafe_allow_html=True)

# Apple Style Switch
cols_ui = st.columns([1, 1, 1])
with cols_ui[1]:
    is_test = st.toggle("🔥 測試模式 (盤後覆盤專用)", value=False)

if st.button("🚀 啟動全市場秒級掃描"):
    now_ts = now_taipei()
    with st.status("⚡正在解析市場動態...", expanded=True) as status:
        meta = get_stock_list()
        pre_df, mis_err = fast_mis_scan(meta, status, now_ts, is_test)
        final_res, stats = core_filter_engine(pre_df, meta, now_ts, status, mis_err, is_test)
        status.update(label="✅ 分析完成", state="complete")

    # --- 頂部儀表板 ---
    st.markdown("### 📊 市場實時統計")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("初始候選", stats.get("Total", 0))
    m2.metric("錄取檔數", len(final_res))
    m3.metric("鎖死率", f"{int(len(final_res[final_res['狀態']=='🔒 已鎖'])/len(final_res)*100)}%" if not final_res.empty else "0%")
    m4.metric("MIS 狀態", "🟢 正常" if mis_err==0 else f"🔴 失敗 {mis_err}")

    # --- 戰損名單 (透明化) ---
    with st.expander("🔍 淘汰數據分析 (點擊查看被刷掉的標的)", expanded=False):
        for reason, stocks in stats.items():
            if isinstance(stocks, list) and stocks:
                st.markdown(f"**{reason}**")
                # 用標籤雲呈現淘汰名單
                tags_html = "".join([f'<span class="fail-tag">{s}</span>' for s in stocks])
                st.markdown(f'<div>{tags_html}</div>', unsafe_allow_html=True)
                st.markdown('<div style="height:10px;"></div>', unsafe_allow_html=True)

    # --- 最終名單渲染 ---
    if not final_res.empty:
        st.markdown("---")
        st.markdown("### 🎯 錄取名單")
        cols = st.columns(4)
        for i, r in final_res.iterrows():
            with cols[i % 4]:
                st.markdown(f"""
                <div class="pro-card">
                    <div class="tag-pro tag-rise">{r['階段']}</div>
                    <div class="code-label">{r['代號']}</div>
                    <div class="stock-name">{r['名稱']}</div>
                    <div style="height:15px;"></div>
                    <div class="price-large">{r['現價']:.2f}</div>
                    <div style="font-size:13px; color:#86868b; margin-top:10px;">
                        {r['狀態']} | 爆量 {r['爆量x']:.1f}x
                    </div>
                </div>
                """, unsafe_allow_html=True)
    else:
        st.warning("🌙 凌晨測試中，全市場未發現符合『特種部隊』濾網的標的。建議開啟測試模式。")
