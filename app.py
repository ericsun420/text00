# app.py — 起漲戰情室｜戰神修正版 3.7｜1~N 根連板通吃｜動態閾值同步與精度強化
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
st.set_page_config(page_title="起漲戰情室｜戰神 3.7", page_icon="⚡", layout="wide", initial_sidebar_state="collapsed")

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
.tag-stage1{ font-size: 11px; padding: 3px 7px; border-radius: 999px; border:1px solid #3b82f6; background: rgba(59,130,246,0.2); color: #93c5fd; font-weight: bold;}
.tag-stage2{ font-size: 11px; padding: 3px 7px; border-radius: 999px; border:1px solid #f97316; background: rgba(249,115,22,0.2); color: #fdba74; font-weight: bold;}
.tag-stage3{ font-size: 11px; padding: 3px 7px; border-radius: 999px; border:1px solid #ef4444; background: rgba(239,68,68,0.2); color: #fca5a5; font-weight: bold;}
.tag-stage4{ font-size: 11px; padding: 3px 7px; border-radius: 999px; border:1px solid #a855f7; background: rgba(168,85,247,0.2); color: #d8b4fe; font-weight: bold;}
.stButton>button{ border-radius: 16px !important; border: 1px solid rgba(255,255,255,0.2) !important; background: linear-gradient(90deg, #1f2937, #111827) !important; color: white !important; font-weight: 900 !important; font-size: 20px !important; padding: 25px !important; transition: all 0.3s ease; box-shadow: 0 4px 15px rgba(0,0,0,0.5); }
.stButton>button:hover{ border-color: #f87171 !important; transform: translateY(-2px); box-shadow: 0 6px 20px rgba(248,113,113,0.2); }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

# =========================
# HELPERS
# =========================
def now_taipei(): return datetime.utcnow() + timedelta(hours=8)

def get_vol_frac_and_dist(ts):
    m = int((datetime.combine(ts.date(), ts.time()) - datetime.combine(ts.date(), dtime(9, 0))).total_seconds() // 60)
    m = max(0, min(270, m)) 
    
    # 距離動態收斂
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
    # 【核心修正】: 向下調整到合法 Tick
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
            
            c_col = col_map.get('code')  or (df.columns[1] if len(df.columns) > 1 else None)
            n_col = col_map.get('name')  or (df.columns[2] if len(df.columns) > 2 else None)
            g_col = col_map.get('group') or (df.columns[6] if len(df.columns) > 6 else None)
            t_col = col_map.get('type')  or (df.columns[0] if len(df.columns) > 0 else None)
            
            if not c_col: continue

            for _, row in df.iterrows():
                code = str(row[c_col]).strip()
                if len(code) == 4 and code.isdigit():
                    stype = str(row[t_col]) if t_col else ""
                    if "權證" in stype or "ETF" in stype: continue
                    # 【核心修正 4】: 更徹底的產業名稱清洗
                    ind = str(row[g_col]).strip() if g_col and pd.notna(row[g_col]) else ""
                    if not ind or ind.lower() in ("nan", "none") or ind in ("-", "—"):
                        ind = "未分類"
                    meta[code] = {"name": str(row[n_col]) if n_col else "未知", "ind": ind, "ex": ex}
        except: pass
    if not meta: raise ValueError("清單載入失敗。")
    return meta

# =========================
# ENGINE 2: MIS 盤中快篩
# =========================
def fast_mis_scan(meta_dict, status_placeholder, now_ts):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://mis.twse.com.tw/stock/fibest.jsp?lang=zh_tw",
    }
    s = requests.Session()
    try: s.get("https://mis.twse.com.tw/stock/fibest.jsp?lang=zh_tw", headers=headers, timeout=15, verify=False)
    except: pass
    
    # 使用傳入的 now_ts 保持時間一致
    _, dist_limit = get_vol_frac_and_dist(now_ts)
    codes = list(meta_dict.keys())
    rows, err_mis = [], 0
    batch_size = 80 
    
    for i in range(0, len(codes), batch_size):
        chunk = codes[i:i+batch_size]
        ex_ch = "%7c".join([f"{meta_dict[c]['ex']}_{c}.tw" for c in chunk])
        url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={ex_ch}&json=1&delay=0&_={int(time.time()*1000)}"
        status_placeholder.update(label=f"📡 MIS 快篩中 ({i//batch_size + 1}/{math.ceil(len(codes)/batch_size)})...", state="running")
        
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
                
                if (vol_sh / 1000) >= 800 and dist_pct <= dist_limit:
                    bp = split_nums(q.get("b")); bv = split_nums(q.get("g"))
                    ap = split_nums(q.get("a")); av = split_nums(q.get("f"))
                    rows.append({
                        "code": c, "last": last, "upper": upper, "dist": dist_pct, 
                        "vol_sh": vol_sh, "prev_close": prev_close,
                        "high": float(q.get("h") if q.get("h")!="-" else last),
                        "low": float(q.get("l") if q.get("l")!="-" else last),
                        "best_bid": bp[0] if bp else 0, "bid_sh1": bv[0] if bv else 0,
                        "best_ask": ap[0] if ap else 0, "ask_sh1": av[0] if av else 0
                    })
        except: err_mis += 1
            
    return pd.DataFrame(rows), err_mis

# =========================
# ENGINE 3: 核心濾網
# =========================
def core_filter_engine(candidates_df, meta_dict, now_ts, status_placeholder, mis_err):
    if candidates_df.empty: return pd.DataFrame(), {}
    
    candidates_df = candidates_df.sort_values(["dist", "vol_sh"], ascending=[True, False]).head(80)
    stats = {"Total": len(candidates_df), "NotLocked": 0, "Hype": 0, "Pullback": 0, "VolRatio": 0, "WeakClose": 0, "Err": 0, "YF_Fail": 0, "MIS_Err": mis_err}
    syms = [f"{c}.{'TW' if meta_dict[c]['ex']=='tse' else 'TWO'}" for c in candidates_df["code"]]
    
    try:
        raw_daily = yf.download(tickers=" ".join(syms), period="100d", interval="1d", group_by="ticker", auto_adjust=False, threads=False, progress=False)
        if len(syms) > 1 and not isinstance(raw_daily.columns, pd.MultiIndex):
            stats["YF_Fail"] += 1; return pd.DataFrame(), stats
    except: return pd.DataFrame(), stats

    results, today_date = [], now_ts.date()
    frac, _ = get_vol_frac_and_dist(now_ts)

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
            
            # 【核心修正 3】: 混合型逼近門檻
            past_boards = 0
            if len(past_df) >= 10:
                past_10 = past_df.tail(10)
                for i in range(len(past_10)-1, 0, -1):
                    cp, pp, hp = float(past_10["Close"].iloc[i]), float(past_10["Close"].iloc[i-1]), float(past_10["High"].iloc[i])
                    l10, l20 = calc_limit_up(pp, 0.10), calc_limit_up(pp, 0.20)
                    d10, d20 = min(abs(cp - l10), abs(hp - l10)), min(abs(cp - l20), abs(hp - l20))
                    # 判斷制度：混合 2*Tick 與 0.1% Price
                    tol20 = max(2 * tw_tick(l20), l20 * 0.001)
                    use20 = (d20 < d10) and (d20 <= tol20)
                    daily_lim = l20 if use20 else l10
                    if cp >= (daily_lim - tw_tick(daily_lim)): past_boards += 1
                    else: break
            
            stage_label, stage_class, stage_bonus = ("🚀 第一根", "tag-stage1", 10.0) if past_boards == 0 else \
                                                     ("🔥 第二連", "tag-stage2", 5.0) if past_boards == 1 else \
                                                     ("⚠️ 第三連", "tag-stage3", -5.0) if past_boards == 2 else \
                                                     (f"💀 第{past_boards+1}連", "tag-stage4", -15.0)

            if past_boards == 0:
                had_limit_past = False
                for j in range(len(past_df)-1, max(1, len(past_df)-10), -1):
                    cp_j, pp_j, hp_j = float(past_df["Close"].iloc[j]), float(past_df["Close"].iloc[j-1]), float(past_df["High"].iloc[j])
                    l10_j, l20_j = calc_limit_up(pp_j, 0.10), calc_limit_up(pp_j, 0.20)
                    d10_j, d20_j = min(abs(cp_j - l10_j), abs(hp_j - l10_j)), min(abs(cp_j - l20_j), abs(hp_j - l20_j))
                    tol20_j = max(2 * tw_tick(l20_j), l20_j * 0.001)
                    lim_j = l20_j if (d20_j < d10_j and d20_j <= tol20_j) else l10_j
                    if cp_j >= (lim_j - tw_tick(lim_j)): had_limit_past = True; break
                if had_limit_past: stats["Hype"] += 1; continue

            # 鎖死判定進化
            bid_sh1, ask_sh1 = r["bid_sh1"], r["ask_sh1"]
            has_ask = r["best_ask"] > 0
            ask_vol_unk = has_ask and (ask_sh1 <= 0)
            min_bid_sh = 80_000 if r["last"] < 50 else 120_000 if r["last"] < 100 else 200_000
            
            is_locked = (r["best_bid"] >= r["upper"] - tw_tick(r["upper"])) and (bid_sh1 >= min_bid_sh)
            # 【核心修正 2】: 賣盤容忍 2 Tick
            ask_at_upper = (not has_ask) or (r["best_ask"] >= r["upper"] - 2 * tw_tick(r["upper"]))
            
            if is_locked:
                if not ask_at_upper: is_locked = False
                elif not ask_vol_unk:
                    if ask_sh1 > max(150000, bid_sh1 * 0.6): is_locked = False
            
            if not is_locked: stats["NotLocked"] += 1

            vol_ratio = r["vol_sh"] / (vol_ma20_sh * frac + 1e-9)
            if vol_ratio < 1.3: stats["VolRatio"] += 1; continue

            rng_raw = r["high"] - r["low"]
            close_pos = 1.0 if rng_raw <= 2 * tw_tick(r["upper"]) else (r["last"] - r["low"]) / rng_raw
            pullback = (r["high"] - r["last"]) / max(1e-9, r["high"])
            
            if pullback > 0.0039: stats["Pullback"] += 1; continue
            if close_pos < 0.80: stats["WeakClose"] += 1; continue

            score = 40.0 + stage_bonus + (15.0 if is_locked else 0.0)
            score += 15.0 * min(1.0, max(0.0, (close_pos - 0.85) / 0.15))
            score += 15.0 * min(1.0, max(0.0, (vol_ratio - 1.5) / 2.5))
            
            results.append({
                "代號": c, "名稱": meta_dict[c]["name"], "族群": meta_dict[c]["ind"],
                "現價": r["last"], "距離(%)": r["dist"], "較昨收(%)": ((r["last"]/r["prev_close"])-1)*100, 
                "累積量": int(r["vol_sh"]/1000), "爆量x": vol_ratio, "狀態": "鎖死" if is_locked else "未鎖", 
                "買一": int(bid_sh1/1000), "賣一": int(ask_sh1/1000) if not ask_vol_unk else 0,
                "連板序號": past_boards + 1, "潛力分": max(0.0, min(100.0, score)),
                "階段": stage_label, "Class": stage_class
            })
        except: stats["Err"] += 1

    return pd.DataFrame(results).sort_values("潛力分", ascending=False).reset_index(drop=True) if results else pd.DataFrame(), stats

# =========================
# MAIN APP
# =========================
st.markdown('<div class="title">🧊 起漲戰情室</div>', unsafe_allow_html=True)
st.markdown('<div class="subtitle">戰神旗艦 3.7 ｜ 最終實戰版 ｜ 全方位資料一致性與制度識別</div>', unsafe_allow_html=True)

run_scan = st.button("🚀 啟動掃描 (自動鎖定強勢先機)", use_container_width=True)

if run_scan:
    # 【核心修正 1】: 全局鎖定 now_ts
    now_ts = now_taipei()
    
    with st.status("⚡ 狙擊中...", expanded=True) as status:
        try:
            meta = get_stock_list()
            pre_df, mis_err = fast_mis_scan(meta, status, now_ts)
            
            if mis_err >= 5:
                st.warning(f"⚠️ MIS 批次失敗 {mis_err} 次，可能漏掉部分標的。")

            if pre_df.empty:
                status.update(label="✅ 掃描完畢", state="complete")
                st.info(f"😴 目前沒標的。 (MIS錯誤: {mis_err})"); st.stop()
                
            final_res, stats = core_filter_engine(pre_df, meta, now_ts, status, mis_err)
            status.update(label="✅ 計算完成！", state="complete")
        except Exception as e:
            st.error(f"系統崩潰：{e}"); st.stop()

    with st.expander("📊 掃描統計戰報 (淘汰原因追蹤)", expanded=False):
        st.json(stats)

    if final_res.empty:
        st.warning("⚠️ 標的皆被濾網剔除，請見上方統計展開。")
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
                        <span>買/賣: <b>{r['買一']}/{r['賣一']}</b></span>
                    </div>
                </div>
                """, unsafe_allow_html=True)
        
        with st.expander("📋 數據總表"):
            st.dataframe(final_res.drop(columns=["Class"]).style.format({"現價":"{:.2f}","距離(%)":"{:.2f}%","較昨收(%)":"{:.2f}%","爆量x":"{:.1f}x","潛力分":"{:.1f}"}), use_container_width=True)
