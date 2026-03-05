# app.py — 起漲戰情室｜戰神 5.1｜防彈工業級架構｜Apple Pro 旗艦視覺｜持久化與狀態同步
import io
import math
import time
from datetime import datetime, timedelta, time as dtime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
import pandas as pd
import yfinance as yf
import streamlit as st

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================
# SYSTEM UTILS (Retry & Caching)
# =========================
def make_retry_session():
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",)
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

@st.cache_data(ttl=6*3600, show_spinner=False)
def yf_download_daily(syms):
    if not syms: return None
    return yf.download(
        tickers=" ".join(syms),
        period="120d",
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        threads=True,
        progress=False,
    )

# =========================
# UI / THEME (Apple Pro 4.0 Refined)
# =========================
st.set_page_config(page_title="WarRoom Pro", page_icon="⚡", layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
<style>
    [data-testid="stAppViewContainer"] { background: radial-gradient(circle at top right, #1c1c1e, #000000) !important; color: #f5f5f7 !important; }
    .block-container { padding-top: 2rem; max-width: 1200px; }
    [data-testid="stSidebar"] { display: none !important; }
    .title { font-size: 52px; font-weight: 800; letter-spacing: -1.5px; background: linear-gradient(180deg, #ffffff 0%, #a1a1a6 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; text-align: center; margin-bottom: 5px; }
    .subtitle { color: #86868b; font-size: 16px; text-align: center; margin-bottom: 30px; }
    .pro-card { background: rgba(28, 28, 30, 0.7); backdrop-filter: blur(20px); border: 1px solid rgba(255, 255, 255, 0.1); border-radius: 24px; padding: 24px; margin-bottom: 16px; }
    .code-label { font-size: 14px; color: #0a84ff; font-weight: 600; text-transform: uppercase; }
    .stock-name { font-size: 20px; font-weight: 700; color: #ffffff; margin-bottom: 8px; }
    .price-large { font-size: 32px; font-weight: 700; color: #ffffff; }
    .tag-pro { padding: 4px 12px; border-radius: 8px; font-size: 11px; font-weight: 700; display: inline-block; margin-bottom: 12px; background: rgba(0, 122, 255, 0.15); color: #0a84ff; border: 1px solid rgba(0, 122, 255, 0.3); }
    .fail-tag { display: inline-block; padding: 4px 10px; background: rgba(255, 69, 58, 0.08); color: #ff453a; border-radius: 6px; margin: 3px; font-size: 11px; border: 1px solid rgba(255, 69, 58, 0.15); }
    .stButton>button { border-radius: 14px !important; background: #ffffff !important; color: #000000 !important; font-weight: 700; padding: 18px !important; width: 100% !important; border: none; }
    .status-caption { color: #86868b; font-size: 12px; text-align: center; margin-top: 5px; }
</style>
""", unsafe_allow_html=True)

# =========================
# HELPERS
# =========================
def now_taipei(): return datetime.utcnow() + timedelta(hours=8)

def split_nums(s):
    out = []
    for x in str(s or "").split("_"):
        x = x.strip()
        if not x or x in ("-", "—"): continue
        try: out.append(float(x))
        except: pass
    return out

def get_vol_frac_and_dist(ts, is_test):
    m = int((datetime.combine(ts.date(), ts.time()) - datetime.combine(ts.date(), dtime(9, 0))).total_seconds() // 60)
    m = max(0, min(270, m)) 
    if is_test: return 0.5, 5.0, 0.05
    dist_lim = 3.1 if m <= 60 else 2.2 if m <= 180 else 1.5           
    pb_lim = 0.012 if m <= 90 else 0.0039
    frac = 0.12 if m <= 30 else 0.12 + (0.5 - 0.12) * ((m - 30) / 90.0) if m <= 120 else min(1.0, 0.5 + (1.0 - 0.5) * ((m - 120) / 150.0))
    return frac, dist_lim, pb_lim

def tw_tick(price):
    return 0.01 if price<10 else 0.05 if price<50 else 0.1 if price<100 else 0.5 if price<500 else 1.0 if price<1000 else 5.0

def calc_limit_up(prev_close, limit_pct=0.10):
    raw = prev_close * (1.0 + limit_pct)
    tick = tw_tick(raw)
    n = math.floor((raw + 1e-12) / tick)
    price = n * tick
    return round(price, 2 if tick < 0.1 else 1 if tick < 1 else 0)

# =========================
# ENGINES
# =========================
@st.cache_data(ttl=3600, show_spinner=False)
def get_stock_list():
    meta = {}
    session = make_retry_session()
    urls = [("tse", "https://raw.githubusercontent.com/mlouielu/twstock/master/twstock/codes/twse_equities.csv"),
            ("otc", "https://raw.githubusercontent.com/mlouielu/twstock/master/twstock/codes/tpex_equities.csv")]
    for ex, url in urls:
        try:
            r = session.get(url, timeout=15)
            r.raise_for_status() # 【修正 2】: 確保不是下載到 HTML 報錯頁面
            df = pd.read_csv(io.StringIO(r.text.replace("\r", "")), dtype=str, engine="python", on_bad_lines="skip")
            col_map = {c.strip().lower(): c for c in df.columns}
            c_col = col_map.get('code') or df.columns[1]
            n_col = col_map.get('name') or df.columns[2]
            g_col = col_map.get('group') or (df.columns[6] if len(df.columns) > 6 else None)
            t_col = col_map.get('type') or df.columns[0]
            for _, row in df.iterrows():
                code = str(row[c_col]).strip()
                if len(code) == 4 and code.isdigit():
                    if t_col and ("權證" in str(row[t_col]) or "ETF" in str(row[t_col])): continue
                    meta[code] = {"name": str(row[n_col]), "ind": str(row[g_col]) if g_col else "未分類", "ex": ex}
        except: pass
    return meta

def fast_mis_scan(meta_dict, status_placeholder, now_ts, is_test):
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://mis.twse.com.tw/stock/fibest.jsp?lang=zh_tw"}
    session = make_retry_session()
    try: session.get("https://mis.twse.com.tw/stock/fibest.jsp?lang=zh_tw", headers=headers, timeout=15)
    except: pass
    
    _, dist_limit, _ = get_vol_frac_and_dist(now_ts, is_test)
    vol_limit = 200 if is_test else 800
    codes = list(meta_dict.keys())
    rows, err_mis = [], 0
    batch_size = 80
    
    for i in range(0, len(codes), batch_size):
        chunk = codes[i:i+batch_size]
        ex_ch = "%7c".join([f"{meta_dict[c]['ex']}_{c}.tw" for c in chunk])
        url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={ex_ch}&json=1&delay=0&_={int(time.time()*1000)}"
        status_placeholder.update(label=f"📡 全市場雷達掃描中... ({i}/{len(codes)})", state="running")
        try:
            r = session.get(url, headers=headers, timeout=12)
            data = r.json().get("msgArray", [])
        except:
            err_mis += 1
            continue

        for q in data:
            try: # 【核心修正 1】: 單檔 Try-Except，拒絕 Batch 報廢
                c, z, u, v, y = q.get("c"), q.get("z"), q.get("u"), q.get("v"), q.get("y")
                if not c or c not in meta_dict: continue
                if z in (None, "", "-", "—") or u in (None, "", "-", "—") or y in (None, "", "-", "—", "0"): continue
                
                last, upper, prev_close, vol_sh = float(z), float(u), float(y), float(v or 0)
                dist_pct = max(0.0, ((upper - last) / upper) * 100)
                
                if (vol_sh / 1000) >= vol_limit and dist_pct <= dist_limit:
                    bp, bv = split_nums(q.get("b")), split_nums(q.get("g"))
                    rows.append({
                        "code": c, "last": last, "upper": upper, "dist": dist_pct, "vol_sh": vol_sh, "prev_close": prev_close,
                        "high": float(q.get("h") if q.get("h") not in (None, "", "-", "—") else last),
                        "low": float(q.get("l") if q.get("l") not in (None, "", "-", "—") else last),
                        "best_bid": bp[0] if bp else 0.0, "bid_sh1": bv[0] if bv else 0.0
                    })
            except: continue
        time.sleep(0.12 if not is_test else 0.01)
    return pd.DataFrame(rows), err_mis

def core_filter_engine(candidates_df, meta_dict, now_ts, status_placeholder, mis_err, is_test, use_strict_lock, use_hype_check):
    stats = {"Total": 0, "爆量不足": [], "回落過大": [], "收盤太弱": [], "過熱排除": [], "未鎖死": [], "YF失敗": 0, "MIS_Err": mis_err}
    if candidates_df.empty: return pd.DataFrame(), stats
    
    candidates_df = candidates_df.sort_values(["dist", "vol_sh"], ascending=[True, False]).head(80)
    stats["Total"] = len(candidates_df)
    syms = [f"{c}.{'TW' if meta_dict[c]['ex']=='tse' else 'TWO'}" for c in candidates_df["code"]]
    
    raw_daily = yf_download_daily(syms)
    if raw_daily is None: return pd.DataFrame(), stats

    results, today_date = [], now_ts.date()
    frac, _, pb_lim = get_vol_frac_and_dist(now_ts, is_test)
    v_ratio_lim, cp_lim = (0.5, 0.50) if is_test else (1.3, 0.80)

    for _, r in candidates_df.iterrows():
        c, name = r["code"], meta_dict[r["code"]]["name"]
        label = f"{c} {name}"
        sym = f"{c}.{'TW' if meta_dict[c]['ex']=='tse' else 'TWO'}"
        try:
            dfD = raw_daily[sym].dropna() if isinstance(raw_daily.columns, pd.MultiIndex) else raw_daily.dropna()
            if len(dfD) < 30: 
                stats["YF失敗"] += 1 # 【修正 3】: 累計失敗次數
                continue
            
            has_today = dfD.index[-1].date() == today_date
            past_df = dfD.iloc[:-1].copy() if has_today else dfD.copy()
            vol_ma20_sh = float(past_df["Volume"].rolling(20).mean().iloc[-1])
            
            past_boards = 0
            past_10 = past_df.tail(10)
            for i in range(len(past_10)-1, 0, -1):
                cp, pp, hp = float(past_10["Close"].iloc[i]), float(past_10["Close"].iloc[i-1]), float(past_10["High"].iloc[i])
                l10, l20 = calc_limit_up(pp, 0.10), calc_limit_up(pp, 0.20)
                tol = max(2*tw_tick(l20), l20*0.001)
                daily_lim = l20 if (abs(cp-l20)<abs(cp-l10) and abs(cp-l20)<=tol) else l10
                if cp >= (daily_lim - tw_tick(daily_lim)): past_boards += 1
                else: break

            if use_hype_check and past_boards == 0:
                if any(float(past_10["Close"].iloc[j]/past_10["Close"].iloc[j-1]) > 1.095 for j in range(1, len(past_10))):
                    stats["過熱排除"].append(label); continue

            min_bid = 80_000 if r["last"] < 50 else 120_000 if r["last"] < 100 else 200_000
            is_locked = (r["best_bid"] >= r["upper"] - tw_tick(r["upper"])) and (r["bid_sh1"] >= min_bid)
            if not is_locked:
                stats["未鎖死"].append(label)
                if use_strict_lock: continue

            vol_ratio = r["vol_sh"] / (vol_ma20_sh * frac + 1e-9)
            if vol_ratio < v_ratio_lim: stats["爆量不足"].append(label); continue

            rng_raw = r["high"] - r["low"]
            close_pos = 1.0 if rng_raw <= 2 * tw_tick(r["upper"]) else (r["last"] - r["low"]) / rng_raw
            pullback = (r["high"] - r["last"]) / max(1e-9, r["high"])
            if pullback > pb_lim: stats["回落過大"].append(label); continue
            if close_pos < cp_lim: stats["收盤太弱"].append(label); continue

            results.append({"代號": c, "名稱": name, "現價": r["last"], "距離(%)": r["dist"], "爆量x": vol_ratio, "狀態": "🔒 已鎖" if is_locked else "⚡ 發動", "階段": f"連續 {past_boards+1} 板"})
        except: stats["其他錯誤"] = stats.get("其他錯誤",0)+1
    return pd.DataFrame(results), stats

# =========================
# MAIN APP
# =========================
st.markdown('<div class="title">WarRoom Pro</div>', unsafe_allow_html=True)
st.markdown('<div class="subtitle">起漲戰情室 ｜ Apple Pro 工業級旗艦版 </div>', unsafe_allow_html=True)

c1, c2, c3, c4 = st.columns(4)
with c1: is_test = st.toggle("🔥 測試模式", value=False)
with c2: use_strict_lock = st.toggle("⚔️ 必須鎖死", value=False)
with c3: use_hype_check = st.toggle("🛡️ 過熱排除", value=True)

if st.button("🚀 啟動全市場秒級掃描"):
    now_ts = now_taipei()
    with st.status("⚡正在解析市場動態...", expanded=True) as status:
        meta = get_stock_list()
        pre_df, mis_err = fast_mis_scan(meta, status, now_ts, is_test)
        final_res, stats = core_filter_engine(pre_df, meta, now_ts, status, mis_err, is_test, use_strict_lock, use_hype_check)
        status.update(label="✅ 分析完成", state="complete")
    # 【核心修正】: 存入 Session 實現持久化渲染
    st.session_state["last_scan"] = {"ts": now_ts, "final_res": final_res, "stats": stats, "toggles": {"test": is_test, "lock": use_strict_lock, "hype": use_hype_check}}

scan = st.session_state.get("last_scan")
if scan:
    final_res, stats, ts, toggles = scan["final_res"], scan["stats"], scan["ts"], scan["toggles"]
    t_str = f"測試:{'ON' if toggles['test'] else 'OFF'} | 鎖死:{'ON' if toggles['lock'] else 'OFF'} | 過熱:{'ON' if toggles['hype'] else 'OFF'}"
    st.markdown(f'<div class="status-caption">上次更新：{ts.strftime("%H:%M:%S")}（{t_str}）</div>', unsafe_allow_html=True)
    
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("初始候選", stats.get("Total", 0))
    m2.metric("錄取檔數", len(final_res))
    lock_rate = int(len(final_res[final_res['狀態']=='🔒 已鎖'])/len(final_res)*100) if not final_res.empty else 0
    m3.metric("鎖死率", f"{lock_rate}%")
    m4.metric("監控異常", stats.get("MIS_Err",0) + stats.get("YF失敗",0))

    with st.expander("🔍 淘汰數據分析 (實名名單)", expanded=False):
        for reason, stocks in stats.items():
            if isinstance(stocks, list) and stocks:
                st.markdown(f"**{reason}**")
                tags_html = "".join([f'<span class="fail-tag">{s}</span>' for s in stocks])
                st.markdown(f'<div>{tags_html}</div>', unsafe_allow_html=True)

    if not final_res.empty:
        st.markdown("---")
        cols = st.columns(4)
        for i, r in final_res.iterrows():
            with cols[i % 4]:
                st.markdown(f"""<div class="pro-card"><div class="tag-pro">{r['階段']}</div><div class="code-label">{r['代號']}</div><div class="stock-name">{r['名稱']}</div>
                    <div style="height:15px;"></div><div class="price-large">{r['現價']:.2f}</div>
                    <div style="font-size:12px; color:#86868b; margin-top:10px;">{r['狀態']} | 爆量 {r['爆量x']:.1f}x</div></div>""", unsafe_allow_html=True)
    else: st.warning("目前沒有標的存活。")
