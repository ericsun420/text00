# app.py — 起漲戰情室｜戰神 5.2｜工業級診斷架構｜Apple Pro 美學｜效能與資料品質監控
import io
import math
import time
from datetime import datetime, timedelta, time as dtime
from collections import deque

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
import pandas as pd
import yfinance as yf
import streamlit as st

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================
# SYSTEM DIAGNOSTICS & UTILS
# =========================
def diag_init():
    return {
        "meta_count": 0, "mis_req_err": 0, "mis_rows": 0, "mis_skip_parse": 0,
        "yf_symbols": 0, "yf_fail": 0, "other_err": 0,
        "last_errors": deque(maxlen=5),
        "t_meta": 0.0, "t_mis": 0.0, "t_yf": 0.0, "t_filter": 0.0, "total": 0.0
    }

def diag_err(diag, e, tag="ERR"):
    diag["last_errors"].append(f"[{tag}] {type(e).__name__}: {e}")

def make_retry_session():
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=0.4, status_forcelist=(429, 500, 502, 503, 504), allowed_methods=("GET",))
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20))
    return s

@st.cache_data(ttl=6*3600, show_spinner=False)
def yf_download_daily(syms):
    if not syms: return None
    return yf.download(tickers=" ".join(syms), period="120d", interval="1d", group_by="ticker", auto_adjust=False, threads=True, progress=False)

# =========================
# UI / THEME
# =========================
st.set_page_config(page_title="WarRoom Pro 5.2", page_icon="⚡", layout="wide", initial_sidebar_state="collapsed")
st.markdown("""
<style>
    [data-testid="stAppViewContainer"] { background: radial-gradient(circle at top right, #1c1c1e, #000000) !important; color: #f5f5f7 !important; }
    .block-container { padding-top: 2rem; max-width: 1200px; }
    [data-testid="stSidebar"] { display: none !important; }
    .title { font-size: 52px; font-weight: 800; background: linear-gradient(180deg, #ffffff, #a1a1a6); -webkit-background-clip: text; -webkit-text-fill-color: transparent; text-align: center; }
    .pro-card { background: rgba(28, 28, 30, 0.7); backdrop-filter: blur(20px); border: 1px solid rgba(255, 255, 255, 0.1); border-radius: 24px; padding: 24px; margin-bottom: 16px; }
    .stock-name { font-size: 20px; font-weight: 700; color: #ffffff; }
    .price-large { font-size: 32px; font-weight: 700; color: #ffffff; }
    .tag-pro { padding: 4px 12px; border-radius: 8px; font-size: 11px; font-weight: 700; background: rgba(0, 122, 255, 0.15); color: #0a84ff; }
    .fail-tag { display: inline-block; padding: 4px 10px; background: rgba(255, 69, 58, 0.08); color: #ff453a; border-radius: 6px; margin: 3px; font-size: 11px; border: 1px solid rgba(255, 69, 58, 0.15); }
    .stButton>button { border-radius: 14px !important; background: #ffffff !important; color: #000000 !important; font-weight: 700; padding: 18px !important; width: 100% !important; border: none; }
</style>
""", unsafe_allow_html=True)

# =========================
# ENGINES (Diag Enabled)
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

def calc_limit_up(prev_close, limit_pct=0.10):
    raw = prev_close * (1.0 + limit_pct)
    tick = 0.01 if raw<10 else 0.05 if raw<50 else 0.1 if raw<100 else 0.5 if raw<500 else 1.0 if raw<1000 else 5.0
    n = math.floor((raw + 1e-12) / tick)
    return round(n * tick, 2 if tick < 0.1 else 1 if tick < 1 else 0)

@st.cache_data(ttl=3600, show_spinner=False)
def get_stock_list():
    meta, session = {}, make_retry_session()
    urls = [("tse", "https://raw.githubusercontent.com/mlouielu/twstock/master/twstock/codes/twse_equities.csv"),
            ("otc", "https://raw.githubusercontent.com/mlouielu/twstock/master/twstock/codes/tpex_equities.csv")]
    for ex, url in urls:
        try:
            r = session.get(url, timeout=15)
            r.raise_for_status()
            df = pd.read_csv(io.StringIO(r.text.replace("\r", "")), dtype=str, engine="python", on_bad_lines="skip")
            c_col = [c for c in df.columns if 'code' in c.lower()] or [df.columns[1]]
            n_col = [c for c in df.columns if 'name' in c.lower()] or [df.columns[2]]
            for _, row in df.iterrows():
                code = str(row[c_col[0]]).strip()
                if len(code) == 4 and code.isdigit():
                    meta[code] = {"name": str(row[n_col[0]]), "ex": ex}
        except: pass
    return meta

def fast_mis_scan(meta_dict, status_placeholder, now_ts, is_test):
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://mis.twse.com.tw/stock/fibest.jsp?lang=zh_tw"}
    session, rows, err_mis = make_retry_session(), [], 0
    mis_diag = {"mis_req_err": 0, "mis_rows": 0, "mis_skip_parse": 0}
    
    m = int((datetime.combine(now_ts.date(), now_ts.time()) - datetime.combine(now_ts.date(), dtime(9, 0))).total_seconds() // 60)
    dist_limit = 5.0 if is_test else (3.1 if m <= 60 else 2.2 if m <= 180 else 1.5)
    vol_limit = 200 if is_test else 800
    
    codes = list(meta_dict.keys())
    for i in range(0, len(codes), 80):
        chunk = codes[i:i+80]
        ex_ch = "%7c".join([f"{meta_dict[c]['ex']}_{c}.tw" for c in chunk])
        url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={ex_ch}&json=1&delay=0&_={int(time.time()*1000)}"
        status_placeholder.update(label=f"📡 快篩雷達中 ({i}/{len(codes)})", state="running")
        try:
            r = session.get(url, headers=headers, timeout=12)
            data = r.json().get("msgArray", [])
        except Exception as e:
            err_mis += 1; mis_diag["mis_req_err"] += 1; continue

        for q in data:
            try:
                c, z, u, v, y = q.get("c"), q.get("z"), q.get("u"), q.get("v"), q.get("y")
                if not c or c not in meta_dict: continue
                if z in (None, "", "-", "—") or u in (None, "", "-", "—") or y in (None, "", "-", "—", "0"):
                    mis_diag["mis_skip_parse"] += 1; continue
                last, upper, prev_close, vol_sh = float(z), float(u), float(y), float(v or 0)
                if upper <= 0 or prev_close <= 0: # 【修正：防止除以 0】
                    mis_diag["mis_skip_parse"] += 1; continue
                dist_pct = max(0.0, ((upper - last) / upper) * 100)
                if (vol_sh / 1000) >= vol_limit and dist_pct <= dist_limit:
                    rows.append({"code": c, "last": last, "upper": upper, "dist": dist_pct, "vol_sh": vol_sh, "prev_close": prev_close,
                                 "high": float(q.get("h") if q.get("h") not in (None,"","-","—") else last),
                                 "best_bid": split_nums(q.get("b"))[0] if q.get("b") else 0,
                                 "bid_sh1": split_nums(q.get("g"))[0] if q.get("g") else 0})
                    mis_diag["mis_rows"] += 1
            except: mis_diag["mis_skip_parse"] += 1
        time.sleep(0.12 if not is_test else 0.01)
    return pd.DataFrame(rows), err_mis, mis_diag

def core_filter_engine(candidates_df, meta_dict, now_ts, is_test, use_lock, use_hype):
    stats = {"Total": 0, "爆量不足": [], "回落過大": [], "收盤太弱": [], "過熱排除": [], "未鎖死": []}
    yf_diag = {"yf_symbols": 0, "yf_fail": 0, "other_err": 0}
    if candidates_df.empty: return pd.DataFrame(), stats, yf_diag
    
    candidates_df = candidates_df.sort_values(["dist", "vol_sh"], ascending=[True, False]).head(80)
    stats["Total"] = len(candidates_df)
    syms = [f"{c}.{'TW' if meta_dict[c]['ex']=='tse' else 'TWO'}" for c in candidates_df["code"]]
    yf_diag["yf_symbols"] = len(syms)
    
    raw_daily = yf_download_daily(syms)
    if raw_daily is None: return pd.DataFrame(), stats, yf_diag

    results, today_date = [], now_ts.date()
    m = int((datetime.combine(now_ts.date(), now_ts.time()) - datetime.combine(now_ts.date(), dtime(9, 0))).total_seconds() // 60)
    frac = 0.12 if m <= 30 else 0.12 + (0.5 - 0.12) * ((m - 30) / 90.0) if m <= 120 else min(1.0, 0.5 + (1.0 - 0.5) * ((m - 120) / 150.0))
    if is_test: frac = 0.5
    
    for _, r in candidates_df.iterrows():
        c, name = r["code"], meta_dict[r["code"]]["name"]
        sym = f"{c}.{'TW' if meta_dict[c]['ex']=='tse' else 'TWO'}"
        try:
            dfD = raw_daily[sym].dropna() if isinstance(raw_daily.columns, pd.MultiIndex) else raw_daily.dropna()
            if len(dfD) < 30: yf_diag["yf_fail"] += 1; continue
            
            past_df = dfD.iloc[:-1].copy() if dfD.index[-1].date() == today_date else dfD.copy()
            vol_ma20_sh = float(past_df["Volume"].rolling(20).mean().iloc[-1])
            
            # 連板計算
            past_boards, past_10 = 0, past_df.tail(10)
            for i in range(len(past_10)-1, 0, -1):
                cp, pp = float(past_10["Close"].iloc[i]), float(past_10["Close"].iloc[i-1])
                lim = calc_limit_up(pp, 0.20) if abs(cp/pp - 1.20) < 0.01 else calc_limit_up(pp, 0.10)
                if cp >= (lim - 0.05): past_boards += 1
                else: break

            if use_hype and past_boards == 0 and any(float(past_10["Close"].iloc[j]/past_10["Close"].iloc[j-1]) > 1.095 for j in range(1, len(past_10))):
                stats["過熱排除"].append(f"{c} {name}"); continue

            min_bid = 80_000 if r["last"] < 50 else 120_000 if r["last"] < 100 else 200_000
            is_locked = (r["best_bid"] >= r["upper"] - 0.1) and (r["bid_sh1"] >= min_bid)
            if not is_locked:
                stats["未鎖死"].append(f"{c} {name}")
                if use_lock: continue

            vol_ratio = r["vol_sh"] / (vol_ma20_sh * frac + 1e-9)
            if vol_ratio < (0.5 if is_test else 1.3): stats["爆量不足"].append(f"{c} {name}"); continue
            
            rng = r["high"] - r["low"]
            if (r["high"] - r["last"]) / max(1e-9, r["high"]) > (0.05 if is_test else 0.0039): stats["回落過大"].append(f"{c} {name}"); continue
            if (r["last"] - r["low"]) / max(1e-9, rng) < (0.5 if is_test else 0.80) and rng > 0.1: stats["收盤太弱"].append(f"{c} {name}"); continue

            results.append({"代號": c, "名稱": name, "現價": r["last"], "爆量": vol_ratio, "狀態": "🔒 已鎖" if is_locked else "⚡ 發動", "階段": f"連續 {past_boards+1} 板"})
        except Exception as e: yf_diag["other_err"] += 1; diag_err(yf_diag, e, "FILTER")
    return pd.DataFrame(results), stats, yf_diag

# =========================
# MAIN
# =========================
st.markdown('<div class="title">WarRoom Pro 5.2</div>', unsafe_allow_html=True)
col_cfg = st.columns(4)
with col_cfg[0]: is_test = st.toggle("🔥 測試模式", value=False)
with col_cfg[1]: use_lock = st.toggle("⚔️ 必須鎖死", value=False)
with col_cfg[2]: use_hype = st.toggle("🛡️ 過熱排除", value=True)

if st.button("🚀 啟動診斷級掃描"):
    t0 = time.perf_counter()
    diag = diag_init()
    
    with st.status("⚡ 正在分析...", expanded=True) as status:
        t = time.perf_counter()
        meta = get_stock_list()
        diag["t_meta"] = time.perf_counter() - t; diag["meta_count"] = len(meta)
        
        t = time.perf_counter(); now_ts = now_taipei()
        pre_df, mis_err, mis_diag = fast_mis_scan(meta, status, now_ts, is_test)
        diag["t_mis"] = time.perf_counter() - t; diag.update(mis_diag)
        
        t = time.perf_counter()
        final_res, stats, yf_diag = core_filter_engine(pre_df, meta, now_ts, is_test, use_lock, use_hype)
        diag["t_filter"] = time.perf_counter() - t; diag.update(yf_diag)
        
        diag["total"] = time.perf_counter() - t0
        status.update(label="✅ 分析完成", state="complete")
    st.session_state["last_scan"] = {"final_res": final_res, "stats": stats, "diag": diag, "ts": now_ts}

scan = st.session_state.get("last_scan")
if scan:
    d, res, sts = scan["diag"], scan["final_res"], scan["stats"]
    st.caption(f"上次更新：{scan['ts'].strftime('%H:%M:%S')} | 耗時：{d['total']:.2f}s")
    
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("初始候選", d.get("Total", 0))
    m2.metric("錄取檔數", len(res))
    m3.metric("資料品質", f"{100 - (d.get('mis_skip_parse',0)/max(1,d.get('meta_count',1))*100):.1f}%")
    m4.metric("系統異常", d.get("mis_req_err",0) + d.get("yf_fail",0) + d.get("other_err",0))

    with st.expander("🧪 系統診斷 (資料源/效能/錯誤)", expanded=False):
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Meta 檔數", d.get("meta_count"))
        c2.metric("MIS 入列", d.get("mis_rows"))
        c3.metric("MIS 請求失敗", d.get("mis_req_err"))
        c4.metric("YF 下載失敗", d.get("yf_fail"))
        st.caption(f"耗時分布：Meta {d['t_meta']:.2f}s | MIS {d['t_mis']:.2f}s | Filter {d['t_filter']:.2f}s")
        if d.get("last_errors"): st.code("\n".join(d["last_errors"]))

    with st.expander("🔍 淘汰數據分析 (實名點名)", expanded=True):
        for reason, stocks in sts.items():
            if isinstance(stocks, list) and stocks:
                st.markdown(f"**{reason}**")
                st.markdown(f'<div>' + "".join([f'<span class="fail-tag">{s}</span>' for s in stocks]) + '</div>', unsafe_allow_html=True)

    if not res.empty:
        cols = st.columns(4)
        for i, r in res.iterrows():
            with cols[i % 4]:
                st.markdown(f"""<div class="pro-card"><div class="tag-pro">{r['階段']}</div><div class="stock-name">{r['名稱']}</div>
                    <div style="height:15px;"></div><div class="price-large">{r['現價']:.2f}</div>
                    <div style="font-size:12px; color:#86868b; margin-top:10px;">{r['狀態']} | 爆量 {r['爆量']:.1f}x</div></div>""", unsafe_allow_html=True)
    else: st.warning("目前無標的存活。")
