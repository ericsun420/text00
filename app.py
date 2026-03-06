# app.py — 起漲戰情室｜戰神 v10.2 機構級瞬切版｜資料快取分離｜毫秒級切換濾網
import io
import math
import time
import re
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
# FUGLE API CONFIGURATION
# =========================
FUGLE_API_KEY = "ZWJjZDhjZWYtMjhhMi00YWI2LTliNWQtMmViYzVhMmIzODdjIGY1N2Y0MGZmLWQ1MjgtNDk1OC1iZTljLWMxOWUwODQ4Y2U2Zg=="
API_TIMEOUT = (3.0, 5.0)

# =========================
# SYSTEM DIAGNOSTICS
# =========================
def diag_init():
    return {
        "meta_count": 0, "rank_count": 0, "cand_total": 0, "fugle_req_err": 0,
        "rank_src": "None", 
        "fugle_seen": 0, "fugle_parse_ok": 0, "fugle_parse_fail": 0, "fugle_rows": 0,
        "fugle_http_err": 0,
        "yf_symbols": 0, "yf_returned": 0, "yf_fail": 0, "other_err": 0,
        "yf_bulk_fail": 0, "yf_rescue_used": 0,
        "yf_parts_ok": 0, "yf_parts_fail": 0,
        "last_errors": deque(maxlen=8),
        "t_meta": 0.0, "t_rank": 0.0, "t_api": 0.0, "t_yf": 0.0, "t_filter": 0.0, "total": 0.0
    }

def diag_err(diag, e, tag="ERR"):
    diag["last_errors"].append(f"[{tag}] {type(e).__name__}: {e}")

def get_base_headers():
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
    }

def make_retry_session(base_headers=None):
    s = requests.Session()
    retry = Retry(total=2, backoff_factor=1.0, status_forcelist=(429, 500, 502, 503, 504), allowed_methods=("GET",))
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    if base_headers:
        s.headers.update(base_headers)
    return s

# =========================
# RANKING FETCHER (Top-Down Logic)
# =========================
def fetch_top_volume_tickers(diag):
    tickers = []
    session = make_retry_session(base_headers=get_base_headers())
    
    try:
        r = session.get("https://tw.stock.yahoo.com/rank/volume?exchange=ALL", timeout=8, verify=True)
        tks = re.findall(r'/quote/([0-9]{4})', r.text)
        if tks:
            tickers.extend(tks)
            diag["rank_src"] = "Yahoo 股市"
    except Exception as e:
        diag_err(diag, e, "RANK_YAHOO_FAIL")

    if len(set(tickers)) < 30:
        try:
            r = session.get("https://www.wantgoo.com/stock/ranking/volume", timeout=8, verify=True)
            tks = re.findall(r'/stock/([0-9]{4})', r.text)
            if tks:
                tickers.extend(tks)
                diag["rank_src"] = "玩股網 WantGoo"
        except Exception as e:
            diag_err(diag, e, "RANK_WANTGOO_FAIL")

    seen = set()
    final_tks = []
    for t in tickers:
        if t not in seen and len(t) == 4:
            seen.add(t)
            final_tks.append(t)

    return final_tks[:50]

# =========================
# DATA FETCHING
# =========================
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_text(url: str):
    s = make_retry_session(base_headers=get_base_headers())
    r = s.get(url, timeout=(3.0, 15.0), verify=False)
    r.raise_for_status()
    return r.text.replace("\r", "")

def get_stock_list():
    meta, errors = {}, []
    urls = [("tse", "https://raw.githubusercontent.com/mlouielu/twstock/master/twstock/codes/twse_equities.csv"),
            ("otc", "https://raw.githubusercontent.com/mlouielu/twstock/master/twstock/codes/tpex_equities.csv")]
    for ex, url in urls:
        try:
            text = fetch_text(url)
            df = pd.read_csv(io.StringIO(text), dtype=str, engine="python", on_bad_lines="skip")
            col_map = {c.strip().lower(): c for c in df.columns}
            c_col, n_col, t_col = col_map.get('code') or df.columns[1], col_map.get('name') or df.columns[2], col_map.get('type')
            for _, row in df.iterrows():
                stype = str(row.get(t_col, "")) if t_col else ""
                if t_col and ("權證" in stype or "ETF" in stype): continue
                code = str(row[c_col]).strip()
                if len(code) == 4 and code.isdigit(): meta[code] = {"name": str(row[n_col]), "ex": ex}
        except Exception as e:
            if not isinstance(e, pd.errors.ParserError):
                errors.append(f"{ex} - {str(e)}")
    return meta, errors

@st.cache_data(ttl=6*3600, show_spinner=False)
def yf_download_daily(syms):
    if not syms: return None
    df = yf.download(tickers=" ".join(syms), period="120d", interval="1d", group_by="ticker", auto_adjust=False, threads=True, progress=False)
    if df is None or getattr(df, "empty", False): return df
    if not isinstance(df.columns, pd.MultiIndex):
        t = syms[0]
        df.columns = pd.MultiIndex.from_product([[t], df.columns])
    df = df[~df.index.duplicated(keep="last")]
    df = df.sort_index()
    return df

# =========================
# HELPERS
# =========================
def now_taipei(): return datetime.utcnow() + timedelta(hours=8)
def idx_date_taipei(idx):
    try:
        if getattr(idx, "tz", None) is not None: return idx.tz_convert("Asia/Taipei").date
    except: pass
    return idx.date
def tw_tick(price): return 0.01 if price < 10 else 0.05 if price < 50 else 0.1 if price < 100 else 0.5 if price < 500 else 1.0 if price < 1000 else 5.0
def calc_limit_up(prev_close, limit_pct=0.10):
    raw = prev_close * (1.0 + limit_pct)
    tick = tw_tick(raw)
    n = math.floor((raw + 1e-12) / tick)
    return round(n * tick, 2 if tick < 0.1 else 1 if tick < 1 else 0)

# =========================
# API ENGINE (RAW FETCH ONLY)
# =========================
# ✅ 修正 1：純粹抓取資料，不做任何條件過濾，把完整資料還原保留
def fast_fugle_scan_raw(meta_dict, status_placeholder, diag, target_tickers):
    session = make_retry_session()
    headers = {"X-API-KEY": FUGLE_API_KEY}
    rows = []
    
    for idx, c in enumerate(target_tickers):
        if c not in meta_dict: continue
        
        url = f"https://api.fugle.tw/marketdata/v1.0/stock/intraday/quote/{c}"
        status_placeholder.update(label=f"💎 富果 VIP 專線直通中... ({idx+1}/{len(target_tickers)} 檔)", state="running")
        diag["fugle_seen"] += 1
        
        try:
            r = session.get(url, headers=headers, timeout=API_TIMEOUT)
            if r.status_code != 200:
                diag["fugle_req_err"] += 1
                diag["fugle_http_err"] = diag.get("fugle_http_err", 0) + 1
                diag_err(diag, Exception(f"HTTP_{r.status_code} for {c}"), "FUGLE_HTTP")
                time.sleep(0.2)
                continue
                
            data = r.json()
            ref_price = data.get("referencePrice", 0)
            last = data.get("closePrice", ref_price)
            high = data.get("highPrice", last)
            low = data.get("lowPrice", last)
            vol_shares = data.get("total", {}).get("tradeVolume", 0)
            
            if ref_price <= 0 or last <= 0:
                diag["fugle_parse_fail"] += 1
                continue
                
            upper = calc_limit_up(ref_price)
            dist_pct = max(0.0, ((upper - last) / upper) * 100)
            
            bids = data.get("bids", [])
            best_bid = bids[0].get("price", 0) if bids else 0.0
            bid_sh1 = bids[0].get("size", 0) if bids else 0.0 
            
            diag["fugle_parse_ok"] += 1
            
            # 不管價格、量多少，全部存起來備用！
            rows.append({
                "code": c, "last": last, "upper": upper, "dist": dist_pct, 
                "vol_sh": vol_shares, "prev_close": ref_price,
                "high": high, "low": low, "best_bid": best_bid, "bid_sh1": bid_sh1
            })
                
        except Exception as e:
            diag["fugle_req_err"] += 1
            diag_err(diag, e, "FUGLE_REQ_FAIL")
            
        time.sleep(0.1)

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["dist", "vol_sh"], ascending=[True, False]).drop_duplicates("code", keep="first")
    diag["fugle_rows"] = len(df)
    return df

# =========================
# DYNAMIC FILTER ENGINE
# =========================
# ✅ 修正 2：將過濾邏輯獨立出來，套用開關條件，實現毫秒級瞬切
def apply_dynamic_filters(raw_df, meta_dict, now_ts, is_test, use_bloodline, base_diag):
    diag = base_diag.copy() # 複製一份診斷數據，避免切換時數據重複疊加
    stats = {"Total": 0, "爆量不足": [], "回落過大": [], "收盤太弱": [], "非連板標的": []}
    
    # 初始化 YF 監控數據
    diag["yf_symbols"] = 0; diag["yf_fail"] = 0; diag["other_err"] = 0
    diag["yf_bulk_fail"] = 0; diag["yf_rescue_used"] = 0; diag["yf_returned"] = 0
    diag["yf_parts_ok"] = 0; diag["yf_parts_fail"] = 0
    
    if raw_df.empty: return pd.DataFrame(), stats, diag

    # 套用即時開關門檻
    m = int((datetime.combine(now_ts.date(), now_ts.time()) - datetime.combine(now_ts.date(), dtime(9, 0))).total_seconds() // 60)
    m = max(0, min(270, m)) 
    dist_limit = 100.0 if is_test else (3.1 if m <= 60 else 2.2 if m <= 180 else 1.5)
    vol_limit = 0 if is_test else 800_000 
    
    # 執行第一階段瞬切過濾
    candidates_df = raw_df[(raw_df['dist'] <= dist_limit) & (raw_df['vol_sh'] >= vol_limit)].copy()
    candidates_df = candidates_df.head(50)
    stats["Total"] = len(candidates_df)
    
    if candidates_df.empty: return pd.DataFrame(), stats, diag

    syms = [f"{c}.{'TW' if meta_dict[c]['ex']=='tse' else 'TWO'}" for c in candidates_df["code"]]
    diag["yf_symbols"] = len(syms)
    
    t_yf_start = time.perf_counter()
    raw_daily = None
    
    def try_yf_parts(parts):
        res_frames = []
        for part in parts:
            if not part: res_frames.append(None); continue
            try: res_frames.append(yf_download_daily(part))
            except Exception as e:
                diag_err(diag, e, "YF_PART_FAIL")
                res_frames.append(None)
        return res_frames

    # YF 資料因為有 st.cache_data 緩存，瞬切時不會重新下載，而是秒回傳！
    try:
        raw_daily = yf_download_daily(syms)
        if raw_daily is None or getattr(raw_daily, "empty", False): raise Exception("YF_BULK_EMPTY")
    except Exception as e:
        tag = "YF_BULK_EMPTY" if str(e) == "YF_BULK_EMPTY" else "YF_BULK_FAIL"
        diag_err(diag, e, tag)
        diag["yf_bulk_fail"] = diag.get("yf_bulk_fail", 0) + 1
        diag["yf_rescue_used"] = 1
        
        mid = max(1, len(syms)//2)
        parts1 = [syms[:mid], syms[mid:]]
        frames1 = try_yf_parts(parts1)
        
        diag["yf_parts_ok"] = diag.get("yf_parts_ok", 0) + sum(1 for f in frames1 if f is not None and not getattr(f, "empty", False))
        diag["yf_parts_fail"] = diag.get("yf_parts_fail", 0) + sum(1 for f in frames1 if f is None or getattr(f, "empty", False))
        frames_ok = [f for f in frames1 if f is not None and not getattr(f, "empty", False)]
        
        if len(frames_ok) < 2:
            parts2 = []
            for i, f in enumerate(frames1):
                if f is None or getattr(f, "empty", False):
                    p = parts1[i]
                    if not p: continue 
                    if len(p) > 1:
                        m2 = len(p)//2
                        parts2.extend([p[:m2], p[m2:]])
                    else: parts2.append(p)
            if parts2:
                frames2 = try_yf_parts(parts2)
                diag["yf_parts_ok"] += sum(1 for f in frames2 if f is not None and not getattr(f, "empty", False))
                diag["yf_parts_fail"] += sum(1 for f in frames2 if f is None or getattr(f, "empty", False))
                frames_ok.extend([f for f in frames2 if f is not None and not getattr(f, "empty", False)])

        if frames_ok: 
            raw_daily = pd.concat(frames_ok, axis=1)
            if raw_daily is not None and not isinstance(raw_daily.columns, pd.MultiIndex):
                fallback_t = syms[0]
                try:
                    for f in frames_ok:
                        if f is not None and isinstance(getattr(f, "columns", None), pd.MultiIndex):
                            fallback_t = f.columns.get_level_values(0)[0]; break
                except: pass
                raw_daily.columns = pd.MultiIndex.from_product([[fallback_t], raw_daily.columns])
            if isinstance(raw_daily.columns, pd.MultiIndex):
                raw_daily = raw_daily.loc[:, ~raw_daily.columns.duplicated()]
            raw_daily = raw_daily[~raw_daily.index.duplicated(keep="last")]
            raw_daily = raw_daily.sort_index()

    diag["t_yf"] = time.perf_counter() - t_yf_start
    if raw_daily is None or getattr(raw_daily, "empty", False):
        diag["other_err"] += 1; return pd.DataFrame(), stats, diag

    if isinstance(raw_daily.columns, pd.MultiIndex): diag["yf_returned"] = int(raw_daily.columns.get_level_values(0).nunique())
    else: diag["yf_returned"] = 1

    results, today_date = [], now_ts.date()
    
    frac = 0.0 if is_test else (0.12 if m <= 30 else 0.12 + (0.5 - 0.12) * ((m - 30) / 90.0) if m <= 120 else min(1.0, 0.5 + (1.0 - 0.5) * ((m - 120) / 150.0)))
    pb_lim = 1.0 if is_test else (0.012 if m <= 90 else 0.0039)

    for _, r in candidates_df.iterrows():
        c, name = r["code"], meta_dict[r["code"]]["name"]
        sym = f"{c}.{'TW' if meta_dict[c]['ex']=='tse' else 'TWO'}"
        try:
            if isinstance(raw_daily.columns, pd.MultiIndex):
                if sym not in raw_daily.columns.get_level_values(0): diag["yf_fail"] += 1; continue
                df_sym = raw_daily[sym]
            else: df_sym = raw_daily
            
            if not {"Close", "Volume"}.issubset(set(df_sym.columns)): diag["yf_fail"] += 1; continue
            dfD = df_sym[["Close", "Volume"]].dropna()
            if len(dfD) < 30: diag["yf_fail"] += 1; continue
            
            dates_tw = idx_date_taipei(dfD.index)
            past_df = dfD[dates_tw < today_date].copy()
            if len(past_df) < 30: diag["yf_fail"] += 1; continue
            
            vol_ma20_sh = float(past_df["Volume"].rolling(20).mean().iloc[-1])
            if (not math.isfinite(vol_ma20_sh)) or vol_ma20_sh <= 0: diag["yf_fail"] += 1; continue

            past_boards, past_10 = 0, past_df.tail(10)
            for i in range(len(past_10)-1, 0, -1):
                cp, pp = float(past_10["Close"].iloc[i]), float(past_10["Close"].iloc[i-1])
                lim = calc_limit_up(pp)
                if cp >= (lim - tw_tick(lim)): past_boards += 1
                else: break

            if use_bloodline and (not is_test) and past_boards < 1:
                stats["非連板標的"].append(f"{c} {name}"); continue

            is_locked = (r["best_bid"] >= r["upper"] - tw_tick(r["upper"])) and (r["bid_sh1"] >= (80000 if r["last"]<50 else 120000 if r["last"]<100 else 200000))
            vol_ratio = r["vol_sh"] / (vol_ma20_sh * frac + 1e-9)
            
            if vol_ratio < (0.0 if is_test else 1.3): stats["爆量不足"].append(f"{c} {name}"); continue
            
            rng = r["high"] - r["low"]
            if (r["high"] - r["last"]) / max(1e-9, r["high"]) > pb_lim: stats["回落過大"].append(f"{c} {name}"); continue
            if (r["last"] - r["low"]) / max(1e-9, rng) < (0.0 if is_test else 0.80) and rng > 0.1: stats["收盤太弱"].append(f"{c} {name}"); continue

            results.append({"代號": c, "名稱": name, "現價": r["last"], "爆量": vol_ratio, "狀態": "🔒 已鎖" if is_locked else "⚡ 發動", "階段": f"連續 {past_boards+1} 板", "board_val": past_boards})
        except Exception as e:
            diag["other_err"] += 1; diag_err(diag, e, "FILTER")
            
    res_df = pd.DataFrame(results)
    if not res_df.empty: res_df = res_df.sort_values(["board_val", "爆量"], ascending=[False, False])
    return res_df, stats, diag

# =========================
# UI / MAIN EXECUTION
# =========================
st.set_page_config(page_title="起漲戰情室 Ultra", page_icon="⚡", layout="wide", initial_sidebar_state="collapsed")
st.markdown("""
<style>
    [data-testid="stAppViewContainer"], .main { background: #050505 !important; background-image: radial-gradient(circle at 15% 50%, rgba(20, 20, 20, 1), transparent 25%), radial-gradient(circle at 85% 30%, rgba(10, 25, 40, 0.8), transparent 25%) !important; color: #e2e8f0 !important; }
    .block-container { padding-top: 2rem; max-width: 1280px; }
    [data-testid="stSidebar"] { display: none !important; }
    .title { font-size: 58px; font-weight: 900; letter-spacing: -2px; background: linear-gradient(135deg, #ffffff 0%, #718096 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; text-align: center; margin-bottom: 5px; }
    .status-caption { color: #64748b; font-size: 13px; text-align: center; margin-bottom: 30px; letter-spacing: 1px;}
    .pro-card { background: linear-gradient(145deg, rgba(22, 24, 29, 0.9), rgba(13, 15, 18, 0.9)); backdrop-filter: blur(24px); border: 1px solid rgba(255, 255, 255, 0.05); border-top: 1px solid rgba(255, 255, 255, 0.1); border-radius: 20px; padding: 24px; margin-bottom: 16px; transition: all 0.4s cubic-bezier(0.2, 0.8, 0.2, 1); box-shadow: 0 10px 30px -10px rgba(0,0,0,0.5); }
    .pro-card:hover { border-color: rgba(56, 189, 248, 0.4); transform: translateY(-5px) scale(1.01); box-shadow: 0 20px 40px -10px rgba(56, 189, 248, 0.15); }
    .stock-name { font-size: 22px; font-weight: 800; color: #f8fafc; letter-spacing: 1px;}
    .price-large { font-size: 36px; font-weight: 900; color: #ffffff; font-variant-numeric: tabular-nums; text-shadow: 0 2px 10px rgba(255,255,255,0.1);}
    .tag-pro { padding: 5px 14px; border-radius: 6px; font-size: 11px; font-weight: 800; background: rgba(56, 189, 248, 0.1); color: #38bdf8; border: 1px solid rgba(56, 189, 248, 0.2); letter-spacing: 1px;}
    .fail-tag { display: inline-block; padding: 6px 12px; background: rgba(244, 63, 94, 0.05); color: #f43f5e; border-radius: 8px; margin: 4px; font-size: 12px; border: 1px solid rgba(244, 63, 94, 0.15); font-weight: 600;}
    .stButton>button { border-radius: 16px !important; background: linear-gradient(135deg, #f8fafc 0%, #cbd5e1 100%) !important; color: #0f172a !important; font-weight: 900 !important; padding: 20px !important; width: 100% !important; border: none !important; font-size: 18px !important; letter-spacing: 2px !important; box-shadow: 0 4px 15px rgba(255,255,255,0.1) !important; transition: all 0.3s ease !important; }
    .stButton>button:hover { transform: translateY(-2px); box-shadow: 0 8px 25px rgba(255,255,255,0.2) !important; }
    [data-testid="stMetric"] { background: rgba(20,20,20,0.6); padding: 15px; border-radius: 16px; border: 1px solid rgba(255,255,255,0.03); }
    [data-testid="stMetricValue"] { font-size: 32px !important; font-weight: 900 !important; color: #f1f5f9 !important; }
    [data-testid="stMetricLabel"] { font-size: 13px !important; color: #94a3b8 !important; font-weight: 600 !important; letter-spacing: 1px; }
    [data-testid="stExpander"] { background: transparent !important; border: 1px solid rgba(255,255,255,0.05) !important; border-radius: 16px !important; }
    [data-testid="stExpander"] summary { background: rgba(20,20,20,0.4) !important; border-radius: 16px !important; }
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="title">起漲戰情室 ULTRA</div>', unsafe_allow_html=True)
st.markdown('<div class="status-caption">量化交易終端機 v10.2 毫秒瞬切版</div>', unsafe_allow_html=True)

col_cfg = st.columns([1.2, 1.2, 1, 1])
with col_cfg[0]: is_test = st.toggle("🔥 寬鬆測試模式 (無底線顯示)", value=False)
with col_cfg[1]: use_bloodline = st.toggle("🛡️ 嚴格連板血統", value=True)

now_time = time.time()
last_run = st.session_state.get("last_run_ts", 0)
cooldown_seconds = 60

# =========================
# FETCH PHASE (Network calls only)
# =========================
if st.button("🚀 啟動熱門資金狙擊 (富果專線)"):
    if now_time - last_run < cooldown_seconds:
        st.warning(f"⏳ 保護 API 額度中，請等待 {int(cooldown_seconds - (now_time - last_run))} 秒後再發動狙擊...")
    else:
        st.session_state["last_run_ts"] = now_time
        t0, base_diag = time.perf_counter(), diag_init()
        
        with st.status("⚡ 鎖定市場熱點資金中 (抓取原始資料)...", expanded=True) as status:
            t = time.perf_counter(); meta, meta_errs = get_stock_list()
            base_diag["t_meta"] = time.perf_counter() - t; base_diag["meta_count"] = len(meta)
            for err in meta_errs: diag_err(base_diag, Exception(err), "META_ERR")
            
            t = time.perf_counter()
            status.update(label="🔥 攔截市場成交量排行榜...", state="running")
            top_tickers = fetch_top_volume_tickers(base_diag)
            base_diag["t_rank"] = time.perf_counter() - t
            base_diag["rank_count"] = len(top_tickers)
            
            if not top_tickers:
                st.error("🚨 無法取得排行榜資料，請稍後再試。")
                st.stop()

            filtered_meta = {k: v for k, v in meta.items() if k in top_tickers}

            t = time.perf_counter(); now_ts = now_taipei()
            # 抓取「無任何過濾」的原始富果資料
            raw_fugle_df = fast_fugle_scan_raw(filtered_meta, status, base_diag, top_tickers)
            base_diag["t_api"] = time.perf_counter() - t
            base_diag["total_fetch_time"] = time.perf_counter() - t0
            status.update(label="✅ 資料快取完成！", state="complete")
            
        # 將原始資料存入保險箱
        st.session_state["raw_data_vault"] = {
            "df": raw_fugle_df,
            "meta": meta,
            "ts": now_ts,
            "base_diag": base_diag
        }

# =========================
# FILTER & RENDER PHASE (Instant)
# =========================
if "raw_data_vault" in st.session_state:
    vault = st.session_state["raw_data_vault"]
    
    # 執行瞬間過濾 (0網路請求)
    t_filter_start = time.perf_counter()
    res, sts, final_diag = apply_dynamic_filters(
        raw_df=vault["df"], 
        meta_dict=vault["meta"], 
        now_ts=vault["ts"], 
        is_test=is_test, 
        use_bloodline=use_bloodline, 
        base_diag=vault["base_diag"]
    )
    final_diag["t_filter"] = time.perf_counter() - t_filter_start
    total_time = final_diag.get("total_fetch_time", 0) + final_diag["t_filter"]
    
    # 顯示結果
    ts = vault["ts"]
    t_str = f"測試: {'ON' if is_test else 'OFF'} | 血統: {'ON' if use_bloodline else 'OFF'}"
    st.markdown(f'<div class="status-caption">資料時間：{ts.strftime("%H:%M:%S")} | {t_str} | 濾網運算耗時：{final_diag["t_filter"]:.3f}s</div>', unsafe_allow_html=True)
    
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("排行榜捕捉數量", f"{final_diag.get('rank_count', 0)} 檔", f"來源: {final_diag.get('rank_src', '未知')}")
    m2.metric("嚴選錄取檔數", len(res))
    total_parse = final_diag.get("fugle_parse_ok", 0) + final_diag.get("fugle_parse_fail", 0)
    m3.metric("API 解析良率", f"{(final_diag.get('fugle_parse_ok', 0)/max(1,total_parse)*100):.1f}%")
    m4.metric("系統異常阻擋", final_diag.get("fugle_req_err",0) + final_diag.get("yf_fail",0) + final_diag.get("other_err",0))

    with st.expander("⚙️ 系統診斷與底層監控 (白盒分析)", expanded=False):
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Meta 總檔數", final_diag.get("meta_count"))
        c2.metric("富果 API 有效", final_diag.get("fugle_parse_ok"))
        st.caption(f"🛡️ 富果探針：HTTP 錯誤 {final_diag.get('fugle_http_err',0)}")
        c3.metric("YF 數據覆蓋", f"{final_diag.get('yf_returned',0)} / {final_diag.get('yf_symbols',0)}")
        rescue_msg = f"{'🟢 啟動' if final_diag.get('yf_rescue_used', 0) else '⚪ 待命'} | ERR {final_diag.get('other_err',0)}"
        c4.metric("救援協議 / 錯誤", rescue_msg)
        if final_diag.get('yf_rescue_used', 0):
            st.caption(f"⚠️ 細胞分裂救援：成功 {final_diag.get('yf_parts_ok', 0)} 塊 / 失敗 {final_diag.get('yf_parts_fail', 0)} 塊")
            
        st.caption(f"耗時分布：Meta {final_diag['t_meta']:.2f}s | 榜單 {final_diag['t_rank']:.2f}s | 富果 API {final_diag['t_api']:.2f}s | 濾網瞬切 {final_diag['t_filter']:.3f}s")
        if final_diag.get("last_errors"): st.code("\n".join(final_diag["last_errors"]))

    with st.expander("🎯 戰損與淘汰名單 (實名點名)", expanded=True):
        for reason, stocks in sts.items():
            if isinstance(stocks, list) and stocks:
                st.markdown(f"**{reason}**")
                st.markdown(f'<div>' + "".join([f'<span class="fail-tag">{s}</span>' for s in stocks]) + '</div>', unsafe_allow_html=True)

    if not res.empty:
        st.markdown("<br>", unsafe_allow_html=True)
        cols = st.columns(4)
        for i, r in res.iterrows():
            with cols[i % 4]:
                st.markdown(f"""<div class="pro-card">
                    <div class="tag-pro">{r['階段']}</div>
                    <div class="stock-name">{r['代號']} {r['名稱']}</div>
                    <div style="height:12px;"></div>
                    <div class="price-large">{r['現價']:.2f}</div>
                    <div style="font-size:13px; color:#94a3b8; margin-top:12px; font-weight:600;">
                        {r['狀態']} | 動能 {r['爆量']:.1f}x
                    </div>
                </div>""", unsafe_allow_html=True)
    else: 
        if final_diag.get("fugle_parse_ok", 0) == 0:
            st.error("🚨 嚴重警告：無法連接到富果 API。請確認您的授權金鑰是否有效。")
        else:
            st.warning("⚠️ 目前無標的通過您當前設定的濾網條件，請嘗試切換「寬鬆測試模式」。")
