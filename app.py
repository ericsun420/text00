# app.py — 起漲戰情室｜戰神 7.1 終極破壁版｜全中文專業介面｜Ultra Pro
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

# 隱藏 SSL 警告，保持版面乾淨
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================
# SYSTEM DIAGNOSTICS & UTILS
# =========================
def diag_init():
    return {
        "meta_count": 0, "cand_total": 0, "mis_req_err": 0,
        "mis_seen": 0, "mis_parse_ok": 0, "mis_parse_fail": 0, "mis_rows": 0,
        "yf_symbols": 0, "yf_returned": 0, "yf_fail": 0, "other_err": 0,
        "yf_bulk_fail": 0, "yf_rescue_used": 0,
        "yf_parts_ok": 0, "yf_parts_fail": 0,
        "last_errors": deque(maxlen=5),
        "t_meta": 0.0, "t_mis": 0.0, "t_yf": 0.0, "t_filter": 0.0, "total": 0.0
    }

def diag_err(diag, e, tag="ERR"):
    diag["last_errors"].append(f"[{tag}] {type(e).__name__}: {e}")

def make_retry_session():
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=0.4, status_forcelist=(429, 500, 502, 503, 504), allowed_methods=("GET",), respect_retry_after_header=True)
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

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
# UI / THEME (Ultra Pro Design)
# =========================
st.set_page_config(page_title="起漲戰情室 Ultra", page_icon="⚡", layout="wide", initial_sidebar_state="collapsed")
st.markdown("""
<style>
    /* 深邃宇宙黑背景 */
    [data-testid="stAppViewContainer"], .main { 
        background: #050505 !important; 
        background-image: radial-gradient(circle at 15% 50%, rgba(20, 20, 20, 1), transparent 25%), radial-gradient(circle at 85% 30%, rgba(10, 25, 40, 0.8), transparent 25%) !important;
        color: #e2e8f0 !important; 
    }
    .block-container { padding-top: 2rem; max-width: 1280px; }
    [data-testid="stSidebar"] { display: none !important; }
    
    /* 頂部標題 */
    .title { font-size: 58px; font-weight: 900; letter-spacing: -2px; background: linear-gradient(135deg, #ffffff 0%, #718096 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; text-align: center; margin-bottom: 5px; }
    .status-caption { color: #64748b; font-size: 13px; text-align: center; margin-bottom: 30px; letter-spacing: 1px;}
    
    /* 奢華流光卡片 */
    .pro-card { 
        background: linear-gradient(145deg, rgba(22, 24, 29, 0.9), rgba(13, 15, 18, 0.9)); 
        backdrop-filter: blur(24px); 
        border: 1px solid rgba(255, 255, 255, 0.05); 
        border-top: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 20px; 
        padding: 24px; 
        margin-bottom: 16px; 
        transition: all 0.4s cubic-bezier(0.2, 0.8, 0.2, 1);
        box-shadow: 0 10px 30px -10px rgba(0,0,0,0.5);
    }
    .pro-card:hover { 
        border-color: rgba(56, 189, 248, 0.4); 
        transform: translateY(-5px) scale(1.01); 
        box-shadow: 0 20px 40px -10px rgba(56, 189, 248, 0.15);
    }
    
    /* 卡片內文字 */
    .stock-name { font-size: 22px; font-weight: 800; color: #f8fafc; letter-spacing: 1px;}
    .price-large { font-size: 36px; font-weight: 900; color: #ffffff; font-variant-numeric: tabular-nums; text-shadow: 0 2px 10px rgba(255,255,255,0.1);}
    .tag-pro { padding: 5px 14px; border-radius: 6px; font-size: 11px; font-weight: 800; background: rgba(56, 189, 248, 0.1); color: #38bdf8; border: 1px solid rgba(56, 189, 248, 0.2); letter-spacing: 1px;}
    .fail-tag { display: inline-block; padding: 6px 12px; background: rgba(244, 63, 94, 0.05); color: #f43f5e; border-radius: 8px; margin: 4px; font-size: 12px; border: 1px solid rgba(244, 63, 94, 0.15); font-weight: 600;}
    
    /* 頂級按鈕 */
    .stButton>button { 
        border-radius: 16px !important; 
        background: linear-gradient(135deg, #f8fafc 0%, #cbd5e1 100%) !important; 
        color: #0f172a !important; 
        font-weight: 900 !important; 
        padding: 20px !important; 
        width: 100% !important; 
        border: none !important;
        font-size: 18px !important;
        letter-spacing: 2px !important;
        box-shadow: 0 4px 15px rgba(255,255,255,0.1) !important;
        transition: all 0.3s ease !important;
    }
    .stButton>button:hover { transform: translateY(-2px); box-shadow: 0 8px 25px rgba(255,255,255,0.2) !important; }
    
    /* 美化 Streamlit 預設 Metric */
    [data-testid="stMetric"] { background: rgba(20,20,20,0.6); padding: 15px; border-radius: 16px; border: 1px solid rgba(255,255,255,0.03); }
    [data-testid="stMetricValue"] { font-size: 32px !important; font-weight: 900 !important; color: #f1f5f9 !important; }
    [data-testid="stMetricLabel"] { font-size: 13px !important; color: #94a3b8 !important; font-weight: 600 !important; letter-spacing: 1px; }
    
    /* Expander 美化 */
    [data-testid="stExpander"] { background: transparent !important; border: 1px solid rgba(255,255,255,0.05) !important; border-radius: 16px !important; }
    [data-testid="stExpander"] summary { background: rgba(20,20,20,0.4) !important; border-radius: 16px !important; }
</style>
""", unsafe_allow_html=True)

# =========================
# HELPERS
# =========================
def now_taipei(): return datetime.utcnow() + timedelta(hours=8)

def idx_date_taipei(idx):
    try:
        if getattr(idx, "tz", None) is not None:
            try: return idx.tz_convert("Asia/Taipei").date
            except: return idx.tz_localize(None).date
    except: pass
    return idx.date

def tw_tick(price):
    return 0.01 if price < 10 else 0.05 if price < 50 else 0.1 if price < 100 else 0.5 if price < 500 else 1.0 if price < 1000 else 5.0

def calc_limit_up(prev_close, limit_pct=0.10):
    raw = prev_close * (1.0 + limit_pct)
    tick = tw_tick(raw)
    n = math.floor((raw + 1e-12) / tick)
    return round(n * tick, 2 if tick < 0.1 else 1 if tick < 1 else 0)

def infer_daily_limit(pp, cp):
    l10 = calc_limit_up(pp, 0.10); l20 = calc_limit_up(pp, 0.20)
    tol20 = max(tw_tick(l20), l20 * 0.0005) 
    if abs(cp - l20) <= tol20 and abs(cp - l20) < abs(cp - l10): return l20
    return l10

def split_nums(s):
    out = []
    for x in str(s or "").split("_"):
        x = x.strip()
        if not x or x in ("-", "—"): continue
        try: out.append(float(x))
        except: pass
    return out

# =========================
# ENGINES
# =========================
@st.cache_data(ttl=3600, show_spinner=False)
def get_stock_list():
    meta, session = {}, make_retry_session()
    urls = [("tse", "https://raw.githubusercontent.com/mlouielu/twstock/master/twstock/codes/twse_equities.csv"),
            ("otc", "https://raw.githubusercontent.com/mlouielu/twstock/master/twstock/codes/tpex_equities.csv")]
    for ex, url in urls:
        try:
            r = session.get(url, timeout=15, verify=False); r.raise_for_status()
            df = pd.read_csv(io.StringIO(r.text.replace("\r", "")), dtype=str, engine="python", on_bad_lines="skip")
            col_map = {c.strip().lower(): c for c in df.columns}
            c_col, n_col, t_col = col_map.get('code') or df.columns[1], col_map.get('name') or df.columns[2], col_map.get('type')
            for _, row in df.iterrows():
                stype = str(row.get(t_col, "")) if t_col else ""
                if t_col and ("權證" in stype or "ETF" in stype): continue
                code = str(row[c_col]).strip()
                if len(code) == 4 and code.isdigit(): meta[code] = {"name": str(row[n_col]), "ex": ex}
        except: pass
    return meta

def fast_mis_scan(meta_dict, status_placeholder, now_ts, is_test, diag):
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://mis.twse.com.tw/stock/fibest.jsp?lang=zh_tw"}
    session, rows, err_mis = make_retry_session(), [], 0
    mis_diag = {"mis_req_err": 0, "mis_seen": 0, "mis_parse_ok": 0, "mis_parse_fail": 0, "mis_rows": 0}
    
    try: session.get("https://mis.twse.com.tw/stock/fibest.jsp?lang=zh_tw", headers=headers, timeout=10, verify=False)
    except Exception as e: diag_err(diag, e, "MIS_WARMUP")

    m = int((datetime.combine(now_ts.date(), now_ts.time()) - datetime.combine(now_ts.date(), dtime(9, 0))).total_seconds() // 60)
    m = max(0, min(270, m)) 
    dist_limit = 5.0 if is_test else (3.1 if m <= 60 else 2.2 if m <= 180 else 1.5)
    vol_limit = 200 if is_test else 800
    
    codes = list(meta_dict.keys())
    for i in range(0, len(codes), 80):
        chunk = codes[i:i+80]
        ex_ch = "%7c".join([f"{meta_dict[c]['ex']}_{c}.tw" for c in chunk])
        url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={ex_ch}&json=1&delay=0&_={int(time.time()*1000)}"
        status_placeholder.update(label=f"📡 穿透雷達掃描中... ({i}/{len(codes)})", state="running")
        try:
            r = session.get(url, headers=headers, timeout=12, verify=False)
            data = r.json().get("msgArray", [])
        except:
            err_mis += 1; mis_diag["mis_req_err"] += 1; time.sleep(0.1); continue

        for q in data:
            mis_diag["mis_seen"] += 1
            c = q.get("c")
            if not c or c not in meta_dict: continue 
            try:
                z, u, v, y = q.get("z"), u=q.get("u"), v=q.get("v"), y=q.get("y")
                if z in (None, "", "-", "—") or u in (None, "", "-", "—") or y in (None, "", "-", "—", "0"):
                    mis_diag["mis_parse_fail"] += 1; continue
                last, upper, prev_close, vol_sh = float(z), float(u), float(y), float(v or 0)
                if upper <= 0 or prev_close <= 0: mis_diag["mis_parse_fail"] += 1; continue
                mis_diag["mis_parse_ok"] += 1
                dist_pct = max(0.0, ((upper - last) / upper) * 100)
                if (vol_sh / 1000) >= vol_limit and dist_pct <= dist_limit:
                    bp, bv = split_nums(q.get("b")), split_nums(q.get("g"))
                    rows.append({
                        "code": c, "last": last, "upper": upper, "dist": dist_pct, "vol_sh": vol_sh, "prev_close": prev_close,
                        "high": float(q.get("h") if q.get("h") not in (None,"","-","—") else last),
                        "low":  float(q.get("l") if q.get("l") not in (None,"","-","—") else last),
                        "best_bid": bp[0] if bp else 0.0, "bid_sh1": bv[0] if bv else 0.0
                    })
            except: mis_diag["mis_parse_fail"] += 1
        time.sleep(0.12 if not is_test else 0.01)

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["dist", "vol_sh"], ascending=[True, False]).drop_duplicates("code", keep="first")
    mis_diag["mis_rows"] = len(df)
    return df, err_mis, mis_diag

def core_filter_engine(candidates_df, meta_dict, now_ts, is_test, diag, use_bloodline):
    stats = {"Total": 0, "爆量不足": [], "回落過大": [], "收盤太弱": [], "非連板標的": []}
    yf_diag = {"yf_symbols": 0, "yf_fail": 0, "other_err": 0}
    if candidates_df.empty: return pd.DataFrame(), stats, yf_diag
    
    candidates_df = candidates_df.sort_values(["dist", "vol_sh"], ascending=[True, False]).head(80)
    stats["Total"] = len(candidates_df)
    syms = [f"{c}.{'TW' if meta_dict[c]['ex']=='tse' else 'TWO'}" for c in candidates_df["code"]]
    yf_diag["yf_symbols"] = len(syms)
    
    t_yf_start = time.perf_counter()
    raw_daily = None
    
    def try_yf_parts(parts):
        res_frames = []
        for part in parts:
            try:
                if part: res_frames.append(yf_download_daily(part))
            except Exception as e:
                diag_err(diag, e, "YF_PART_FAIL")
                res_frames.append(None)
        return res_frames

    try:
        raw_daily = yf_download_daily(syms)
        if raw_daily is None or getattr(raw_daily, "empty", False):
            raise Exception("YF_BULK_EMPTY")
        diag["yf_rescue_used"] = 0 
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
                    if len(p) > 1:
                        m2 = len(p)//2
                        parts2.extend([p[:m2], p[m2:]])
                    else:
                        parts2.append(p)
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
        yf_diag["other_err"] += 1; return pd.DataFrame(), stats, yf_diag

    if isinstance(raw_daily.columns, pd.MultiIndex): diag["yf_returned"] = int(raw_daily.columns.get_level_values(0).nunique())
    else: diag["yf_returned"] = 1

    results, today_date = [], now_ts.date()
    m = int((datetime.combine(now_ts.date(), now_ts.time()) - datetime.combine(now_ts.date(), dtime(9, 0))).total_seconds() // 60)
    m = max(0, min(270, m)) 
    frac = 0.5 if is_test else (0.12 if m <= 30 else 0.12 + (0.5 - 0.12) * ((m - 30) / 90.0) if m <= 120 else min(1.0, 0.5 + (1.0 - 0.5) * ((m - 120) / 150.0)))
    pb_lim = 0.05 if is_test else (0.012 if m <= 90 else 0.0039)

    for _, r in candidates_df.iterrows():
        c, name = r["code"], meta_dict[r["code"]]["name"]
        sym = f"{c}.{'TW' if meta_dict[c]['ex']=='tse' else 'TWO'}"
        try:
            if isinstance(raw_daily.columns, pd.MultiIndex):
                if sym not in raw_daily.columns.get_level_values(0):
                    yf_diag["yf_fail"] += 1; continue
                df_sym = raw_daily[sym]
            else: df_sym = raw_daily
            
            if not {"Close", "Volume"}.issubset(set(df_sym.columns)):
                yf_diag["yf_fail"] += 1; continue
                
            dfD = df_sym[["Close", "Volume"]].dropna()
            if len(dfD) < 30: yf_diag["yf_fail"] += 1; continue
            
            dates_tw = idx_date_taipei(dfD.index)
            past_df = dfD[dates_tw < today_date].copy()
            if len(past_df) < 30: yf_diag["yf_fail"] += 1; continue
            
            vol_ma20_sh = float(past_df["Volume"].rolling(20).mean().iloc[-1])
            if (not math.isfinite(vol_ma20_sh)) or vol_ma20_sh <= 0:
                yf_diag["yf_fail"] += 1; continue

            past_boards, past_10 = 0, past_df.tail(10)
            for i in range(len(past_10)-1, 0, -1):
                cp, pp = float(past_10["Close"].iloc[i]), float(past_10["Close"].iloc[i-1])
                lim = infer_daily_limit(pp, cp)
                if cp >= (lim - tw_tick(lim)): past_boards += 1
                else: break

            if use_bloodline and (not is_test) and past_boards < 1:
                stats["非連板標的"].append(f"{c} {name}"); continue

            is_locked = (r["best_bid"] >= r["upper"] - tw_tick(r["upper"])) and (r["bid_sh1"] >= (80000 if r["last"]<50 else 120000 if r["last"]<100 else 200000))
            vol_ratio = r["vol_sh"] / (vol_ma20_sh * frac + 1e-9)
            if vol_ratio < (0.5 if is_test else 1.3): stats["爆量不足"].append(f"{c} {name}"); continue
            
            rng = r["high"] - r["low"]
            if (r["high"] - r["last"]) / max(1e-9, r["high"]) > pb_lim: stats["回落過大"].append(f"{c} {name}"); continue
            if (r["last"] - r["low"]) / max(1e-9, rng) < (0.5 if is_test else 0.80) and rng > 0.1: stats["收盤太弱"].append(f"{c} {name}"); continue

            results.append({"代號": c, "名稱": name, "現價": r["last"], "爆量": vol_ratio, "狀態": "🔒 已鎖" if is_locked else "⚡ 發動", "階段": f"連續 {past_boards+1} 板", "board_val": past_boards})
        except Exception as e:
            yf_diag["other_err"] += 1; diag_err(diag, e, "FILTER")
            
    res_df = pd.DataFrame(results)
    if not res_df.empty: res_df = res_df.sort_values(["board_val", "爆量"], ascending=[False, False])
    return res_df, stats, yf_diag

# =========================
# MAIN
# =========================
st.markdown('<div class="title">起漲戰情室 ULTRA</div>', unsafe_allow_html=True)
st.markdown('<div class="status-caption">量化交易終端機 v7.1</div>', unsafe_allow_html=True)

col_cfg = st.columns([1.2, 1.2, 1, 1])
with col_cfg[0]: is_test = st.toggle("🔥 寬鬆測試模式", value=False)
with col_cfg[1]: use_bloodline = st.toggle("🛡️ 嚴格連板血統", value=True)

if st.button("🚀 啟動全戰區量化掃描"):
    t0, diag = time.perf_counter(), diag_init()
    with st.status("⚡ 建立安全連線與解析市場中...", expanded=True) as status:
        t = time.perf_counter(); meta = get_stock_list()
        diag["t_meta"] = time.perf_counter() - t; diag["meta_count"] = len(meta)
        if len(meta) < 500: diag_err(diag, Exception(f"清單數量異常 ({len(meta)})"), "META_SUSPECT")
        
        t = time.perf_counter(); now_ts = now_taipei()
        pre_df, mis_err, mis_diag = fast_mis_scan(meta, status, now_ts, is_test, diag)
        diag["t_mis"] = time.perf_counter() - t; diag.update(mis_diag); diag["cand_total"] = mis_diag.get("mis_rows", len(pre_df))
        
        t = time.perf_counter()
        final_res, stats, yf_diag = core_filter_engine(pre_df, meta, now_ts, is_test, diag, use_bloodline)
        diag["t_filter"] = time.perf_counter() - t; diag.update(yf_diag)
        diag["total"] = time.perf_counter() - t0
        status.update(label="✅ 掃描完成", state="complete")
    st.session_state["last_scan"] = {"res": final_res, "stats": stats, "diag": diag, "ts": now_ts, "is_test": is_test, "use_bloodline": use_bloodline}

scan = st.session_state.get("last_scan")
if scan:
    d, res, sts, ts = scan["diag"], scan["res"], scan["stats"], scan["ts"]
    t_str = f"測試: {'ON' if scan['is_test'] else 'OFF'} | 血統: {'ON' if scan['use_bloodline'] else 'OFF'}"
    st.markdown(f'<div class="status-caption">上次更新：{ts.strftime("%H:%M:%S")} | {t_str} | 系統耗時：{d["total"]:.2f}s</div>', unsafe_allow_html=True)
    
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("初始候選標的", d.get("cand_total", 0))
    m2.metric("嚴選錄取檔數", len(res))
    total_parse = d.get("mis_parse_ok", 0) + d.get("mis_parse_fail", 0)
    m3.metric("資料解析良率", f"{(d.get('mis_parse_ok', 0)/max(1,total_parse)*100):.1f}%")
    m4.metric("系統異常阻擋", d.get("mis_req_err",0) + d.get("yf_fail",0) + d.get("other_err",0))

    with st.expander("⚙️ 系統診斷與底層監控", expanded=False):
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("全市場掃描", d.get("meta_count"))
        c2.metric("MIS 有效解析", d.get("mis_parse_ok"))
        c3.metric("YF 數據覆蓋", f"{d.get('yf_returned',0)} / {d.get('yf_symbols',0)}")
        rescue_msg = f"{'🟢 啟動' if d.get('yf_rescue_used', 0) else '⚪ 待命'} | ERR {d.get('other_err',0)}"
        c4.metric("救援協議 / 錯誤", rescue_msg)
        if d.get('yf_rescue_used', 0):
            st.caption(f"⚠️ 細胞分裂救援：成功 {d.get('yf_parts_ok', 0)} 塊 / 失敗 {d.get('yf_parts_fail', 0)} 塊")
        st.caption(f"耗時分布：Meta {d['t_meta']:.2f}s | MIS {d['t_mis']:.2f}s | YF {d.get('t_yf',0):.2f}s | Filter {d['t_filter']:.2f}s")
        if d.get("last_errors"): st.code("\n".join(d["last_errors"]))

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
        if d.get("mis_parse_ok", 0) == 0:
            st.error("🚨 嚴重警告：無法連接到證交所資料庫 (網路連線被阻擋)，請確認執行環境。")
        else:
            st.warning("⚠️ 掃描完畢。目前全市場無標的通過嚴格濾網。")
