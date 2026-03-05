# app.py — 倒裝極速版｜第一根漲停 + 連板潛力（1～8 濾網）｜冷酷黑灰｜一鍵暴力
import os
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
st.set_page_config(page_title="起漲戰情室｜極速掃描", page_icon="⚡", layout="wide")

CSS = """
<style>
:root{
  --bg:#07080b; --panel:#0b0d12; --card:#0f1116; --text:#e5e7eb; --muted:#9ca3af;
  --line:rgba(148,163,184,.16); --shadow: 0 16px 40px rgba(0,0,0,.35);
  --hi: rgba(148,163,184,.08); --ok:#a3e635; --warn:#fbbf24; --bad:#fb7185;
}
[data-testid="stAppViewContainer"]{ background: var(--bg) !important; color: var(--text) !important; }
.main{ background: var(--bg) !important; }
.block-container{ padding-top: 1.15rem; padding-bottom: 2.0rem; }
[data-testid="stHeader"]{ background: rgba(7,8,11,.80) !important; border-bottom: 1px solid var(--line) !important; }
[data-testid="stSidebar"]{ background: var(--panel) !important; border-right: 1px solid var(--line) !important; }
[data-testid="stSidebar"] *{ color: var(--text) !important; }
.header-wrap{ display:flex; align-items:flex-end; justify-content:space-between; gap:18px; padding: 6px 4px 2px 4px; }
.title{ font-size: 42px; font-weight: 900; letter-spacing: .4px; background: linear-gradient(90deg, #f3f4f6, #9ca3af); -webkit-background-clip:text; -webkit-text-fill-color: transparent; margin:0; }
.subtitle{ margin:6px 0 0 2px; color: var(--muted); font-size: 14px; }
.pill{ display:inline-flex; align-items:center; gap:8px; padding: 8px 12px; border:1px solid var(--line); border-radius: 999px; color: var(--text); background: rgba(15,17,22,.85); font-size: 13px; box-shadow: var(--shadow); }
.pill .dot{ width:8px; height:8px; border-radius:999px; background:#9ca3af; display:inline-block; }
.grid{ display:grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin: 12px 0 6px 0; }
.card{ background: linear-gradient(180deg, rgba(15,17,22,.94), rgba(11,13,18,.94)); border:1px solid var(--line); border-radius: 16px; padding: 14px 14px 12px 14px; box-shadow: var(--shadow); }
.k{ color: var(--muted); font-size: 12px; margin-bottom: 6px; }
.v{ color: var(--text); font-size: 20px; font-weight: 800; }
.hr{ height:1px; background: var(--line); margin: 12px 0; }
.banner{ background: rgba(148,163,184,.08); border: 1px solid rgba(148,163,184,.22); color: var(--text); border-radius: 16px; padding: 12px 14px; margin: 10px 0 10px 0; }
.metric{ display:flex; justify-content:space-between; align-items:flex-end; gap:10px; }
.metric .left{ display:flex; flex-direction:column; gap:2px; }
.metric .label{ color: var(--muted); font-size: 12px; display:flex; gap:8px; align-items:center; }
.metric .code{ color: var(--text); font-size: 16px; font-weight: 900; line-height:1.1; }
.metric .name{ color: var(--muted); font-size: 12px; margin-top: 2px; }
.metric .tag{ font-size: 12px; padding: 4px 8px; border-radius: 999px; border:1px solid var(--line); color: var(--text); background: rgba(15,17,22,.8); }
.metric .price{ font-size: 22px; font-weight: 900; color: var(--text); line-height: 1; }
.metric .chg{ font-size: 12px; color: var(--muted); }
.stButton>button{ border-radius: 14px !important; border: 1px solid rgba(203,213,225,.26) !important; background: linear-gradient(90deg, rgba(148,163,184,.16), rgba(107,114,128,.10)) !important; color: var(--text) !important; font-weight: 900 !important; font-size: 18px !important; padding: 15px !important; transition: all 0.2s;}
.stButton>button:hover{ border: 1px solid #f87171 !important; background: rgba(248,113,113,0.1) !important;}
[data-testid="stExpander"]{ border: 1px solid var(--line) !important; border-radius: 16px !important; background: rgba(15,17,22,.55) !important; }
[data-testid="stExpander"] summary{ color: var(--text) !important; font-weight: 900 !important; }
.small-note{ color: var(--muted); font-size: 12px; }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

# =========================
# TIME / MARKET
# =========================
TZ_NAME = "Asia/Taipei"

def now_taipei() -> datetime: return datetime.utcnow() + timedelta(hours=8)
def is_market_time(ts: datetime) -> bool: return dtime(9, 0) <= ts.time() <= dtime(13, 30)
def minutes_elapsed_in_session(ts: datetime) -> int:
    start, end = datetime.combine(ts.date(), dtime(9, 0)), datetime.combine(ts.date(), dtime(13, 30))
    if ts < start: return 0
    if ts > end: return 270
    return int((ts - start).total_seconds() // 60)
def bars_expected_5m(ts: datetime) -> int: return max(1, min(54, int(math.ceil(minutes_elapsed_in_session(ts) / 5.0))))
def tw_tick(price: float) -> float:
    if price < 10: return 0.01
    if price < 50: return 0.05
    if price < 100: return 0.10
    if price < 500: return 0.50
    if price < 1000: return 1.00
    return 5.00
def round_to_tick_nearest(x: float, tick: float) -> float: return round(round(x / tick) * tick, 2 if tick < 0.1 else 1 if tick < 1 else 0)
def calc_limit_up(prev_close: float, limit_pct: float) -> float:
    raw = prev_close * (1.0 + limit_pct)
    return round_to_tick_nearest(raw, tw_tick(raw))

# =========================
# 1. 股票清單 (官方 OpenAPI - 絕對不被擋)
# =========================
@st.cache_data(ttl=24 * 3600, show_spinner=False)
def fetch_listed_stocks_mops() -> pd.DataFrame:
    meta, headers = [], {"User-Agent": "Mozilla/5.0"}
    try:
        r_tse = requests.get("https://openapi.twse.com.tw/v1/opendata/t187ap03_L", headers=headers, timeout=10, verify=False)
        if r_tse.status_code == 200:
            for item in r_tse.json():
                c = str(item.get("公司代號", "")).strip()
                if re.match(r"^\d{4,6}$", c): meta.append({"code": c, "name": str(item.get("公司簡稱", "")).strip(), "industry": str(item.get("產業別", "")).strip() or "未分類", "market": "上市"})
    except: pass
    try:
        r_otc = requests.get("https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O", headers=headers, timeout=10, verify=False)
        if r_otc.status_code == 200:
            for item in r_otc.json():
                c = str(item.get("公司代號", "")).strip()
                if re.match(r"^\d{4,6}$", c): meta.append({"code": c, "name": str(item.get("公司簡稱", "")).strip(), "industry": str(item.get("產業別", "")).strip() or "未分類", "market": "上櫃"})
    except: pass

    if not meta: raise ValueError("無法取得股票清單（官方 OpenAPI 連線失敗），請確認網路狀態。")
    return pd.DataFrame(meta).drop_duplicates("code").sort_values("code").reset_index(drop=True)

# =========================
# 2. MIS 盤中極速快篩 (取代等待)
# =========================
def fetch_mis_and_prefilter(codes: list, min_vol: int, dist_limit: float):
    s = requests.Session()
    s.get("https://mis.twse.com.tw/stock/fibest.jsp?lang=zh_tw", headers={"User-Agent": "Mozilla/5.0"}, timeout=5, verify=False)
    
    rows = []
    prog_bar = st.progress(0, text="📡 第一階段：正在對全台股進行光速掃描...")
    total_batches = math.ceil(len(codes) / 100)

    for i in range(0, len(codes), 100):
        chunk = codes[i:i + 100]
        # 簡易判斷：4碼開頭且大於1100通常為上市，其餘上櫃 (不完美但MIS容錯率高)
        ex_ch = "%7c".join([f"{'tse' if len(c)==4 and c.startswith(('1','2','3','4','5','6','8','9')) else 'otc'}_{c}.tw" for c in chunk])
        url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={ex_ch}&json=1&delay=0&_={int(time.time()*1000)}"
        prog_bar.progress((i // 100 + 1) / total_batches, text=f"📡 第一階段：MIS 全市場快篩 ({i//100 + 1}/{total_batches})...")
        
        try:
            r = s.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5, verify=False)
            arr = r.json().get("msgArray", [])
            for q in arr:
                last, upper, vol = q.get("z"), q.get("u"), q.get("v")
                if last == "-" or not last or upper == "-" or not upper: continue
                last, upper, vol_lots = float(last), float(upper), int(float(vol or 0))
                
                # 【快篩核心】：只留成交量夠大，且距離漲停夠近的股票
                dist_pct = ((upper - last) / upper) * 100
                if vol_lots >= min_vol and dist_pct <= dist_limit:
                    rows.append({
                        "code": q.get("c"), "last": last, "upper": upper, "dist_pct": dist_pct,
                        "prev_close": float(q.get("y")) if q.get("y")!="-" else 0, "vol_lots": vol_lots
                    })
        except: time.sleep(0.05)
    
    prog_bar.empty()
    return pd.DataFrame(rows)

# =========================
# 3. 日線基準 (倒裝過濾：只抓少量候選)
# =========================
def build_daily_baseline_for_candidates(candidates_df: pd.DataFrame) -> pd.DataFrame:
    if candidates_df.empty: return pd.DataFrame()
    syms = [f"{c}.TW" for c in candidates_df["code"]]
    today = now_taipei().date()
    start = (now_taipei() - timedelta(days=380)).date().isoformat()
    
    prog_bar = st.progress(0, text=f"📊 第二階段：鎖定 {len(syms)} 檔候選，正在調閱主力歷史籌碼...")
    
    try:
        # threads=False 防當機
        raw = yf.download(tickers=" ".join(syms), start=start, interval="1d", group_by="ticker", auto_adjust=False, threads=False, progress=False)
    except:
        prog_bar.empty(); return pd.DataFrame()

    rows = []
    for idx, (c, sym) in enumerate(zip(candidates_df["code"], syms)):
        prog_bar.progress((idx + 1) / len(syms), text=f"📊 第二階段：計算基底緊縮與排雷 ({idx+1}/{len(syms)})...")
        try:
            df = raw[sym].dropna().copy() if isinstance(raw.columns, pd.MultiIndex) else raw.dropna().copy()
            df = _drop_today_bar_if_exists(df, today)
            if df.empty or len(df) < 60: continue

            close, high, low, vol = df["Close"].astype(float), df["High"].astype(float), df["Low"].astype(float), df["Volume"].astype(float)
            yday_close, prev2_close = float(close.iloc[-1]), float(close.iloc[-2])
            
            # 你最強的基底與排雷邏輯
            ret_1d = (yday_close / prev2_close - 1.0) if prev2_close else 0
            ret_5d = (yday_close / float(close.iloc[-6]) - 1.0) * 100 if len(close) >= 6 else 0
            
            high60_ex1 = float(high.rolling(60).max().shift(1).iloc[-1])
            range20_pct = float((high.rolling(20).max().iloc[-1] - low.rolling(20).min().iloc[-1]) / yday_close)
            range60_pct = float((high.rolling(60).max().iloc[-1] - low.rolling(60).min().iloc[-1]) / yday_close)

            tr = pd.concat([(high - low).abs(), (high - close.shift(1)).abs(), (low - close.shift(1)).abs()], axis=1).max(axis=1)
            atr20_pct = float(tr.rolling(20).mean().iloc[-1] / yday_close) * 100

            hist_ret = close.pct_change().dropna()
            limit_class_pct = 0.20 if (len(hist_ret) > 10 and float(hist_ret.tail(260).max()) > 0.105) else 0.10
            max_ret_10d = float(hist_ret.tail(10).max()) * 100.0
            had_hype_10d = (max_ret_10d >= (19.0 if limit_class_pct == 0.20 else 9.5))

            y_open, y_high, y_low, y_close = float(df["Open"].iloc[-1]), float(df["High"].iloc[-1]), float(df["Low"].iloc[-1]), float(df["Close"].iloc[-1])
            y_upper_wick_ratio = float((y_high - max(y_open, y_close)) / max(1e-9, y_high - y_low))
            
            vol_ma20_shares = float(vol.rolling(20).mean().iloc[-1])
            yday_vol_spike = (float(vol.iloc[-1]) >= 2.0 * vol_ma20_shares)

            ma20 = close.rolling(20).mean()
            base_len_days = int(((close / (ma20 + 1e-9) - 1.0).abs() <= 0.04).tail(60).sum())
            base_tight_score = float((1.0 - min(1.0, range20_pct / (range60_pct + 1e-9))) * 0.6 + (1.0 - min(1.0, atr20_pct / 8.0)) * 0.4)

            rows.append({
                "code": c, "yday_close": yday_close, "limit_class_pct": limit_class_pct, "vol_ma20_shares": vol_ma20_shares,
                "high60_ex1": high60_ex1, "atr20_pct": atr20_pct, "ret_5d": ret_5d, "max_ret_10d": max_ret_10d,
                "had_hype_10d": had_hype_10d, "yday_upper_wick_ratio": y_upper_wick_ratio, "yday_vol_spike": yday_vol_spike,
                "base_len_days": base_len_days, "base_tight_score": base_tight_score, "ret_1d": ret_1d * 100
            })
        except: continue
        
    prog_bar.empty()
    return pd.DataFrame(rows).set_index("code") if rows else pd.DataFrame()

# =========================
# 4. INTRADAY 5M (僅針對候選)
# =========================
def fetch_intraday_bars_5m(codes: list[str]) -> dict:
    if not codes: return {}
    bars, today = {}, now_taipei().date()
    syms = [f"{c}.TW" for c in codes]
    try:
        raw = yf.download(tickers=" ".join(syms), period="1d", interval="5m", group_by="ticker", auto_adjust=False, threads=False, progress=False)
        for c, sym in zip(codes, syms):
            df = raw[sym].dropna().copy() if isinstance(raw.columns, pd.MultiIndex) else raw.dropna().copy()
            if df.empty: continue
            
            idx = df.index
            try: idx = idx.tz_convert(TZ_NAME).tz_localize(None) if getattr(idx, "tz", None) else idx.tz_localize(None)
            except: pass
            df.index = idx
            
            df = df[df.index.date == today].copy()
            if not df.empty: bars[c] = df
    except: pass
    return bars

def compute_open_board_count(df5m: pd.DataFrame, limit_up: float, tick: float) -> int:
    if df5m is None or df5m.empty: return 999
    close, high = df5m["Close"].astype(float).values, df5m["High"].astype(float).values
    touch = high >= (limit_up - tick)
    if not touch.any(): return 999
    first_idx = int(touch.argmax())
    opened, in_limit_state = 0, True
    for i in range(first_idx + 1, len(close)):
        if in_limit_state:
            if close[i] < (limit_up - 2.0 * tick): opened += 1; in_limit_state = False
        else:
            if high[i] >= (limit_up - tick): in_limit_state = True
    return opened

# =========================
# PRETTY TABLE
# =========================
def render_pretty_table(df: pd.DataFrame) -> None:
    if df.empty: return st.info("沒有資料。")
    def f2(x): return f"{float(x):,.2f}" if pd.notna(x) else str(x)
    def f0(x): return f"{int(float(x)):,}" if pd.notna(x) else str(x)
    def f3(x): return f"{float(x):,.3f}" if pd.notna(x) else str(x)

    rows = "".join([f"<tr><td class='center'>{r.get('排名','')}</td><td>{r.get('代號','')}</td><td>{r.get('名稱','')}</td><td>{r.get('族群','')}</td><td class='num'>{f2(r.get('現價',''))}</td><td class='num'>{f2(r.get('漲停價',''))}</td><td class='num'>{f2(r.get('距離漲停(%)',''))}</td><td class='num'>{f2(r.get('較昨收(%)',''))}</td><td class='num'>{f0(r.get('累積量(張)',''))}</td><td class='num'>{f2(r.get('盤中爆量倍數',''))}</td><td class='num'>{f3(r.get('收在高檔(0-1)',''))}</td><td class='num'>{f2(r.get('回落幅度(%)',''))}</td><td class='num'>{r.get('開板次數(5m)','')}</td><td class='num'>{r.get('基底天數','')}</td><td class='num'>{f2(r.get('近5日漲幅(%)',''))}</td><td class='num'>{f2(r.get('ATR20(%)',''))}</td><td class='num'>{f2(r.get('連板潛力分',''))}</td></tr>" for _, r in df.iterrows()])

    html = f"""
    <!doctype html><html><head><meta charset="utf-8"/><style>:root{{--text:#e5e7eb; --muted:#9ca3af; --line:rgba(148,163,184,.16); --hi: rgba(148,163,184,.08);}} body{{margin:0; background:transparent; color:var(--text); font-family:-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;}} .wrap{{max-height: 580px; overflow:auto; border: 1px solid var(--line); border-radius: 16px; background: rgba(15,17,22,.70);}} table{{width:100%; border-collapse: separate; border-spacing: 0; font-size: 12.5px;}} thead th{{position: sticky; top: 0; z-index: 2; text-align: left; padding: 11px 10px; background: rgba(15,17,22,.98); color: var(--text); border-bottom: 1px solid var(--line); font-weight: 900; white-space: nowrap;}} tbody td{{padding: 10px 10px; border-bottom: 1px solid rgba(148,163,184,.10); color: var(--text); background: rgba(11,13,18,.92); white-space: nowrap;}} tbody tr:hover td{{background: var(--hi);}} .num{{text-align:right; font-variant-numeric: tabular-nums;}} .center{{text-align:center;}}</style></head><body><div class="wrap"><table><thead><tr><th class="center">#</th><th>代號</th><th>名稱</th><th>族群</th><th class="num">現價</th><th class="num">漲停價</th><th class="num">距離漲停(%)</th><th class="num">較昨收(%)</th><th class="num">累積量(張)</th><th class="num">盤中爆量倍數</th><th class="num">收在高檔</th><th class="num">回落(%)</th><th class="num">開板次數</th><th class="num">基底天數</th><th class="num">近5日(%)</th><th class="num">ATR20(%)</th><th class="num">連板潛力分</th></tr></thead><tbody>{rows}</tbody></table></div></body></html>
    """
    components.html(html, height=640, scrolling=False)

# =========================
# HEADER & INIT UI
# =========================
now_ts = now_taipei()
elapsed = minutes_elapsed_in_session(now_ts)

st.markdown(f"""
<div class="header-wrap">
  <div>
    <h1 class="title">起漲戰情室</h1>
    <div class="subtitle">極簡暴力版 ｜ 隱藏所有設定，一鍵秒開，倒裝引擎絕不轉圈圈</div>
  </div>
  <div class="pill"><span class="dot"></span> 台北時間 <b>{now_ts.strftime('%H:%M:%S')}</b>　盤中進度 <b>{elapsed}/270</b></div>
</div>
""", unsafe_allow_html=True)

st.markdown(f"""
<div class="grid">
  <div class="card"><div class="k">市場</div><div class="v">全上市/上櫃</div></div>
  <div class="card"><div class="k">最低盤中量</div><div class="v">1000<small> 張</small></div></div>
  <div class="card"><div class="k">距離漲停</div><div class="v">2.0<small>%以內</small></div></div>
  <div class="card"><div class="k">模式</div><div class="v">標準平衡<small>（內建濾網）</small></div></div>
</div>
""", unsafe_allow_html=True)

st.markdown("""
<div class="banner">
<b>你要的「瑞軒型」我用 8 層過濾做掉：</b><br>
①貼近漲停 ②回落小 ③收在高檔 ④同時間量能爆量 ⑤排除昨日先派發 ⑥排除近10日已嗨過 ⑦基底長＋緊縮 ⑧族群共振加分
</div>
""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)
run_scan = st.button("🚀 一鍵啟動：掃描第一根漲停", use_container_width=True)

# =========================
# CORE SCAN EXECUTION
# =========================
if run_scan:
    try:
        meta_df = fetch_listed_stocks_mops()
        meta_dict = meta_df.set_index("code")[["name", "industry"]].to_dict(orient="index")
        all_codes = meta_df["code"].tolist()
    except Exception as e:
        st.error(f"連線異常：{e}")
        st.stop()

    # 1. 第一階段：極速快篩 (取代等待)
    pre_candidates = fetch_mis_and_prefilter(all_codes, min_vol=1000, dist_limit=2.0)
    
    if pre_candidates.empty:
        st.info("😴 目前盤面沒有符合「量大且接近漲停」的標的。請耐心等待好球進壘！")
        st.stop()

    # 2. 第二階段：只針對候選股抓取日線，進行排雷與基底計算
    base_df = build_daily_baseline_for_candidates(pre_candidates)
    
    if base_df.empty:
        st.warning("⚠️ 候選股未能通過日線排雷濾網 (多半是因為近期已經大漲過，非第一根)。")
        st.stop()

    # 3. 第三階段：針對存活者抓取 5M，計算開板與回落
    with st.spinner("⚙️ 第三階段：進行連板潛力終極運算..."):
        bars_today = fetch_intraday_bars_5m(base_df.index.tolist())
    
    final_rows = []
    frac = max(0.2, min(1.0, elapsed / 270.0))

    for _, r in pre_candidates.iterrows():
        c = r["code"]
        if c not in base_df.index: continue
        b = base_df.loc[c]

        # 核心排雷：如果近10天已經大嗨過，或是昨天爆量長黑，直接淘汰(不是第一根)
        if b["had_hype_10d"] or (b["yday_vol_spike"] and b["yday_upper_wick_ratio"] >= 0.35 and b["ret_1d"] >= 6.0): continue
        
        # 爆量倍數 (用 MIS 累積量 / 日線歷史均量)
        vol_ma = float(b["vol_ma20_shares"])
        vol_ratio = (float(r["vol_lots"]*1000) / (vol_ma * frac + 1e-9)) if vol_ma > 0 else 0.0
        if vol_ratio < 2.0: continue # 爆量不夠淘汰

        # 鎖死品質 (5M 計算)
        df5m = bars_today.get(c, pd.DataFrame())
        open_board = compute_open_board_count(df5m, float(r["upper"]), tw_tick(float(r["upper"])))
        
        day_high, day_low = float(df5m["High"].max()) if not df5m.empty else r["upper"], float(df5m["Low"].min()) if not df5m.empty else r["last"]
        rng = max(1e-9, day_high - day_low)
        close_pos = (r["last"] - day_low) / rng
        pullback = (day_high - r["last"]) / max(1e-9, day_high)

        if pullback > 0.0040: continue # 回落太大淘汰

        # 潛力計分 (滿分100)
        score = 0.0
        score += 30.0 * min(1.0, max(0.0, (close_pos - 0.85) / 0.15))
        score += 20.0 * min(1.0, max(0.0, (0.0038 - pullback) / 0.0038))
        score += 20.0 * min(1.0, max(0.0, (vol_ratio - 1.5) / 2.5))
        score += 15.0 * min(1.0, max(0.0, (float(b["base_len_days"]) - 8) / 40.0))
        score += 10.0 * min(1.0, max(0.0, float(b["base_tight_score"])))
        if open_board != 999: score -= min(10.0, float(open_board) * 3.0)
        if float(r["upper"]) >= float(b["high60_ex1"]) * 0.995: score += 5.0 # 突破60日高加分

        chg_pct = (r["last"] / float(b["yday_close"]) - 1.0) * 100.0

        final_rows.append({
            "代號": c, "名稱": meta_dict.get(c, {}).get("name", ""), "族群": meta_dict.get(c, {}).get("industry", ""),
            "現價": r["last"], "漲停價": r["upper"], "距離漲停(%)": r["dist_pct"], "較昨收(%)": chg_pct,
            "累積量(張)": r["vol_lots"], "盤中爆量倍數": vol_ratio, "收在高檔(0-1)": close_pos, "回落幅度(%)": pullback * 100,
            "開板次數(5m)": open_board if open_board != 999 else None, "基底天數": int(b["base_len_days"]),
            "近5日漲幅(%)": float(b["ret_5d"]), "ATR20(%)": float(b["atr20_pct"]), "連板潛力分": float(max(0.0, min(100.0, score)))
        })

    if not final_rows:
        st.warning("⚠️ 所有快篩標的皆未通過嚴格的『第一根濾網』(多半是因為近期已經大漲過，或是爆量不足)。")
    else:
        # 族群共振加分
        res = pd.DataFrame(final_rows)
        grp = res["族群"].value_counts()
        res["連板潛力分"] += res["族群"].apply(lambda x: min(15.0, max(0.0, (int(grp.get(x, 1)) - 1) * 5.0)) if x and x!="未分類" else 0.0)
        res["連板潛力分"] = res["連板潛力分"].clip(0, 100)
        
        res = res.sort_values(["連板潛力分", "距離漲停(%)"], ascending=[False, True]).reset_index(drop=True)
        res.insert(0, "排名", range(1, len(res) + 1))

        st.success(f"🎯 完美鎖定！為您篩選出 {len(res)} 檔『第一根漲停』候選標的。")

        # 輸出卡片
        cols = st.columns(4)
        for i, (_, r) in enumerate(res.head(8).iterrows(), start=1):
            with cols[(i - 1) % 4]:
                score = float(r["連板潛力分"])
                tag = "🔒 幾乎鎖死" if score >= 75 else "👀 候選"
                st.markdown(f"""
<div class="card">
  <div class="metric">
    <div class="left"><div class="label">#{i} <span class="tag">{tag}</span></div><div class="code">{r['代號']} <span class="name">{r['名稱']}</span></div></div>
    <div style="text-align:right"><div class="price">{float(r['現價']):.2f}</div><div class="chg">距漲停 {float(r['距離漲停(%)']):.2f}%</div></div>
  </div>
  <div class="hr"></div>
  <div class="small-note">較昨收：{float(r['較昨收(%)']):.2f}% ｜ 爆量：{float(r['盤中爆量倍數']):.2f}x</div>
  <div class="small-note">開板(5m)：{r.get('開板次數(5m)', '-')} ｜ <span style="color:#a3e635;">連板分：{score:.1f}</span></div>
</div>
""", unsafe_allow_html=True)

        st.markdown("<div class='hr'></div>", unsafe_allow_html=True)
        st.text_input("一鍵複製（代號清單）", value=",".join(res["代號"].astype(str).tolist()))
        with st.expander("📋 看完整榜單（美化表格）", expanded=True):
            render_pretty_table(res)
