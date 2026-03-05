# app.py — 終極極簡暴力版｜全 Yahoo 引擎防卡死｜保留 1~8 主軸濾網
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
# UI / THEME (冷酷黑灰，完全隱藏設定)
# =========================
st.set_page_config(page_title="起漲戰情室｜極簡暴力", page_icon="⚡", layout="wide", initial_sidebar_state="collapsed")

CSS = """
<style>
:root{
  --bg:#07080b; --card:#0f1116; --text:#e5e7eb; --muted:#9ca3af;
  --line:rgba(148,163,184,.16); --shadow: 0 16px 40px rgba(0,0,0,.35);
  --ok:#a3e635; --warn:#fbbf24; --bad:#fb7185;
}
[data-testid="stAppViewContainer"], .main{ background: var(--bg) !important; color: var(--text) !important; }
.block-container{ padding-top: 2rem; padding-bottom: 3rem; max-width: 1200px; }
[data-testid="stSidebar"] { display: none !important; } /* 暴力隱藏側邊欄 */
[data-testid="stHeader"]{ background: transparent !important; }

.title{ font-size: 46px; font-weight: 900; background: linear-gradient(90deg, #f3f4f6, #9ca3af); -webkit-background-clip:text; -webkit-text-fill-color: transparent; margin:0; text-align: center; }
.subtitle{ color: var(--muted); font-size: 15px; text-align: center; margin-bottom: 30px; letter-spacing: 1px; }

.card{ background: linear-gradient(180deg, rgba(15,17,22,.94), rgba(11,13,18,.94)); border:1px solid var(--line); border-radius: 16px; padding: 18px; box-shadow: var(--shadow); margin-bottom: 12px; }
.metric{ display:flex; justify-content:space-between; align-items:flex-end; border-bottom: 1px solid var(--line); padding-bottom: 10px; margin-bottom: 10px;}
.metric .code{ color: var(--text); font-size: 20px; font-weight: 900; }
.metric .name{ color: var(--muted); font-size: 14px; margin-left: 8px;}
.metric .price{ font-size: 26px; font-weight: 900; color: var(--text); }
.tag{ font-size: 12px; padding: 4px 8px; border-radius: 999px; border:1px solid var(--line); background: rgba(15,17,22,.8); color: var(--text); }

.stButton>button{ border-radius: 16px !important; border: 1px solid rgba(255,255,255,0.2) !important; background: linear-gradient(90deg, #1f2937, #111827) !important; color: white !important; font-weight: 900 !important; font-size: 20px !important; padding: 25px !important; transition: all 0.3s ease; box-shadow: 0 4px 15px rgba(0,0,0,0.5); }
.stButton>button:hover{ border-color: #f87171 !important; transform: translateY(-2px); box-shadow: 0 6px 20px rgba(248,113,113,0.2); }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

# =========================
# TIME & MATH HELPERS
# =========================
TZ_NAME = "Asia/Taipei"
def now_taipei(): return datetime.utcnow() + timedelta(hours=8)
def minutes_elapsed_in_session(ts):
    start, end = datetime.combine(ts.date(), dtime(9, 0)), datetime.combine(ts.date(), dtime(13, 30))
    if ts < start: return 0
    if ts > end: return 270
    return int((ts - start).total_seconds() // 60)

def tw_tick(price):
    return 0.01 if price<10 else 0.05 if price<50 else 0.1 if price<100 else 0.5 if price<500 else 1.0 if price<1000 else 5.0

def calc_limit_up(prev_close, limit_pct):
    raw = prev_close * (1.0 + limit_pct)
    tick = tw_tick(raw)
    return round(round(raw / tick) * tick, 2 if tick < 0.1 else 1 if tick < 1 else 0)

# =========================
# ENGINE 1: 股票清單 (GitHub 純淨版，絕對不被擋)
# =========================
@st.cache_data(ttl=24*3600, show_spinner=False)
def get_stock_list():
    meta = {}
    urls = [
        ("上市", "https://raw.githubusercontent.com/mlouielu/twstock/master/twstock/codes/twse_equities.csv"),
        ("上櫃", "https://raw.githubusercontent.com/mlouielu/twstock/master/twstock/codes/tpex_equities.csv")
    ]
    for market, url in urls:
        try:
            r = requests.get(url, timeout=10, verify=False)
            df = pd.read_csv(io.StringIO(r.text.replace("\r", "")), on_bad_lines="skip")
            if "code" not in df.columns:
                df = pd.read_csv(io.StringIO(r.text.replace("\r", "")), header=None)
                df.columns = ["type","code","name","ISIN","start","market","group","CFI"][:df.shape[1]]
            for _, row in df.iterrows():
                c = str(row.get("code", "")).strip()
                if re.match(r"^\d{4,6}$", c): 
                    meta[c] = {
                        "name": str(row.get("name", "")).strip(), 
                        "ind": str(row.get("group", "")).strip() or "未分類", 
                        "ex": "TW" if market == "上市" else "TWO"
                    }
        except: pass
    if not meta: raise ValueError("無法取得股票清單，請確認網路連線。")
    return meta

# =========================
# ENGINE 2: 全市場 YFinance 極速快篩 (取代會卡死的 MIS)
# =========================
def fast_yahoo_scan(meta_dict, status_placeholder):
    sym_to_code = {f"{c}.{v['ex']}": c for c, v in meta_dict.items()}
    syms = list(sym_to_code.keys())
    
    status_placeholder.update(label=f"📡 正在向 Yahoo 批次索取 {len(syms)} 檔即時報價 (絕不卡死)...", state="running")
    
    rows = []
    # 批次抓取 5 天日線，獲取昨收與今價
    try:
        raw = yf.download(tickers=" ".join(syms), period="5d", interval="1d", group_by="ticker", auto_adjust=False, threads=False, progress=False)
    except Exception as e:
        raise ValueError(f"Yahoo 連線失敗: {e}")

    for sym in syms:
        try:
            df = raw[sym].dropna() if isinstance(raw.columns, pd.MultiIndex) else raw.dropna()
            if len(df) < 2: continue
            
            last = float(df["Close"].iloc[-1])
            prev_close = float(df["Close"].iloc[-2])
            vol_lots = int(float(df["Volume"].iloc[-1]) / 1000)
            
            # 【極簡暴力濾網】：只留盤中量 > 1000張，且距離漲停 < 3.0%
            limit_up = calc_limit_up(prev_close, 0.10)
            dist = ((limit_up - last) / limit_up) * 100
            
            if vol_lots >= 1000 and dist <= 3.0:
                rows.append({
                    "code": sym_to_code[sym], "last": last, "upper": limit_up, 
                    "dist": dist, "vol_lots": vol_lots, "yday": prev_close
                })
        except: continue
        
    return pd.DataFrame(rows)

# =========================
# ENGINE 3: YFinance 日線與 5分K 深度濾網 (套用 1~8)
# =========================
def core_filter_engine(candidates_df, meta_dict, now_ts, status_placeholder):
    if candidates_df.empty: return pd.DataFrame()
    syms = [f"{c}.{meta_dict[c]['ex']}" for c in candidates_df["code"]]
    results = []
    frac = max(0.2, min(1.0, minutes_elapsed_in_session(now_ts) / 270.0))

    status_placeholder.update(label=f"📊 鎖定 {len(syms)} 檔候選，正在進行 1~8 終極濾網運算...", state="running")
    
    try:
        raw_daily = yf.download(tickers=" ".join(syms), period="200d", interval="1d", group_by="ticker", auto_adjust=False, threads=False, progress=False)
        raw_5m = yf.download(tickers=" ".join(syms), period="1d", interval="5m", group_by="ticker", auto_adjust=False, threads=False, progress=False)
    except:
        return pd.DataFrame()

    for _, r in candidates_df.iterrows():
        c = r["code"]
        sym = f"{c}.{meta_dict[c]['ex']}"
        try:
            dfD = raw_daily[sym].dropna() if isinstance(raw_daily.columns, pd.MultiIndex) else raw_daily.dropna()
            df5 = raw_5m[sym].dropna() if isinstance(raw_5m.columns, pd.MultiIndex) else raw_5m.dropna()
            if len(dfD) < 60 or df5.empty: continue

            # --- 日線特徵 ---
            closeD, highD, lowD, volD = dfD["Close"].astype(float), dfD["High"].astype(float), dfD["Low"].astype(float), dfD["Volume"].astype(float)
            yday_close = float(closeD.iloc[-2] if dfD.index[-1].date() == now_ts.date() else closeD.iloc[-1])
            
            hist_ret = closeD.pct_change().dropna()
            limit_pct = 0.20 if (len(hist_ret)>10 and float(hist_ret.tail(150).max()) > 0.105) else 0.10
            
            # 【排雷濾網】：近10天大漲過，或是昨天爆量長上影線 -> 淘汰
            max_ret_10d = float(hist_ret.tail(10).max()) * 100.0
            vol_ma20 = float(volD.rolling(20).mean().iloc[-1])
            if max_ret_10d >= (19.0 if limit_pct == 0.20 else 9.5): continue
            
            # 【基底計算】
            ma20 = closeD.rolling(20).mean()
            base_len = int(((closeD / (ma20 + 1e-9) - 1.0).abs() <= 0.04).tail(60).sum())
            tr = pd.concat([(highD - lowD).abs(), (highD - closeD.shift(1)).abs(), (lowD - closeD.shift(1)).abs()], axis=1).max(axis=1)
            atr20_pct = float(tr.rolling(20).mean().iloc[-1] / yday_close) * 100
            range20_pct = float((highD.rolling(20).max().iloc[-1] - lowD.rolling(20).min().iloc[-1]) / yday_close)
            range60_pct = float((highD.rolling(60).max().iloc[-1] - lowD.rolling(60).min().iloc[-1]) / yday_close)
            base_tight = float((1.0 - min(1.0, range20_pct / (range60_pct + 1e-9))) * 0.6 + (1.0 - min(1.0, atr20_pct / 8.0)) * 0.4)

            # --- 5分K 特徵 ---
            day_high, day_low = float(df5["High"].max()), float(df5["Low"].min())
            tick = tw_tick(r["upper"])
            
            # 【鎖死品質與回落】
            rng = max(1e-9, day_high - day_low)
            close_pos = (r["last"] - day_low) / rng
            pullback = (day_high - r["last"]) / max(1e-9, day_high)
            if pullback > 0.0038: continue # 回落超過 0.38% 淘汰

            # 【開板次數近似】
            high5, close5 = df5["High"].astype(float).values, df5["Close"].astype(float).values
            touch = high5 >= (r["upper"] - tick)
            open_board = 0
            if touch.any():
                in_limit = True
                for i in range(int(touch.argmax()) + 1, len(close5)):
                    if in_limit and close5[i] < (r["upper"] - 2.0 * tick): open_board += 1; in_limit = False
                    elif not in_limit and high5[i] >= (r["upper"] - tick): in_limit = True

            # 【爆量倍數】
            vol_ratio = (r["vol_lots"] * 1000) / (vol_ma20 * frac + 1e-9)
            if vol_ratio < 1.8: continue # 爆量不足 1.8倍 淘汰

            # --- 綜合潛力計分 (0~100) ---
            score = 0.0
            score += 30.0 * min(1.0, max(0.0, (close_pos - 0.85) / 0.15))
            score += 20.0 * min(1.0, max(0.0, (0.0038 - pullback) / 0.0038))
            score += 20.0 * min(1.0, max(0.0, (vol_ratio - 1.5) / 2.5))
            score += 15.0 * min(1.0, max(0.0, (base_len - 8) / 40.0))
            score += 5.0 * min(1.0, max(0.0, base_tight))
            score -= min(10.0, float(open_board) * 3.0) # 開板扣分

            results.append({
                "代號": c, "名稱": meta_dict[c]["name"], "族群": meta_dict[c]["ind"],
                "現價": r["last"], "漲停價": r["upper"], "距離漲停(%)": r["dist"],
                "較昨收(%)": (r["last"] / yday_close - 1.0)*100, "累積量(張)": r["vol_lots"],
                "盤中爆量倍數": vol_ratio, "開板次數": open_board, "基底天數": base_len,
                "潛力分": max(0.0, min(100.0, score))
            })
        except: continue

    if not results: return pd.DataFrame()
    out = pd.DataFrame(results)
    
    # 族群共振加分
    grp = out["族群"].value_counts()
    out["潛力分"] += out["族群"].apply(lambda x: min(15.0, max(0.0, (int(grp.get(x, 1)) - 1) * 5.0)) if x and x != "未分類" else 0.0)
    out["潛力分"] = out["潛力分"].clip(0, 100)
    
    out = out.sort_values(["潛力分", "距離漲停(%)"], ascending=[False, True]).reset_index(drop=True)
    out.index += 1
    return out

# =========================
# MAIN APP (無腦一鍵啟動)
# =========================
st.markdown('<div class="title">🧊 起漲戰情室</div>', unsafe_allow_html=True)
st.markdown('<div class="subtitle">極簡暴力版 ｜ 一鍵貫穿 8 道主力濾網，絕不轉圈圈</div>', unsafe_allow_html=True)

run_scan = st.button("🚀 啟動掃描 (自動鎖定第一根)", use_container_width=True)

if run_scan:
    # 🛡️ 加入 st.status，按下去的瞬間就會跳出面板，告訴你進度！
    with st.status("⚡ 系統極速運算中，請稍候...", expanded=True) as status:
        
        status.update(label="📦 1/3 載入台股最新清單...", state="running")
        try:
            meta = get_stock_list()
        except Exception as e:
            status.update(label="❌ 清單載入失敗", state="error")
            st.error(str(e)); st.stop()
            
        pre_df = fast_yahoo_scan(meta, status)
        
        if pre_df.empty:
            status.update(label="✅ 掃描完畢", state="complete")
            st.info("😴 目前全市場沒有符合「爆量且接近漲停」的標的。")
            st.stop()
            
        final_res = core_filter_engine(pre_df, meta, now_taipei(), status)
        
        status.update(label="✅ 掃描與計算完成！", state="complete")

    # =========================
    # 渲染結果
    # =========================
    if final_res.empty:
        st.warning("⚠️ 有股票接近漲停，但都被『排雷濾網』排除了 (多半是近期已經大漲過、回落太大或爆量不足)。")
    else:
        st.success(f"🎯 完美鎖定！為您篩選出 {len(final_res)} 檔『第一根漲停』完美標的。")
        
        # 精美卡片
        cols = st.columns(min(len(final_res), 4))
        for i, r in final_res.head(8).iterrows():
            with cols[(i-1) % 4]:
                tag = "🔒 幾乎鎖死" if r["潛力分"] >= 75 else "👀 候選"
                st.markdown(f"""
                <div class="card">
                    <div class="metric">
                        <div>
                            <span class="tag">{tag}</span><br>
                            <span class="code">{r['代號']}</span><span class="name">{r['名稱']}</span>
                        </div>
                        <div class="price">{r['現價']:.2f}</div>
                    </div>
                    <div style="display:flex; justify-content:space-between; font-size:13px; color:#9ca3af;">
                        <span>距漲停: <b style="color:#e5e7eb;">{r['距離漲停(%)']:.2f}%</b></span>
                        <span>爆量: <b style="color:#f87171;">{r['盤中爆量倍數']:.1f}x</b></span>
                    </div>
                    <div style="display:flex; justify-content:space-between; font-size:13px; color:#9ca3af; margin-top:6px;">
                        <span>潛力分: <b style="color:#a3e635;">{r['潛力分']:.1f}</b></span>
                        <span>開板: <b>{r['開板次數']}</b></span>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
        # 匯出 CSV 按鈕
        st.markdown("<div style='height:20px;'></div>", unsafe_allow_html=True)
        csv = final_res.to_csv(index=False).encode('utf-8-sig')
        st.download_button("📥 匯出今日戰情報表 (CSV)", data=csv, file_name=f"第一根漲停_{datetime.now().strftime('%Y%m%d_%H%M')}.csv", mime="text/csv", use_container_width=True)
        
        # 完整表格
        with st.expander("📋 展開完整數據表"):
            st.dataframe(final_res.style.format({
                "現價":"{:.2f}", "漲停價":"{:.2f}", "距離漲停(%)":"{:.2f}%", "較昨收(%)":"{:.2f}%",
                "累積量(張)":"{:,}", "盤中爆量倍數":"{:.2f}x", "潛力分":"{:.1f}"
            }), use_container_width=True)
