import time
import re
from datetime import datetime, timedelta, time as dtime

import certifi
import requests
import pandas as pd
import yfinance as yf
import streamlit as st

# =========================
# 基本設定
# =========================
st.set_page_config(page_title="台股盤中起漲第一根掃描器", page_icon="🚀", layout="wide")

CSS = """
<style>
    .main-title {
        font-size: 40px; font-weight: 900;
        background: -webkit-linear-gradient(45deg, #ff4b4b, #ff8f00);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
        text-align: center; margin-bottom: 8px; padding-top: 10px;
    }
    .sub-title { text-align: center; color: #888; font-size: 14px; margin-bottom: 18px; }
    .hint-box {
        background-color: #fff3e0; border-left: 5px solid #ff9800;
        padding: 12px 16px; border-radius: 6px; margin-bottom: 18px; color: #333;
    }
    .small-note { color:#777; font-size: 12px; }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

st.markdown('<div class="main-title">🚀 台股盤中「起漲第一根」掃描器</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-title">全上市掃描｜盤中爆量×突破｜假突破避雷（收在高檔/上影線/過熱/昨日已爆量）</div>', unsafe_allow_html=True)

st.markdown("""
<div class="hint-box">
<b>💡 用法：</b><br>
1) 建議 13:15～13:25 掃描（靠近收盤，型態更接近今日定型）<br>
2) 第一次跑「全上市」會比較久（要建日線基準快取）；之後會快很多<br>
<span class="small-note">盤中即時：TWSE MIS；股票清單：TWSE OpenAPI；日線基準：yfinance。</span>
</div>
""", unsafe_allow_html=True)

# =========================
# 工具：台北時間
# =========================
def now_taipei() -> datetime:
    return datetime.utcnow() + timedelta(hours=8)

def minutes_elapsed_in_session(ts: datetime) -> int:
    start = datetime.combine(ts.date(), dtime(9, 0))
    end   = datetime.combine(ts.date(), dtime(13, 30))
    if ts < start:
        return 0
    if ts > end:
        return 270
    return int((ts - start).total_seconds() // 60)

# =========================
# HTTP helpers（避免你環境 CA 出事）
# =========================
def get_json(url: str, timeout: int = 30) -> list | dict:
    # 先用系統預設驗證；若遇到憑證/CA問題，改用 certifi 的 CA bundle 再試一次
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.SSLError:
        r = requests.get(url, headers=headers, timeout=timeout, verify=certifi.where())
        r.raise_for_status()
        return r.json()

# =========================
# 1) 抓「全上市公司代號清單」（TWSE OpenAPI）
# =========================
@st.cache_data(ttl=24 * 3600)
def fetch_all_twse_listed_stocks() -> pd.DataFrame:
    """
    用 TWSE OpenAPI 取得「上市公司基本資料」：公司代號/公司簡稱
    https://openapi.twse.com.tw/v1/opendata/t187ap03_L
    """
    url = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
    data = get_json(url)

    df = pd.DataFrame(data)
    # 欄位：公司代號、公司簡稱（OpenAPI 回傳）
    df = df.rename(columns={"公司代號": "code", "公司簡稱": "name"})

    # 保留常見股票代號格式（4碼為主；有些會到 5~6碼）
    df["code"] = df["code"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()

    df = df[df["code"].str.match(r"^\d{4,6}$")].copy()
    df = df[["code", "name"]].drop_duplicates("code").sort_values("code").reset_index(drop=True)
    return df

# =========================
# 2) 盤中即時：TWSE MIS getStockInfo（支援 | 批次）
# =========================
SESSION_URL = "https://mis.twse.com.tw/stock/index.jsp"
QUOTE_URL = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"

def _safe_float(x):
    try:
        if x in (None, "-", "", "nan"):
            return None
        return float(x)
    except Exception:
        return None

def _safe_int(x):
    try:
        if x in (None, "-", "", "nan"):
            return None
        return int(float(x))
    except Exception:
        return None

def fetch_realtime_quotes_twse(codes: list[str], batch_size: int = 120) -> pd.DataFrame:
    """
    回傳欄位：
    code, name, last, open, high, low, prev_close, vol_lots, tlong
    vol_lots：MIS v 常見為「累積成交量（張）」
    """
    s = requests.Session()
    s.get(SESSION_URL, timeout=20, headers={"User-Agent": "Mozilla/5.0"})

    out = []
    headers = {"User-Agent": "Mozilla/5.0", "Referer": SESSION_URL}

    for i in range(0, len(codes), batch_size):
        chunk = codes[i:i + batch_size]
        ex_ch = "|".join([f"tse_{c}.tw" for c in chunk])

        params = {
            "ex_ch": ex_ch,
            "json": "1",
            "delay": "0",
            "_": str(int(time.time() * 1000)),
        }

        resp = s.get(QUOTE_URL, params=params, headers=headers, timeout=25)
        data = resp.json()

        if data.get("rtcode") != "0000":
            continue

        for item in data.get("msgArray", []):
            out.append({
                "code": item.get("c"),
                "name": item.get("n"),
                "last": _safe_float(item.get("z")),
                "open": _safe_float(item.get("o")),
                "high": _safe_float(item.get("h")),
                "low": _safe_float(item.get("l")),
                "prev_close": _safe_float(item.get("y")),
                "vol_lots": _safe_int(item.get("v")),
                "tlong": _safe_int(item.get("tlong")),
            })

    return pd.DataFrame(out).drop_duplicates("code")

# =========================
# 3) 日線基準：yfinance（20日高點、均量、MA60、昨日爆量、近5日過熱）
# =========================
def _drop_today_bar_if_exists(df: pd.DataFrame, today_date) -> pd.DataFrame:
    if df.empty:
        return df
    last_date = pd.Timestamp(df.index[-1]).date()
    if last_date == today_date:
        return df.iloc[:-1].copy()
    return df

@st.cache_data(ttl=6 * 3600)
def build_daily_baselines(codes: list[str]) -> pd.DataFrame:
    end_date = now_taipei().date()
    start = (now_taipei() - timedelta(days=200)).date().isoformat()

    batch = 60
    records = []

    for i in range(0, len(codes), batch):
        chunk = codes[i:i + batch]
        tickers = " ".join([f"{c}.TW" for c in chunk])

        try:
            raw = yf.download(
                tickers=tickers,
                start=start,
                interval="1d",
                group_by="ticker",
                auto_adjust=False,
                threads=True,
                progress=False,
            )
        except Exception:
            continue

        for c in chunk:
            t = f"{c}.TW"
            try:
                if isinstance(raw.columns, pd.MultiIndex):
                    if t not in raw.columns.get_level_values(0):
                        continue
                    df = raw[t].dropna().copy()
                else:
                    df = raw.dropna().copy()

                df = _drop_today_bar_if_exists(df, end_date)
                if df.empty or len(df) < 80:
                    continue

                vol_ma20 = df["Volume"].rolling(20).mean().iloc[-1]
                high20 = df["High"].rolling(20).max().shift(1).iloc[-1]  # 排除今天
                ma60 = df["Close"].rolling(60).mean().iloc[-1]

                yday_vol = df["Volume"].iloc[-1]
                yday_close = df["Close"].iloc[-1]

                change_5d = None
                if len(df) >= 6:
                    change_5d = (df["Close"].iloc[-1] / df["Close"].iloc[-6]) - 1.0

                records.append({
                    "code": c,
                    "vol_ma20_shares": float(vol_ma20),
                    "high20": float(high20) if pd.notna(high20) else None,
                    "ma60": float(ma60) if pd.notna(ma60) else None,
                    "yday_vol_shares": float(yday_vol),
                    "yday_close": float(yday_close),
                    "change_5d": float(change_5d) if change_5d is not None else None,
                })
            except Exception:
                continue

    return pd.DataFrame(records).drop_duplicates("code")

# =========================
# 4) 核心掃描（盤中）
# =========================
def scan_intraday_breakouts(
    quotes: pd.DataFrame,
    base: pd.DataFrame,
    now_ts: datetime,
    *,
    breakout_buffer_pct: float,
    vol_mult: float,
    close_pos_min: float,
    upper_shadow_max: float,
    min_cum_lots: int,
    require_above_ma60: bool,
    avoid_yday_spike: bool,
    yday_spike_mult: float,
    avoid_overheat_5d: bool,
    overheat_5d_max: float,
    require_green_body: bool,
    body_min_pct: float,
):
    df = quotes.merge(base, on="code", how="inner").copy()
    df = df.dropna(subset=["last", "open", "high", "low", "high20", "vol_ma20_shares", "yday_close"])

    # 盤中累積量（張 -> 股）
    df["cum_vol_shares"] = df["vol_lots"].fillna(0).astype(float) * 1000.0

    # 突破：現價 > 前20日高 * (1+buffer)
    df["breakout_level"] = df["high20"] * (1.0 + breakout_buffer_pct / 100.0)
    df["cond_breakout"] = df["last"] > df["breakout_level"]

    # 收在高檔（避免衝高回落疲乏）
    rng = (df["high"] - df["low"]).replace(0, 1e-9)
    df["close_pos"] = (df["last"] - df["low"]) / rng
    df["cond_close_pos"] = df["close_pos"] >= close_pos_min

    # 上影線比例（正確算法：High - max(Open, Last)）
    df["real_body_top"] = df[["open", "last"]].max(axis=1)
    df["upper_shadow_ratio"] = (df["high"] - df["real_body_top"]) / rng
    df["cond_shadow"] = df["upper_shadow_ratio"] <= upper_shadow_max

    # 盤中實體強度
    df["body_return"] = (df["last"] - df["open"]) / df["open"]
    if require_green_body:
        df["cond_green_body"] = (df["last"] > df["open"]) & (df["body_return"] >= body_min_pct / 100.0)
    else:
        df["cond_green_body"] = True

    # 盤中爆量：同時間預期量（線性近似）
    elapsed = minutes_elapsed_in_session(now_ts)
    frac = max(1, min(270, elapsed)) / 270.0
    df["expected_vol_shares_now"] = df["vol_ma20_shares"] * frac
    df["vol_ratio_now"] = df["cum_vol_shares"] / (df["expected_vol_shares_now"] + 1e-9)
    df["cond_vol_burst"] = df["vol_ratio_now"] >= vol_mult

    # 最低流動性：累積量（張）
    df["cond_min_cum"] = df["vol_lots"].fillna(0).astype(int) >= int(min_cum_lots)

    # MA60（可選）
    if require_above_ma60:
        df = df.dropna(subset=["ma60"])
        df["cond_above_ma60"] = df["last"] > df["ma60"]
    else:
        df["cond_above_ma60"] = True

    # 昨日已爆量排除（避免第二根）
    if avoid_yday_spike:
        df["cond_yday_ok"] = df["yday_vol_shares"] <= (df["vol_ma20_shares"] * yday_spike_mult)
    else:
        df["cond_yday_ok"] = True

    # 近5日過熱排除（避免高位疲乏）
    if avoid_overheat_5d:
        df["cond_overheat_ok"] = df["change_5d"].fillna(0) <= overheat_5d_max / 100.0
    else:
        df["cond_overheat_ok"] = True

    cond = (
        df["cond_breakout"] &
        df["cond_vol_burst"] &
        df["cond_close_pos"] &
        df["cond_shadow"] &
        df["cond_min_cum"] &
        df["cond_above_ma60"] &
        df["cond_yday_ok"] &
        df["cond_overheat_ok"] &
        df["cond_green_body"]
    )

    out = df[cond].copy()
    if out.empty:
        return out

    out["chg_pct_vs_yday"] = (out["last"] / out["yday_close"] - 1.0) * 100.0

    # 簡單排序分數（你可再調權重）
    out["score"] = (
        2.0 * out["vol_ratio_now"].clip(0, 10) +
        1.5 * out["close_pos"].clip(0, 1) -
        1.0 * out["upper_shadow_ratio"].clip(0, 1) +
        0.3 * out["body_return"].clip(-1, 1)
    )

    out = out.sort_values(["score", "vol_ratio_now"], ascending=False)

    show = out[[
        "code", "name",
        "last", "chg_pct_vs_yday",
        "vol_lots", "vol_ratio_now",
        "high20", "breakout_level",
        "close_pos", "upper_shadow_ratio",
        "ma60", "change_5d",
        "score"
    ]].copy()

    show.rename(columns={
        "code": "代號",
        "name": "名稱",
        "last": "現價",
        "chg_pct_vs_yday": "較昨收(%)",
        "vol_lots": "累積量(張)",
        "vol_ratio_now": "盤中爆量倍數",
        "high20": "前20日高",
        "breakout_level": "突破門檻",
        "close_pos": "收在高檔(0-1)",
        "upper_shadow_ratio": "上影線比",
        "ma60": "MA60",
        "change_5d": "近5日漲幅",
        "score": "綜合分數",
    }, inplace=True)

    return show

# =========================
# Sidebar 參數
# =========================
st.sidebar.header("掃描參數")

universe_mode = st.sidebar.selectbox(
    "股票池模式",
    ["全上市（TWSE 全部上市公司）", "流動性預篩（更快）"],
    index=1
)

breakout_buffer_pct = st.sidebar.slider("突破緩衝(%)：現價需超過前20日高多少", 0.0, 3.0, 1.0, 0.1)
vol_mult = st.sidebar.slider("盤中爆量倍數：累積量/同時間預期量", 1.0, 6.0, 2.5, 0.1)
min_cum_lots = st.sidebar.slider("最低累積量(張)", 0, 20000, 2000, 500)

close_pos_min = st.sidebar.slider("收在高檔門檻(0-1)", 0.3, 0.95, 0.7, 0.05)
upper_shadow_max = st.sidebar.slider("上影線比上限(0-1)", 0.1, 0.9, 0.3, 0.05)

require_above_ma60 = st.sidebar.checkbox("要求現價在 MA60 之上", True)

avoid_yday_spike = st.sidebar.checkbox("排除：昨日已爆量（避免第二根）", True)
yday_spike_mult = st.sidebar.slider("昨日爆量倍數門檻", 1.0, 4.0, 1.8, 0.1)

avoid_overheat_5d = st.sidebar.checkbox("排除：近5日已過熱（避免疲乏）", True)
overheat_5d_max = st.sidebar.slider("近5日漲幅上限(%)", 5, 40, 18, 1)

require_green_body = st.sidebar.checkbox("要求綠K實體強度（盤中）", True)
body_min_pct = st.sidebar.slider("盤中實體漲幅下限(%)", 0.5, 8.0, 3.0, 0.5)

st.sidebar.markdown("---")
run_scan = st.sidebar.button("🚀 立即掃描（盤中）", use_container_width=True)
refresh_base = st.sidebar.button("🔄 重建日線基準快取（較慢）", use_container_width=True)

# =========================
# 主流程
# =========================
try:
    stock_df = fetch_all_twse_listed_stocks()
except Exception as e:
    st.error(f"抓上市清單失敗：{e}")
    st.stop()

all_codes = stock_df["code"].tolist()
st.caption(f"已載入上市公司數：{len(all_codes)} 檔（來源：TWSE OpenAPI）")

if refresh_base:
    build_daily_baselines.clear()
    st.success("已清除日線基準快取，下次掃描會重建。")

now_ts = now_taipei()
st.write(f"🕒 台北時間：**{now_ts.strftime('%Y-%m-%d %H:%M:%S')}**｜已過盤中分鐘：**{minutes_elapsed_in_session(now_ts)} / 270**")

# 股票池：全上市 or 預篩
base_df = None
codes_to_scan = all_codes

if universe_mode == "流動性預篩（更快）":
    with st.spinner("建立日線基準（用於預篩與指標）..."):
        base_df = build_daily_baselines(all_codes)

    # 預篩門檻：20日均量（股） >= 500,000（= 500 張/日）
    liq_threshold_shares = 500_000
    kept = base_df[base_df["vol_ma20_shares"] >= liq_threshold_shares]["code"].tolist()
    codes_to_scan = kept
    st.info(f"流動性預篩：保留 {len(codes_to_scan)} 檔（20日均量≥{liq_threshold_shares/1000:.0f} 張/日）")

if run_scan:
    # 1) 日線基準（必要）
    with st.spinner("取得日線基準（MA/20日高/均量/過熱/昨日爆量）..."):
        if base_df is None:
            base_df = build_daily_baselines(codes_to_scan)

    # 2) 盤中即時（MIS 批次）
    prog = st.progress(0, text="抓取盤中即時報價（分批）...")
    batch = 120
    quotes_parts = []
    total = len(codes_to_scan)

    for i in range(0, total, batch):
        chunk = codes_to_scan[i:i + batch]
        try:
            q = fetch_realtime_quotes_twse(chunk, batch_size=batch)
            if not q.empty:
                quotes_parts.append(q)
        except Exception:
            pass
        prog.progress(min(1.0, (i + batch) / total), text=f"抓取盤中即時報價：{min(i+batch,total)}/{total}")

    prog.empty()

    if not quotes_parts:
        st.error("盤中即時報價抓不到資料（MIS 可能暫時限制/網路問題）。稍後再試。")
        st.stop()

    quotes_df = pd.concat(quotes_parts, ignore_index=True)

    # 3) 掃描
    with st.spinner("運算突破/爆量/避雷條件..."):
        result = scan_intraday_breakouts(
            quotes_df,
            base_df,
            now_ts,
            breakout_buffer_pct=breakout_buffer_pct,
            vol_mult=vol_mult,
            close_pos_min=close_pos_min,
            upper_shadow_max=upper_shadow_max,
            min_cum_lots=min_cum_lots,
            require_above_ma60=require_above_ma60,
            avoid_yday_spike=avoid_yday_spike,
            yday_spike_mult=yday_spike_mult,
            avoid_overheat_5d=avoid_overheat_5d,
            overheat_5d_max=overheat_5d_max,
            require_green_body=require_green_body,
            body_min_pct=body_min_pct,
        )

    if result is None or len(result) == 0:
        st.warning("此刻沒有掃到符合你設定的『盤中起漲第一根』訊號。你可以放寬爆量倍數/突破緩衝/上影線限制再試。")
    else:
        st.success(f"🎯 掃到 {len(result)} 檔符合條件的標的")
        st.dataframe(
            result.style.format({
                "現價": "{:.2f}",
                "較昨收(%)": "{:.2f}",
                "盤中爆量倍數": "{:.2f}",
                "前20日高": "{:.2f}",
                "突破門檻": "{:.2f}",
                "收在高檔(0-1)": "{:.2f}",
                "上影線比": "{:.2f}",
                "MA60": "{:.2f}",
                "近5日漲幅": "{:.2%}",
                "綜合分數": "{:.2f}",
            }),
            use_container_width=True,
            height=560
        )

st.caption("資料來源：上市清單（TWSE OpenAPI t187ap03_L）；盤中即時（TWSE MIS getStockInfo）。")
