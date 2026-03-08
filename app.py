import html
import io
import math
import os
import re
import time
from copy import deepcopy
from datetime import datetime, timedelta, time as dtime, timezone
from collections import deque

import pandas as pd
import requests
import streamlit as st
import urllib3
import yfinance as yf
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================================================
# 基本設定
# ============================================================
APP_TITLE = "起漲戰情室 OMEGA"
APP_SUBTITLE = "v12.0 極速版｜官方資料優先｜熱門榜單搜尋｜歷史模擬測試"
FUGLE_API_KEY = "ZWJjZDhjZWYtMjhhMi00YWI2LTliNWQtMmViYzVhMmIzODdjIGY1N2Y0MGZmLWQ1MjgtNDk1OC1iZTljLWMxOWUwODQ4Y2U2Zg=="
API_TIMEOUT = (3.0, 10.0)
PUBLIC_TIMEOUT = (3.0, 12.0)
RAW_HISTORY_DAYS = 420
DEFAULT_COOLDOWN_SECONDS = 45
DEFAULT_TOP_VOLUME = 100
DEFAULT_TOP_MOVERS = 50
DEFAULT_MIN_BOARD = 1
DEFAULT_HOLD_DAYS = 5
MAX_CANDIDATES = 140
FINAL_ENRICH_LIMIT = 18
YF_DOWNLOAD_CHUNK = 45

# ============================================================
# 診斷
# ============================================================
def diag_init():
    return {
        "meta_count": 0,
        "rank_count": 0,
        "candidate_count": 0,
        "final_count": 0,
        "rank_src": "None",
        "snapshot_ok": 0,
        "snapshot_fail": 0,
        "snapshot_market_ok": 0,
        "quote_enrich_ok": 0,
        "quote_enrich_fail": 0,
        "public_rank_ok": 0,
        "public_rank_fail": 0,
        "yf_symbols": 0,
        "yf_returned": 0,
        "yf_parts_ok": 0,
        "yf_parts_fail": 0,
        "yf_fail": 0,
        "feature_ok": 0,
        "feature_fail": 0,
        "other_err": 0,
        "last_errors": deque(maxlen=12),
        "t_meta": 0.0,
        "t_snapshot": 0.0,
        "t_rank": 0.0,
        "t_features": 0.0,
        "t_enrich": 0.0,
        "t_filter": 0.0,
        "t_backtest": 0.0,
        "total": 0.0,
    }


def diag_err(diag, e, tag="ERR"):
    diag["last_errors"].append(f"[{tag}] {type(e).__name__}: {e}")


# ============================================================
# HTTP / SESSION
# ============================================================
def get_base_headers():
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145 Safari/537.36",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


def make_retry_session(base_headers=None, total=2, backoff=0.7, pool=20):
    s = requests.Session()
    retry = Retry(
        total=total,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
        respect_retry_after_header=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=pool, pool_maxsize=pool)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(get_base_headers())
    if base_headers:
        s.headers.update(base_headers)
    return s


# ============================================================
# 快取資料
# ============================================================
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_text(url: str):
    s = make_retry_session()
    r = s.get(url, timeout=PUBLIC_TIMEOUT, verify=False)
    r.raise_for_status()
    return r.text.replace("\r", "")


@st.cache_data(ttl=6 * 3600, show_spinner=False)
def get_stock_list():
    meta, errors = {}, []
    urls = [
        ("tse", "https://raw.githubusercontent.com/mlouielu/twstock/master/twstock/codes/twse_equities.csv"),
        ("otc", "https://raw.githubusercontent.com/mlouielu/twstock/master/twstock/codes/tpex_equities.csv"),
    ]
    for ex, url in urls:
        try:
            text = fetch_text(url)
            df = pd.read_csv(io.StringIO(text), dtype=str, engine="python", on_bad_lines="skip")
            cols = {c.strip().lower(): c for c in df.columns}
            c_col = cols.get("code") or df.columns[1]
            n_col = cols.get("name") or df.columns[2]
            t_col = cols.get("type")
            for _, row in df.iterrows():
                stype = str(row.get(t_col, "")) if t_col else ""
                if t_col and ("ETF" in stype or "權證" in stype or "受益證券" in stype):
                    continue
                code = str(row.get(c_col, "")).strip()
                if len(code) == 4 and code.isdigit():
                    meta[code] = {
                        "name": str(row.get(n_col, "")).strip(),
                        "ex": ex,
                        "market": "上市" if ex == "tse" else "上櫃",
                    }
        except Exception as e:
            errors.append(f"{ex}: {e}")
    return meta, errors


@st.cache_data(ttl=6 * 3600, show_spinner=False)
def yf_download_daily(symbols, period="420d"):
    if not symbols:
        return pd.DataFrame()
    data = yf.download(
        tickers=" ".join(symbols),
        period=period,
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        actions=False,
        threads=True,
        progress=False,
        multi_level_index=True,
        timeout=15,
    )
    if data is None or getattr(data, "empty", False):
        return pd.DataFrame()
    if not isinstance(data.columns, pd.MultiIndex):
        t = symbols[0]
        data.columns = pd.MultiIndex.from_product([[t], data.columns])
    data = data.loc[~data.index.duplicated(keep="last")]
    data = data.sort_index()
    return data


# ============================================================
# 基本工具
# ============================================================
def now_taipei():
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=8)


def idx_date_taipei(idx):
    try:
        ts = pd.Timestamp(idx)
        if getattr(ts, "tzinfo", None) is not None:
            ts = ts.tz_convert("Asia/Taipei")
        return ts.date()
    except Exception:
        try:
            if getattr(idx, "tz", None) is not None:
                return idx.tz_convert("Asia/Taipei").date()
        except Exception:
            pass
        return pd.Timestamp(idx).date()


def tw_tick(price):
    if price < 10:
        return 0.01
    if price < 50:
        return 0.05
    if price < 100:
        return 0.1
    if price < 500:
        return 0.5
    if price < 1000:
        return 1.0
    return 5.0


def calc_limit_up(prev_close, limit_pct=0.10):
    raw = float(prev_close) * (1.0 + limit_pct)
    tick = tw_tick(raw)
    n = math.floor((raw + 1e-12) / tick)
    if tick < 0.1:
        digits = 2
    elif tick < 1:
        digits = 1
    else:
        digits = 0
    return round(n * tick, digits)


def safe_float(x, default=0.0):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default


def safe_int(x, default=0):
    try:
        if x is None or x == "":
            return default
        return int(float(x))
    except Exception:
        return default


def stable_unique(seq):
    out, seen = [], set()
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def symbol_of(code, meta_dict):
    return f"{code}.{'TW' if meta_dict[code]['ex'] == 'tse' else 'TWO'}"


def market_of(code, meta_dict):
    return meta_dict.get(code, {}).get("market", "上市")


def market_label(m):
    return m


def copy_diag(diag):
    d = deepcopy(diag)
    if not isinstance(d.get("last_errors"), deque):
        d["last_errors"] = deque(d.get("last_errors", []), maxlen=12)
    return d


# ============================================================
# API 資料讀取
# ============================================================
def get_api_key():
    key = ""
    try:
        key = st.secrets.get("FUGLE_API_KEY", "")
    except Exception:
        key = ""
    if not key:
        key = os.getenv("FUGLE_API_KEY", "")
    if not key:
        key = FUGLE_API_KEY
    return str(key).strip()


def fugle_get_json(session, path, api_key, params=None):
    url = f"https://api.fugle.tw/marketdata/v1.0/stock/{path}"
    headers = {"X-API-KEY": api_key}
    r = session.get(url, headers=headers, params=params or {}, timeout=API_TIMEOUT)
    return r


def snapshot_quotes_market(session, api_key, market, diag):
    r = fugle_get_json(session, f"snapshot/quotes/{market}", api_key, params={"type": "COMMONSTOCK"})
    if r.status_code == 200:
        diag["snapshot_market_ok"] += 1
        return r.json()
    if r.status_code in (401, 403, 429):
        raise RuntimeError(f"SNAPSHOT_{market}_{r.status_code}")
    raise RuntimeError(f"SNAPSHOT_{market}_{r.status_code}")


def enrich_quotes_for_codes(session, api_key, codes, diag):
    enriched = {}
    for code in codes:
        try:
            r = fugle_get_json(session, f"intraday/quote/{code}", api_key)
            if r.status_code != 200:
                diag["quote_enrich_fail"] += 1
                diag_err(diag, Exception(f"HTTP_{r.status_code} {code}"), "QUOTE_ENRICH")
                continue
            j = r.json()
            bids = j.get("bids", []) or []
            asks = j.get("asks", []) or []
            top_bid = bids[0] if bids else {}
            top_ask = asks[0] if asks else {}
            enriched[code] = {
                "best_bid": safe_float(top_bid.get("price"), 0.0),
                "best_bid_size": safe_int(top_bid.get("size"), 0),
                "best_ask": safe_float(top_ask.get("price"), 0.0),
                "best_ask_size": safe_int(top_ask.get("size"), 0),
            }
            diag["quote_enrich_ok"] += 1
            time.sleep(0.06)
        except Exception as e:
            diag["quote_enrich_fail"] += 1
            diag_err(diag, e, "QUOTE_ENRICH")
    return enriched


# ============================================================
# 公開榜單備援
# ============================================================
def fetch_public_ranking(diag):
    session = make_retry_session()
    ordered = []

    def push(items, limit=None):
        nonlocal ordered
        if limit is not None:
            items = items[:limit]
        ordered = stable_unique(ordered + [x for x in items if len(x) == 4 and x.isdigit()])

    got_any = False
    try:
        r = session.get("https://tw.stock.yahoo.com/rank/volume?exchange=ALL", timeout=PUBLIC_TIMEOUT, verify=True)
        r.raise_for_status()
        tks = re.findall(r"/quote/([0-9]{4})", r.text)
        if tks:
            push(tks, DEFAULT_TOP_VOLUME)
            got_any = True
            diag["public_rank_ok"] += 1
    except Exception as e:
        diag["public_rank_fail"] += 1
        diag_err(diag, e, "PUB_YAHOO_VOL")

    try:
        r = session.get("https://tw.stock.yahoo.com/rank/change-up?exchange=ALL", timeout=PUBLIC_TIMEOUT, verify=True)
        r.raise_for_status()
        tks = re.findall(r"/quote/([0-9]{4})", r.text)
        if tks:
            push(tks, DEFAULT_TOP_MOVERS)
            got_any = True
            diag["public_rank_ok"] += 1
    except Exception as e:
        diag["public_rank_fail"] += 1
        diag_err(diag, e, "PUB_YAHOO_UP")

    if len(ordered) < 40:
        try:
            r = session.get("https://www.wantgoo.com/stock/ranking/volume", timeout=PUBLIC_TIMEOUT, verify=True)
            r.raise_for_status()
            tks = re.findall(r"/stock/([0-9]{4})", r.text)
            if tks:
                push(tks, 100)
                got_any = True
                diag["public_rank_ok"] += 1
        except Exception as e:
            diag["public_rank_fail"] += 1
            diag_err(diag, e, "PUB_WANTGOO")

    if not got_any:
        raise RuntimeError("PUBLIC_RANK_ALL_FAILED")

    diag["rank_src"] = "網路公開排行榜"
    return ordered[:MAX_CANDIDATES]


# ============================================================
# 官方全市場資料優先
# ============================================================
def build_quotes_from_snapshot(snapshot_json, market, meta_dict):
    rows = []
    for item in snapshot_json.get("data", []) or []:
        code = str(item.get("symbol", "")).strip()
        if code not in meta_dict:
            continue
        last = safe_float(item.get("closePrice"), 0.0)
        high = safe_float(item.get("highPrice"), last)
        low = safe_float(item.get("lowPrice"), last)
        open_ = safe_float(item.get("openPrice"), last)
        vol = safe_int(item.get("tradeVolume"), 0)
        val = safe_float(item.get("tradeValue"), 0.0)
        chg = safe_float(item.get("change"), 0.0)
        chg_pct = safe_float(item.get("changePercent"), 0.0)
        prev_close = last - chg if last > 0 else 0.0
        if prev_close <= 0:
            continue
        upper = calc_limit_up(prev_close)
        dist_pct = max(0.0, (upper - last) / max(upper, 1e-9) * 100.0)
        rows.append(
            {
                "code": code,
                "name": meta_dict[code]["name"],
                "market": market,
                "open": open_,
                "high": high,
                "low": low,
                "last": last,
                "vol_sh": vol,
                "trade_value": val,
                "change": chg,
                "change_pct": chg_pct,
                "prev_close": prev_close,
                "upper": upper,
                "dist": dist_pct,
                "last_updated": safe_int(item.get("lastUpdated"), 0),
            }
        )
    return pd.DataFrame(rows)


def fetch_market_snapshot_and_rank(meta_dict, api_key, diag, status_placeholder):
    t0 = time.perf_counter()
    session = make_retry_session()
    quotes_frames = []
    for market, m_label in zip(("TSE", "OTC"), ("上市", "上櫃")):
        status_placeholder.update(label=f"⚡ 讀取 {m_label} 官方資料中...", state="running")
        try:
            snap = snapshot_quotes_market(session, api_key, market, diag)
            quotes_frames.append(build_quotes_from_snapshot(snap, m_label, meta_dict))
            diag["snapshot_ok"] += 1
        except Exception as e:
            diag["snapshot_fail"] += 1
            diag_err(diag, e, f"SNAPSHOT_{market}")
    diag["t_snapshot"] = time.perf_counter() - t0

    if not quotes_frames:
        raise RuntimeError("SNAPSHOT_ALL_FAILED")

    quotes_df = pd.concat(quotes_frames, ignore_index=True)
    quotes_df = quotes_df.drop_duplicates("code", keep="first")

    vol_top = quotes_df.sort_values(["vol_sh", "trade_value"], ascending=[False, False])["code"].head(DEFAULT_TOP_VOLUME).tolist()
    mover_top = quotes_df.sort_values(["change_pct", "trade_value"], ascending=[False, False])["code"].head(DEFAULT_TOP_MOVERS).tolist()
    ranked_codes = stable_unique(vol_top + mover_top)[:MAX_CANDIDATES]

    candidate_df = quotes_df[quotes_df["code"].isin(ranked_codes)].copy()
    order_map = {c: i for i, c in enumerate(ranked_codes)}
    candidate_df["rank_order"] = candidate_df["code"].map(order_map)
    candidate_df = candidate_df.sort_values(["rank_order", "dist", "vol_sh"], ascending=[True, True, False]).reset_index(drop=True)

    diag["rank_src"] = "官方全市場最新資料"
    diag["rank_count"] = len(ranked_codes)
    diag["candidate_count"] = len(candidate_df)
    diag["t_rank"] = max(diag.get("t_rank", 0.0), time.perf_counter() - t0)
    return candidate_df, ranked_codes


def fetch_candidate_rows_by_public_rank(meta_dict, api_key, diag, status_placeholder):
    t0 = time.perf_counter()
    ranked_codes = fetch_public_ranking(diag)
    session = make_retry_session()
    rows = []

    for idx, code in enumerate(ranked_codes, start=1):
        if code not in meta_dict:
            continue
        try:
            if idx <= 35:
                sleep_sec = 0.05
                stage = "⚡ 快速掃描最熱門的股票"
            elif idx <= 80:
                sleep_sec = 0.18
                stage = "🛰️ 掃描其他中段班股票"
            else:
                sleep_sec = 0.30
                stage = "🛡️ 慢慢掃描後段班股票"
            status_placeholder.update(label=f"{stage}... ({idx}/{len(ranked_codes)})", state="running")
            r = fugle_get_json(session, f"intraday/quote/{code}", api_key)
            if r.status_code != 200:
                diag["snapshot_fail"] += 1
                diag_err(diag, Exception(f"HTTP_{r.status_code} {code}"), "PUBLIC_QUOTE")
                time.sleep(min(0.6, sleep_sec + 0.1))
                continue
            j = r.json()
            ref = safe_float(j.get("referencePrice"), 0.0)
            last = safe_float(j.get("closePrice"), ref)
            high = safe_float(j.get("highPrice"), last)
            low = safe_float(j.get("lowPrice"), last)
            open_ = safe_float(j.get("openPrice"), ref)
            vol = safe_int((j.get("total") or {}).get("tradeVolume"), 0)
            bids = j.get("bids", []) or []
            asks = j.get("asks", []) or []
            best_bid = safe_float(bids[0].get("price"), 0.0) if bids else 0.0
            best_bid_size = safe_int(bids[0].get("size"), 0) if bids else 0
            best_ask = safe_float(asks[0].get("price"), 0.0) if asks else 0.0
            best_ask_size = safe_int(asks[0].get("size"), 0) if asks else 0
            if ref <= 0 or last <= 0:
                continue
            upper = calc_limit_up(ref)
            rows.append(
                {
                    "code": code,
                    "name": meta_dict[code]["name"],
                    "market": market_of(code, meta_dict),
                    "open": open_,
                    "high": high,
                    "low": low,
                    "last": last,
                    "vol_sh": vol,
                    "trade_value": 0.0,
                    "change": last - ref,
                    "change_pct": ((last - ref) / ref * 100.0) if ref else 0.0,
                    "prev_close": ref,
                    "upper": upper,
                    "dist": max(0.0, (upper - last) / max(upper, 1e-9) * 100.0),
                    "last_updated": 0,
                    "best_bid": best_bid,
                    "best_bid_size": best_bid_size,
                    "best_ask": best_ask,
                    "best_ask_size": best_ask_size,
                    "rank_order": idx - 1,
                }
            )
            diag["snapshot_ok"] += 1
        except Exception as e:
            diag["snapshot_fail"] += 1
            diag_err(diag, e, "PUBLIC_QUOTE")
        time.sleep(sleep_sec)

    df = pd.DataFrame(rows).drop_duplicates("code", keep="first") if rows else pd.DataFrame()
    diag["rank_count"] = len(ranked_codes)
    diag["candidate_count"] = len(df)
    diag["t_rank"] = time.perf_counter() - t0
    return df, ranked_codes


# ============================================================
# 歷史表現預先計算
# ============================================================
def _extract_symbol_frame(raw_daily, sym):
    if raw_daily is None or getattr(raw_daily, "empty", False):
        return pd.DataFrame()
    if isinstance(raw_daily.columns, pd.MultiIndex):
        if sym not in raw_daily.columns.get_level_values(0):
            return pd.DataFrame()
        return raw_daily[sym].copy()
    return raw_daily.copy()


def _consecutive_limit_ups(past_df, tail_n=12):
    if len(past_df) < 2:
        return 0
    streak = 0
    tail = past_df.tail(tail_n)
    for i in range(len(tail) - 1, 0, -1):
        cp = safe_float(tail["Close"].iloc[i], 0.0)
        pp = safe_float(tail["Close"].iloc[i - 1], 0.0)
        if cp <= 0 or pp <= 0:
            break
        lim = calc_limit_up(pp)
        if cp >= lim - tw_tick(lim):
            streak += 1
        else:
            break
    return streak


def compute_feature_cache(candidate_df, meta_dict, diag, status_placeholder, period="420d"):
    t0 = time.perf_counter()
    if candidate_df.empty:
        return {}, pd.DataFrame()

    codes = [c for c in candidate_df["code"].tolist() if c in meta_dict]
    syms = [symbol_of(c, meta_dict) for c in codes]
    diag["yf_symbols"] = len(syms)

    raw_parts = []
    for i in range(0, len(syms), YF_DOWNLOAD_CHUNK):
        part = syms[i : i + YF_DOWNLOAD_CHUNK]
        status_placeholder.update(label=f"📚 正在下載過去的表現紀錄... ({min(i + len(part), len(syms))}/{len(syms)})", state="running")
        try:
            part_df = yf_download_daily(part, period=period)
            if part_df is not None and not getattr(part_df, "empty", False):
                raw_parts.append(part_df)
                diag["yf_parts_ok"] += 1
            else:
                diag["yf_parts_fail"] += 1
        except Exception as e:
            diag["yf_parts_fail"] += 1
            diag_err(diag, e, "YF_PART")

    if not raw_parts:
        diag["t_features"] = time.perf_counter() - t0
        return {}, pd.DataFrame()

    raw_daily = pd.concat(raw_parts, axis=1)
    if isinstance(raw_daily.columns, pd.MultiIndex):
        raw_daily = raw_daily.loc[:, ~raw_daily.columns.duplicated()]
        diag["yf_returned"] = int(raw_daily.columns.get_level_values(0).nunique())
    else:
        diag["yf_returned"] = 1
    raw_daily = raw_daily.loc[~raw_daily.index.duplicated(keep="last")].sort_index()

    today_date = now_taipei().date()
    features = {}

    for code in codes:
        sym = symbol_of(code, meta_dict)
        try:
            df = _extract_symbol_frame(raw_daily, sym)
            if df.empty or not {"Close", "Volume", "High", "Low", "Open"}.issubset(set(df.columns)):
                diag["feature_fail"] += 1
                continue
            df = df[["Open", "High", "Low", "Close", "Volume"]].dropna().copy()
            dates_tw = pd.Index([idx_date_taipei(x) for x in df.index])
            past_df = df[dates_tw < today_date].copy()
            if len(past_df) < 35:
                diag["feature_fail"] += 1
                continue

            close = past_df["Close"].astype(float)
            vol = past_df["Volume"].astype(float)
            high = past_df["High"].astype(float)
            low = past_df["Low"].astype(float)

            vol_ma20 = safe_float(vol.rolling(20).mean().iloc[-1], 0.0)
            high_52w = safe_float(high.tail(252).max(), 0.0)
            board_streak = _consecutive_limit_ups(past_df, tail_n=12)
            prev_close_hist = safe_float(close.iloc[-1], 0.0)
            atr20 = safe_float((high - low).rolling(20).mean().iloc[-1], 0.0)
            ret5 = safe_float((close.iloc[-1] / close.iloc[-6] - 1) * 100.0, 0.0) if len(close) >= 6 and close.iloc[-6] > 0 else 0.0
            ret20 = safe_float((close.iloc[-1] / close.iloc[-21] - 1) * 100.0, 0.0) if len(close) >= 21 and close.iloc[-21] > 0 else 0.0

            features[code] = {
                "vol_ma20": vol_ma20,
                "high_52w": high_52w,
                "board_streak": board_streak,
                "prev_close_hist": prev_close_hist,
                "atr20": atr20,
                "ret5": ret5,
                "ret20": ret20,
            }
            diag["feature_ok"] += 1
        except Exception as e:
            diag["feature_fail"] += 1
            diag_err(diag, e, "FEATURE")

    diag["t_features"] = time.perf_counter() - t0
    return features, raw_daily


# ============================================================
# 篩選標準（即時過濾，不重複下載）
# ============================================================
def intraday_progress_fraction(now_ts):
    m = int((datetime.combine(now_ts.date(), now_ts.time()) - datetime.combine(now_ts.date(), dtime(9, 0))).total_seconds() // 60)
    m = max(0, min(270, m))
    if m <= 30:
        return 0.12
    if m <= 120:
        return 0.12 + (0.50 - 0.12) * ((m - 30) / 90.0)
    return min(1.0, 0.50 + (1.00 - 0.50) * ((m - 120) / 150.0))


def get_thresholds(now_ts, is_test=False):
    m = int((datetime.combine(now_ts.date(), now_ts.time()) - datetime.combine(now_ts.date(), dtime(9, 0))).total_seconds() // 60)
    m = max(0, min(270, m))
    if is_test:
        return {"dist_limit": 100.0, "vol_limit": 0, "pullback_lim": 1.0, "close_pos_min": 0.0, "vol_ratio_min": 0.0}
    if m <= 60:
        dist_limit = 3.2
    elif m <= 180:
        dist_limit = 2.2
    else:
        dist_limit = 1.5
    return {
        "dist_limit": dist_limit,
        "vol_limit": 800_000,
        "pullback_lim": 0.012 if m <= 90 else 0.0042,
        "close_pos_min": 0.80,
        "vol_ratio_min": 1.30,
    }


def score_to_star_count(signal_score, dist_pct, vol_ratio, board_streak, close_pos, proximity_52w, status_text=""):
    stars = 1
    if signal_score >= 8.8:
        stars = 5
    elif signal_score >= 7.0:
        stars = 4
    elif signal_score >= 5.4:
        stars = 3
    elif signal_score >= 4.0:
        stars = 2

    bonus = 0.0
    if dist_pct <= 0.20:
        bonus += 1.0
    elif dist_pct <= 0.50:
        bonus += 0.5

    if vol_ratio >= 3.0:
        bonus += 1.0
    elif vol_ratio >= 2.0:
        bonus += 0.5

    if board_streak >= 2:
        bonus += 1.0
    elif board_streak >= 1:
        bonus += 0.5

    if close_pos >= 0.95:
        bonus += 0.5
    elif close_pos < 0.85:
        bonus -= 0.5

    if proximity_52w >= 95:
        bonus += 0.5
    elif proximity_52w < 85:
        bonus -= 0.25

    if "最高價" in str(status_text) or "鎖" in str(status_text):
        bonus += 0.5

    stars = int(round(stars + bonus * 0.5))
    return max(1, min(5, stars))


def render_star_bar(stars):
    stars = max(1, min(5, int(stars)))
    return "★" * stars + "☆" * (5 - stars)


def compute_feature_from_history(df, today_date):
    if df is None or getattr(df, "empty", False):
        return None
    if not {"Close", "Volume", "High", "Low", "Open"}.issubset(set(df.columns)):
        return None
    df = df[["Open", "High", "Low", "Close", "Volume"]].dropna().copy()
    if df.empty:
        return None
    dates_tw = pd.Index([idx_date_taipei(x) for x in df.index])
    past_df = df[dates_tw < today_date].copy()
    if len(past_df) < 35:
        return None

    close = past_df["Close"].astype(float)
    vol = past_df["Volume"].astype(float)
    high = past_df["High"].astype(float)
    low = past_df["Low"].astype(float)
    return {
        "vol_ma20": safe_float(vol.rolling(20).mean().iloc[-1], 0.0),
        "high_52w": safe_float(high.tail(252).max(), 0.0),
        "board_streak": _consecutive_limit_ups(past_df, tail_n=12),
        "prev_close_hist": safe_float(close.iloc[-1], 0.0),
        "atr20": safe_float((high - low).rolling(20).mean().iloc[-1], 0.0),
        "ret5": safe_float((close.iloc[-1] / close.iloc[-6] - 1) * 100.0, 0.0) if len(close) >= 6 and close.iloc[-6] > 0 else 0.0,
        "ret20": safe_float((close.iloc[-1] / close.iloc[-21] - 1) * 100.0, 0.0) if len(close) >= 21 and close.iloc[-21] > 0 else 0.0,
    }


def resolve_stock_query(query, meta_dict):
    q = str(query or "").strip()
    if not q:
        return None, []
    nq = re.sub(r"\s+", "", q).upper()
    if nq.isdigit() and len(nq) == 4 and nq in meta_dict:
        return nq, []

    exact_name = [code for code, info in meta_dict.items() if re.sub(r"\s+", "", str(info.get("name", ""))).upper() == nq]
    if len(exact_name) == 1:
        return exact_name[0], []

    prefix = []
    partial = []
    for code, info in meta_dict.items():
        name_norm = re.sub(r"\s+", "", str(info.get("name", ""))).upper()
        if code.startswith(nq) or name_norm.startswith(nq):
            prefix.append(code)
        elif nq in code or nq in name_norm:
            partial.append(code)
    matches = stable_unique(exact_name + prefix + partial)
    if len(matches) == 1:
        return matches[0], []
    return None, matches[:8]


def fetch_single_quote_row(session, api_key, code, meta_dict):
    r = fugle_get_json(session, f"intraday/quote/{code}", api_key)
    if r.status_code != 200:
        raise RuntimeError(f"QUOTE_{code}_{r.status_code}")
    j = r.json()
    ref = safe_float(j.get("referencePrice"), 0.0)
    last = safe_float(j.get("closePrice"), ref)
    high = safe_float(j.get("highPrice"), last)
    low = safe_float(j.get("lowPrice"), last)
    open_ = safe_float(j.get("openPrice"), ref)
    vol = safe_int((j.get("total") or {}).get("tradeVolume"), 0)
    bids = j.get("bids", []) or []
    asks = j.get("asks", []) or []
    best_bid = safe_float(bids[0].get("price"), 0.0) if bids else 0.0
    best_bid_size = safe_int(bids[0].get("size"), 0) if bids else 0
    best_ask = safe_float(asks[0].get("price"), 0.0) if asks else 0.0
    best_ask_size = safe_int(asks[0].get("size"), 0) if asks else 0
    if ref <= 0 or last <= 0:
        raise RuntimeError(f"QUOTE_{code}_EMPTY")
    upper = calc_limit_up(ref)
    return {
        "code": code,
        "name": meta_dict[code]["name"],
        "market": market_of(code, meta_dict),
        "open": open_,
        "high": high,
        "low": low,
        "last": last,
        "vol_sh": vol,
        "trade_value": 0.0,
        "change": last - ref,
        "change_pct": ((last - ref) / ref * 100.0) if ref else 0.0,
        "prev_close": ref,
        "upper": upper,
        "dist": max(0.0, (upper - last) / max(upper, 1e-9) * 100.0),
        "last_updated": 0,
        "best_bid": best_bid,
        "best_bid_size": best_bid_size,
        "best_ask": best_ask,
        "best_ask_size": best_ask_size,
        "rank_order": 9999,
    }


def evaluate_candidate_record(r, feat, now_ts, is_test, use_bloodline, only_tse, min_board):
    code = r["code"]
    name = r["name"]
    market = r.get("market", "上市")
    if only_tse and market != "上市":
        return {"passed": False, "reason_key": "市場不符", "reason_text": "目前設定只看上市", "item": None}
    if not feat:
        return {"passed": False, "reason_key": "資訊不足", "reason_text": "缺少過去的表現資料", "item": None}

    vol_ma20 = safe_float(feat.get("vol_ma20"), 0.0)
    board_streak = safe_int(feat.get("board_streak"), 0)
    high_52w = safe_float(feat.get("high_52w"), 0.0)
    ret5 = safe_float(feat.get("ret5"), 0.0)
    ret20 = safe_float(feat.get("ret20"), 0.0)
    if vol_ma20 <= 0:
        return {"passed": False, "reason_key": "資訊不足", "reason_text": "過去 20 天的交易量資料不足", "item": None}

    th = get_thresholds(now_ts, is_test=is_test)
    frac = intraday_progress_fraction(now_ts)
    vol_ratio_live = r["vol_sh"] / max(vol_ma20 * (1.0 if is_test else frac), 1e-9)
    rng = max(r["high"] - r["low"], 0.0)
    pullback = (r["high"] - r["last"]) / max(r["high"], 1e-9)
    close_pos = 1.0 if rng < 1e-9 else (r["last"] - r["low"]) / max(rng, 1e-9)

    bid_price = safe_float(r.get("best_bid", 0.0), 0.0)
    bid_size = safe_int(r.get("best_bid_size", 0), 0)
    near_limit = r["last"] >= r["upper"] - tw_tick(r["upper"])
    hard_locked = near_limit and bid_price >= r["upper"] - tw_tick(r["upper"]) and bid_size >= (80000 if r["last"] < 50 else 120000 if r["last"] < 100 else 200000)
    proximity_52w = (r["last"] / max(high_52w, 1e-9) * 100.0) if high_52w > 0 else 0.0

    score = 0.0
    score += min(3.0, max(0.0, 3.0 - r["dist"] * 1.4))
    score += min(3.0, max(0.0, vol_ratio_live - 1.0))
    score += 1.0 if close_pos >= 0.92 else 0.5 if close_pos >= 0.85 else 0.0
    score += min(2.0, board_streak * 0.9)
    score += 0.8 if proximity_52w >= 92 else 0.3 if proximity_52w >= 85 else 0.0
    score += 0.4 if ret5 > 0 else 0.0
    score += 0.4 if ret20 > 0 else 0.0
    signal_score = min(10.0, round(score, 2))

    status = "🔒 漲到頂買不到" if hard_locked else "🟣 快漲到最高價" if near_limit else "⚡ 強力上漲中"
    star_count = score_to_star_count(
        signal_score=signal_score,
        dist_pct=r["dist"],
        vol_ratio=vol_ratio_live,
        board_streak=board_streak,
        close_pos=close_pos,
        proximity_52w=proximity_52w,
        status_text=status,
    )
    item = {
        "代號": code,
        "名稱": name,
        "市場": market,
        "現價": r["last"],
        "距離最高價%": r["dist"],
        "交易熱度": vol_ratio_live,
        "今日表現分數": signal_score,
        "推薦星等": star_count,
        "推薦指數": render_star_bar(star_count),
        "狀態": status,
        "階段": f"過去連續大漲 {board_streak} 天",
        "board_val": board_streak,
        "close_pos": close_pos,
        "pullback": pullback,
        "接近一年最高價%": proximity_52w,
        "近5天表現%": ret5,
        "近20天表現%": ret20,
        "best_bid": bid_price,
        "best_bid_size": bid_size,
        "best_ask": safe_float(r.get("best_ask", 0.0), 0.0),
        "best_ask_size": safe_int(r.get("best_ask_size", 0), 0),
        "成交量": safe_int(r.get("vol_sh", 0), 0),
    }

    if r["dist"] > th["dist_limit"] or r["vol_sh"] < th["vol_limit"]:
        return {"passed": False, "reason_key": "未達基本熱門門檻", "reason_text": "未達到基本篩選條件", "item": item}
    if vol_ratio_live < th["vol_ratio_min"]:
        return {"passed": False, "reason_key": "買賣不夠熱絡", "reason_text": "即時的交易熱度不夠", "item": item}
    if pullback > th["pullback_lim"]:
        return {"passed": False, "reason_key": "從高點掉下來太多", "reason_text": "目前價格已經從今天最高點掉落太多", "item": item}
    if close_pos < th["close_pos_min"] and rng > max(0.1, r["last"] * 0.002):
        return {"passed": False, "reason_key": "目前價格相對弱勢", "reason_text": "今天收盤位置處在相對不夠強勢的地方", "item": item}
    if use_bloodline and not is_test and board_streak < min_board:
        return {"passed": False, "reason_key": "過去沒有連續大漲紀錄", "reason_text": f"過去連續大漲不到 {min_board} 天", "item": item}
    return {"passed": True, "reason_key": "通過", "reason_text": "符合當前所有嚴格條件", "item": item}


def evaluate_single_search(query, meta_dict, api_key, now_ts, is_test, use_bloodline, min_board, vault=None):
    code, matches = resolve_stock_query(query, meta_dict)
    if not code:
        if matches:
            return {
                "ok": False,
                "kind": "ambiguous",
                "message": "找到多個類似的目標，請輸入更完整的股票代號或名稱。",
                "matches": [{"code": c, "name": meta_dict[c]["name"], "market": market_label(meta_dict[c]["market"])} for c in matches],
            }
        return {"ok": False, "kind": "not_found", "message": "找不到這支股票，請確認代號或名稱是否正確。", "matches": []}

    row = None
    feat = None
    source = []
    if vault:
        cdf = vault.get("candidate_df")
        if cdf is not None and not getattr(cdf, "empty", False):
            hit = cdf[cdf["code"] == code]
            if not hit.empty:
                row = hit.iloc[0].to_dict()
                source.append("已下載好的資料庫")
        feat = (vault.get("feature_cache") or {}).get(code)
        if feat:
            source.append("已計算過的過去表現")

    if row is None:
        session = make_retry_session()
        row = fetch_single_quote_row(session, api_key, code, meta_dict)
        source.append("即時查詢最新報價")

    if feat is None:
        sym = symbol_of(code, meta_dict)
        raw_daily = yf_download_daily([sym], period=f"{RAW_HISTORY_DAYS}d")
        df = _extract_symbol_frame(raw_daily, sym)
        feat = compute_feature_from_history(df, now_ts.date())
        source.append("剛下載好的歷史資料")

    assessment = evaluate_candidate_record(
        r=row,
        feat=feat,
        now_ts=now_ts,
        is_test=is_test,
        use_bloodline=use_bloodline,
        only_tse=False,
        min_board=min_board,
    )
    return {
        "ok": True,
        "kind": "result",
        "code": code,
        "name": meta_dict[code]["name"],
        "market": market_label(meta_dict[code]["market"]),
        "assessment": assessment,
        "source": " / ".join(source),
    }


def apply_dynamic_filters(raw_df, feature_cache, now_ts, is_test, use_bloodline, only_tse, min_board, base_diag):
    diag = copy_diag(base_diag)
    stats = {"候選總數": 0, "買賣不夠熱絡": [], "從高點掉下來太多": [], "目前價格相對弱勢": [], "過去沒有連續大漲紀錄": [], "資訊不足": [], "未達基本熱門門檻": [], "市場不符": []}
    if raw_df is None or raw_df.empty:
        return pd.DataFrame(), stats, diag

    th = get_thresholds(now_ts, is_test=is_test)
    work = raw_df.copy()
    if only_tse:
        work = work[work["market"] == "上市"].copy()

    work = work[(work["dist"] <= th["dist_limit"]) & (work["vol_sh"] >= th["vol_limit"])].copy()
    stats["候選總數"] = len(work)
    if work.empty:
        diag["final_count"] = 0
        return pd.DataFrame(), stats, diag

    out = []
    for _, r in work.iterrows():
        assessment = evaluate_candidate_record(
            r=r,
            feat=feature_cache.get(r["code"]),
            now_ts=now_ts,
            is_test=is_test,
            use_bloodline=use_bloodline,
            only_tse=only_tse,
            min_board=min_board,
        )
        if not assessment.get("passed"):
            reason_key = assessment.get("reason_key", "資訊不足")
            if reason_key not in stats:
                stats[reason_key] = []
            stats[reason_key].append(f"{r['code']} {r['name']}")
            if reason_key == "資訊不足":
                diag["yf_fail"] += 1
            continue
        out.append(assessment["item"])

    res = pd.DataFrame(out)
    if not res.empty:
        res = res.sort_values(["推薦星等", "今日表現分數", "board_val", "交易熱度", "距離最高價%"], ascending=[False, False, False, False, True]).reset_index(drop=True)
    diag["final_count"] = len(res)
    return res, stats, diag


# ============================================================
# 歷史模擬驗證
# ============================================================
def pick_backtest_universe(raw_df, top_n=16):
    if raw_df is None or raw_df.empty:
        return []
    df = raw_df.sort_values(["dist", "vol_sh"], ascending=[True, False]).head(top_n)
    return df["code"].tolist()


def run_surrogate_backtest(raw_daily, universe_codes, meta_dict, lookback_days=126, hold_days=5, use_bloodline=True, min_board=1):
    trades = []
    if raw_daily is None or getattr(raw_daily, "empty", False) or not universe_codes:
        return pd.DataFrame(), {
            "signals": 0,
            "wins": 0,
            "win_rate": 0.0,
            "avg_return": 0.0,
            "median_return": 0.0,
            "best": 0.0,
            "worst": 0.0,
        }

    for code in universe_codes:
        if code not in meta_dict:
            continue
        sym = symbol_of(code, meta_dict)
        df = _extract_symbol_frame(raw_daily, sym)
        if df.empty or not {"Open", "High", "Low", "Close", "Volume"}.issubset(set(df.columns)):
            continue
        df = df[["Open", "High", "Low", "Close", "Volume"]].dropna().copy()
        if len(df) < max(70, lookback_days + hold_days + 5):
            continue
        df = df.tail(lookback_days + hold_days + 40).copy()
        df["vol_ma20"] = df["Volume"].rolling(20).mean()
        df["ret"] = df["Close"].pct_change()
        df["prev_close"] = df["Close"].shift(1)
        df["chg_pct"] = (df["Close"] / df["prev_close"] - 1.0) * 100.0
        df["range"] = (df["High"] - df["Low"]).clip(lower=1e-9)
        df["close_pos"] = (df["Close"] - df["Low"]) / df["range"]
        df["vol_ratio"] = df["Volume"] / df["vol_ma20"]

        board_list = [0] * len(df)
        vals = df.reset_index(drop=False)
        for i in range(1, len(vals)):
            streak = 0
            j = i
            while j >= 1:
                cp = safe_float(vals.loc[j, "Close"], 0.0)
                pp = safe_float(vals.loc[j - 1, "Close"], 0.0)
                if cp > 0 and pp > 0 and cp >= calc_limit_up(pp) - tw_tick(calc_limit_up(pp)):
                    streak += 1
                    j -= 1
                else:
                    break
            board_list[i] = streak
        df["board_streak"] = board_list

        signal = (
            (df["chg_pct"] >= 7.0)
            & (df["vol_ratio"] >= 1.8)
            & (df["close_pos"] >= 0.80)
        )
        if use_bloodline:
            signal &= df["board_streak"] >= min_board

        sig_idx = df.index[signal.fillna(False)].tolist()
        for idx in sig_idx:
            pos = df.index.get_loc(idx)
            if pos + 1 >= len(df):
                continue
            entry_idx = df.index[pos + 1]
            exit_pos = min(pos + hold_days, len(df) - 1)
            exit_idx = df.index[exit_pos]
            entry = safe_float(df.loc[entry_idx, "Open"], 0.0)
            exit_ = safe_float(df.loc[exit_idx, "Close"], 0.0)
            if entry <= 0 or exit_ <= 0:
                continue
            ret = (exit_ / entry - 1.0) * 100.0
            trades.append(
                {
                    "code": code,
                    "name": meta_dict[code]["name"],
                    "signal_date": str(pd.Timestamp(idx).date()),
                    "entry_date": str(pd.Timestamp(entry_idx).date()),
                    "exit_date": str(pd.Timestamp(exit_idx).date()),
                    "entry": round(entry, 2),
                    "exit": round(exit_),
                    "return_pct": round(ret, 2),
                    "board_streak": int(df.loc[idx, "board_streak"]),
                    "vol_ratio": round(safe_float(df.loc[idx, "vol_ratio"], 0.0), 2),
                }
            )

    bt = pd.DataFrame(trades)
    if bt.empty:
        return bt, {
            "signals": 0,
            "wins": 0,
            "win_rate": 0.0,
            "avg_return": 0.0,
            "median_return": 0.0,
            "best": 0.0,
            "worst": 0.0,
        }

    wins = int((bt["return_pct"] > 0).sum())
    stats = {
        "signals": int(len(bt)),
        "wins": wins,
        "win_rate": round(wins / len(bt) * 100.0, 1),
        "avg_return": round(float(bt["return_pct"].mean()), 2),
        "median_return": round(float(bt["return_pct"].median()), 2),
        "best": round(float(bt["return_pct"].max()), 2),
        "worst": round(float(bt["return_pct"].min()), 2),
    }
    bt = bt.sort_values(["signal_date", "return_pct"], ascending=[False, False]).reset_index(drop=True)
    return bt, stats


def make_backtest_display(bt_df: pd.DataFrame):
    if bt_df is None or bt_df.empty:
        return pd.DataFrame()

    display_df = bt_df.rename(
        columns={
            "code": "股票代號",
            "name": "股票名稱",
            "signal_date": "出現機會日",
            "entry_date": "進場日",
            "exit_date": "賣出日",
            "entry": "買進價格",
            "exit": "賣出價格",
            "return_pct": "獲利報酬%",
            "board_streak": "過去大漲次數",
            "vol_ratio": "交易熱度倍數",
        }
    ).copy()

    display_df = display_df[["股票代號", "股票名稱", "出現機會日", "進場日", "賣出日", "買進價格", "賣出價格", "獲利報酬%", "過去大漲次數", "交易熱度倍數"]]
    return display_df


def render_error_panel(errors):
    if not errors:
        return

    counts = {}
    order = []
    for msg in errors:
        if msg not in counts:
            counts[msg] = 0
            order.append(msg)
        counts[msg] += 1

    rows = []
    for msg in order:
        count = counts[msg]
        tag = "重複" if count > 1 else "單次"
        badge = f"<span class='log-count'>{count}x</span>" if count > 1 else ""
        rows.append(
            f"<div class='log-row'><span class='log-tag'>{tag}</span><span class='log-msg'>{html.escape(msg)}</span>{badge}</div>"
        )

    st.markdown("<div class='log-panel'>" + "".join(rows) + "</div>", unsafe_allow_html=True)


def render_backtest_table(display_df: pd.DataFrame):
    if display_df is None or display_df.empty:
        return

    headers = list(display_df.columns)
    header_html = "".join([f"<th>{html.escape(str(h))}</th>" for h in headers])

    body_rows = []
    for _, row in display_df.iterrows():
        ret = float(row["獲利報酬%"]) if pd.notna(row["獲利報酬%"]) else 0.0
        if ret >= 6:
            ret_class = "ret-strong"
        elif ret > 0:
            ret_class = "ret-pos"
        elif ret <= -6:
            ret_class = "ret-weak"
        elif ret < 0:
            ret_class = "ret-neg"
        else:
            ret_class = "ret-flat"

        cells = []
        for col in headers:
            val = row[col]
            cls = "num" if col in ["買進價格", "賣出價格", "過去大漲次數", "交易熱度倍數"] else ""
            if col == "獲利報酬%":
                val_html = f"<span class='ret-chip {ret_class}'>{ret:+.2f}%</span>"
                cls = "num"
            elif col in ["買進價格", "賣出價格"]:
                val_html = f"{float(val):.2f}"
            elif col == "交易熱度倍數":
                val_html = f"{float(val):.2f}x"
            else:
                val_html = html.escape(str(val))
            cells.append(f"<td class='{cls}'>{val_html}</td>")
        body_rows.append("<tr>" + "".join(cells) + "</tr>")

    table_html = f"""
    <div class='bt-wrap'>
      <div class='bt-table-scroll'>
        <table class='bt-table'>
          <thead><tr>{header_html}</tr></thead>
          <tbody>{''.join(body_rows)}</tbody>
        </table>
      </div>
    </div>
    """
    st.markdown(table_html, unsafe_allow_html=True)


def render_search_result_box(search_result):
    if not search_result:
        return
    if not search_result.get("ok"):
        kind = search_result.get("kind")
        if kind == "ambiguous":
            tags = ''.join([
                f"<span class='fail-tag'>{html.escape(m['code'])} {html.escape(m['name'])}｜{html.escape(m['market'])}</span>"
                for m in search_result.get("matches", [])
            ])
            st.markdown(
                f"<div class='search-panel'><div class='search-head'>獨立搜尋結果</div><div class='search-bad'>{html.escape(search_result.get('message', ''))}</div><div class='fail-bag'>{tags}</div></div>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f"<div class='search-panel'><div class='search-head'>獨立搜尋結果</div><div class='search-bad'>{html.escape(search_result.get('message', ''))}</div></div>",
                unsafe_allow_html=True,
            )
        return

    assess = search_result.get("assessment") or {}
    item = assess.get("item") or {}
    if not item:
        st.markdown(
            f"<div class='search-panel'><div class='search-head'>獨立搜尋結果</div><div class='search-bad'>{html.escape(assess.get('reason_text', '目前資料庫沒這支股票的完整數據。'))}</div></div>",
            unsafe_allow_html=True,
        )
        return

    passed = assess.get("passed", False)
    badge_cls = "search-good" if passed else "search-warn"
    badge_text = "順利通過當前條件" if passed else f"沒有通過｜{assess.get('reason_text', '表現未達標準')}"
    html_block = f"""
    <div class='search-panel'>
      <div class='search-head-row'>
        <div>
          <div class='search-head'>獨立搜尋與評分</div>
          <div class='search-source'>資料來源：{html.escape(search_result.get('source', ''))}</div>
        </div>
        <div class='{badge_cls}'>{html.escape(badge_text)}</div>
      </div>
      <div class='card search-card'>
        <div class='card-stage'>{html.escape(item.get('階段', ''))}</div>
        <div class='card-code'>{html.escape(str(item.get('代號', '')))}</div>
        <div class='card-name'>{html.escape(str(item.get('名稱', '')))} ｜ {html.escape(str(item.get('市場', '')))}</div>
        <div class='card-price'>{safe_float(item.get('現價', 0.0), 0.0):.2f}</div>
        <div class='card-status'>{html.escape(str(item.get('狀態', '')))}</div>
        <div class='card-stars-wrap'>
          <div class='card-stars'>{html.escape(str(item.get('推薦指數', '')))}</div>
          <div class='card-stars-badge'>推薦 {int(safe_int(item.get('推薦星等', 1), 1))}/5</div>
        </div>
        <div class='card-grid'>
          <div class='stat-pill'><div class='stat-k'>今日分數</div><div class='stat-v'>{safe_float(item.get('今日表現分數', 0.0), 0.0):.2f}</div></div>
          <div class='stat-pill'><div class='stat-k'>交易熱度</div><div class='stat-v'>{safe_float(item.get('交易熱度', 0.0), 0.0):.2f}x</div></div>
          <div class='stat-pill'><div class='stat-k'>距最高價</div><div class='stat-v'>{safe_float(item.get('距離最高價%', 0.0), 0.0):.2f}%</div></div>
          <div class='stat-pill'><div class='stat-k'>接近最高點</div><div class='stat-v'>{safe_float(item.get('接近一年最高價%', 0.0), 0.0):.1f}%</div></div>
          <div class='stat-pill'><div class='stat-k'>近 5 天</div><div class='stat-v'>{safe_float(item.get('近5天表現%', 0.0), 0.0):+.2f}%</div></div>
          <div class='stat-pill'><div class='stat-k'>近 20 天</div><div class='stat-v'>{safe_float(item.get('近20天表現%', 0.0), 0.0):+.2f}%</div></div>
        </div>
      </div>
    </div>
    """
    st.markdown(html_block, unsafe_allow_html=True)


# ============================================================
# UI 介面
# ============================================================
st.set_page_config(page_title=APP_TITLE, page_icon="⚡", layout="wide", initial_sidebar_state="collapsed")

st.markdown(
    """
<style>
/* CSS保持不變，維持你的版面設計 */
:root {
    --bg0: #040506;
    --bg1: #0a0d11;
    --bg2: #10141b;
    --line: rgba(255,255,255,0.06);
    --line2: rgba(255,255,255,0.10);
    --txt: #f8fafc;
    --muted: #94a3b8;
    --cyan: #38bdf8;
    --violet: #a78bfa;
    --green: #22c55e;
    --gold: #f59e0b;
    --rose: #fb7185;
}
[data-testid="stAppViewContainer"], .main {
    background:
        radial-gradient(circle at 10% 20%, rgba(56, 189, 248, 0.10), transparent 24%),
        radial-gradient(circle at 85% 18%, rgba(167, 139, 250, 0.11), transparent 20%),
        radial-gradient(circle at 40% 85%, rgba(34, 197, 94, 0.07), transparent 18%),
        linear-gradient(180deg, #040506 0%, #080a0d 35%, #0b0f14 100%) !important;
    color: var(--txt) !important;
}
.block-container {max-width: 1380px; padding-top: 1.6rem; padding-bottom: 3.0rem;}
[data-testid="stSidebar"] {display: none !important;}
.hero-wrap {
    padding: 20px 0 14px 0;
    margin-bottom: 8px;
}
.hero-title {
    font-size: 60px;
    line-height: 1.0;
    font-weight: 950;
    letter-spacing: -2.3px;
    background: linear-gradient(135deg, #ffffff 0%, #b8dbff 35%, #c4b5fd 72%, #ffffff 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}
.hero-sub {
    margin-top: 10px;
    color: #8ea2b8;
    font-size: 14px;
    letter-spacing: 1.1px;
}
.glass-row {
    background: linear-gradient(180deg, rgba(17, 24, 39, 0.55), rgba(10, 15, 23, 0.74));
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 22px;
    padding: 16px 18px;
    backdrop-filter: blur(18px);
    box-shadow: 0 18px 48px rgba(0,0,0,0.28);
    margin-bottom: 14px;
}
.search-panel {
    background: linear-gradient(180deg, rgba(12,17,25,0.84), rgba(8,12,18,0.95));
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 22px;
    padding: 18px;
    box-shadow: 0 18px 48px rgba(0,0,0,0.24);
    margin-bottom: 14px;
}
.search-head-row {display:flex; align-items:flex-start; justify-content:space-between; gap:12px; margin-bottom:14px;}
.search-head {font-size:18px; font-weight:900; color:#f8fafc; letter-spacing:.3px;}
.search-source {font-size:12px; color:#8ea5bb; margin-top:6px;}
.search-good, .search-warn, .search-bad {
    border-radius: 999px; padding: 7px 12px; font-size: 12px; font-weight: 900; display:inline-flex; align-items:center;
}
.search-good {background: rgba(34,197,94,0.14); color:#bbf7d0; border:1px solid rgba(34,197,94,0.22);}
.search-warn {background: rgba(245,158,11,0.14); color:#fde68a; border:1px solid rgba(245,158,11,0.22);}
.search-bad {background: rgba(251,113,133,0.12); color:#fecdd3; border:1px solid rgba(251,113,133,0.18); display:inline-flex; margin-top:8px;}
.search-card {min-height: unset;}
.mini-kicker {
    display:inline-block;
    padding: 6px 12px;
    border-radius: 999px;
    border: 1px solid rgba(56, 189, 248, 0.20);
    color: #8bd6ff;
    background: rgba(56, 189, 248, 0.08);
    font-size: 12px;
    font-weight: 800;
    letter-spacing: 1px;
}
.card {
    background: linear-gradient(160deg, rgba(17, 20, 26, 0.96), rgba(10, 12, 18, 0.94));
    border: 1px solid rgba(255,255,255,0.06);
    border-top: 1px solid rgba(255,255,255,0.12);
    border-radius: 24px;
    padding: 18px 18px 16px 18px;
    min-height: 218px;
    box-shadow: 0 18px 50px rgba(0,0,0,0.30);
    transition: all .22s ease;
}
.card:hover {
    transform: translateY(-4px);
    border-color: rgba(56, 189, 248, 0.24);
    box-shadow: 0 22px 56px rgba(56, 189, 248, 0.10);
}
.card-stage {
    display:inline-block;
    padding: 5px 12px;
    border-radius: 999px;
    color: #c4b5fd;
    background: rgba(167, 139, 250, 0.10);
    border: 1px solid rgba(167, 139, 250, 0.18);
    font-size: 11px;
    font-weight: 800;
    letter-spacing: 1px;
}
.card-code {font-size: 22px; font-weight: 900; color: #ffffff; margin-top: 14px; letter-spacing: .5px;}
.card-name {font-size: 14px; color: #9fb0c5; font-weight: 700; margin-top: 2px;}
.card-price {font-size: 38px; font-weight: 950; color: #ffffff; margin-top: 14px; letter-spacing: -1px;}
.card-status {font-size: 13px; color: #d8e3ef; font-weight: 700; margin-top: 10px;}
.card-stars-wrap {display:flex; align-items:center; justify-content:space-between; gap:10px; margin-top: 14px;}
.card-stars {font-size: 18px; letter-spacing: 1px; font-weight: 900; color: #ffd76a;}
.card-stars-badge {font-size: 12px; color: #09111b; background: linear-gradient(135deg, #ffe082 0%, #f6c453 100%); border-radius: 999px; padding: 5px 10px; font-weight: 900;}
.card-grid {display:grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; margin-top: 14px;}
.stat-pill {
    border-radius: 14px;
    border: 1px solid rgba(255,255,255,0.06);
    padding: 10px 12px;
    background: rgba(255,255,255,0.03);
}
.stat-k {font-size: 11px; color: #8ba2b8; font-weight: 700; letter-spacing: .8px;}
.stat-v {font-size: 15px; color: #f8fafc; font-weight: 900; margin-top: 2px;}
.fail-bag {margin: 6px 0 4px 0;}
.fail-tag {
    display: inline-block;
    padding: 6px 10px;
    margin: 4px 6px 0 0;
    border-radius: 999px;
    font-size: 12px;
    font-weight: 700;
    color: #fecdd3;
    background: rgba(251, 113, 133, 0.08);
    border: 1px solid rgba(251, 113, 133, 0.16);
}
.soft-note {
    color: #8da3ba;
    font-size: 12px;
    line-height: 1.6;
}
[data-testid="stMetric"] {
    background: linear-gradient(180deg, rgba(13,18,24,0.72), rgba(10,12,18,0.90));
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 18px;
    padding: 16px;
}
[data-testid="stMetricLabel"] {color: #8ea5bb !important; font-size: 13px !important; font-weight: 700 !important; letter-spacing: .6px;}
[data-testid="stMetricValue"] {color: #f8fafc !important; font-size: 32px !important; font-weight: 950 !important;}
.stButton>button {
    width: 100% !important;
    border: none !important;
    border-radius: 18px !important;
    min-height: 58px !important;
    font-size: 18px !important;
    font-weight: 950 !important;
    letter-spacing: 1.2px !important;
    color: #09111a !important;
    background: linear-gradient(135deg, #ffffff 0%, #b8dbff 48%, #c4b5fd 100%) !important;
    box-shadow: 0 16px 44px rgba(56, 189, 248, 0.16) !important;
}
.stButton>button:hover {
    transform: translateY(-2px);
    box-shadow: 0 22px 50px rgba(56, 189, 248, 0.22) !important;
}
[data-testid="stExpander"] {
    border: 1px solid rgba(255,255,255,0.06) !important;
    border-radius: 18px !important;
    background: rgba(10, 14, 20, 0.46) !important;
}
[data-testid="stExpander"] summary {
    border-radius: 18px !important;
    background: rgba(255,255,255,0.02) !important;
}
[data-testid="stDataFrame"] {
    border: 1px solid rgba(255,255,255,0.08) !important;
    border-radius: 18px !important;
    overflow: hidden !important;
    background: linear-gradient(180deg, rgba(10,14,20,0.78), rgba(8,11,16,0.95)) !important;
    box-shadow: inset 0 1px 0 rgba(255,255,255,0.03), 0 14px 40px rgba(0,0,0,0.22) !important;
}
[data-testid="stDataFrame"] [role="grid"] {
    background: transparent !important;
}

.log-panel {
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 18px;
    background: linear-gradient(180deg, rgba(10,14,20,0.92), rgba(7,10,14,0.98));
    padding: 10px 12px;
    box-shadow: inset 0 1px 0 rgba(255,255,255,0.03);
}
.log-row {
    display:flex; align-items:flex-start; gap:10px;
    padding: 10px 8px; border-bottom: 1px solid rgba(255,255,255,0.05);
}
.log-row:last-child {border-bottom:none;}
.log-tag {
    min-width: 42px; text-align:center;
    border-radius: 999px; padding: 3px 8px;
    background: rgba(251,113,133,0.10); color:#fecdd3;
    border:1px solid rgba(251,113,133,0.16); font-size:11px; font-weight:800;
}
.log-msg {
    flex:1; color:#d7e5f4; font-size:13px; line-height:1.55;
    font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    word-break: break-word;
}
.log-count {
    color:#93c5fd; background:rgba(59,130,246,0.10); border:1px solid rgba(59,130,246,0.18);
    border-radius:999px; padding:3px 8px; font-size:11px; font-weight:900;
}
.bt-wrap {
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 18px;
    background: linear-gradient(180deg, rgba(9,13,19,0.96), rgba(6,9,13,0.99));
    box-shadow: inset 0 1px 0 rgba(255,255,255,0.03), 0 18px 44px rgba(0,0,0,0.26);
    overflow: hidden;
}
.bt-table-scroll {
    overflow-x: auto;
}
.bt-table {
    width: 100%;
    min-width: 1120px;
    border-collapse: separate;
    border-spacing: 0;
    table-layout: fixed;
}
.bt-table thead th {
    position: sticky; top: 0; z-index: 2;
    text-align: left;
    padding: 13px 12px;
    background: linear-gradient(180deg, rgba(18,24,34,0.98), rgba(13,18,26,0.98));
    color: #9fb7cf;
    font-size: 13px;
    font-weight: 800;
    letter-spacing: .5px;
    border-bottom: 1px solid rgba(255,255,255,0.08);
}
.bt-table tbody td {
    padding: 12px;
    color: #edf4fb;
    font-size: 14px;
    border-bottom: 1px solid rgba(255,255,255,0.05);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}
.bt-table tbody tr:nth-child(odd) td {background: rgba(255,255,255,0.015);}
.bt-table tbody tr:nth-child(even) td {background: rgba(255,255,255,0.028);}
.bt-table tbody tr:hover td {background: rgba(56,189,248,0.08);}
.bt-table td.num {text-align: right; font-variant-numeric: tabular-nums;}
.ret-chip {
    display:inline-flex; align-items:center; justify-content:center; min-width:82px;
    border-radius:999px; padding:5px 10px; font-weight:900; letter-spacing:.2px;
}
.ret-strong {background: rgba(34,197,94,0.18); color:#bbf7d0; border:1px solid rgba(34,197,94,0.22);}
.ret-pos {background: rgba(74,222,128,0.12); color:#dcfce7; border:1px solid rgba(74,222,128,0.18);}
.ret-flat {background: rgba(148,163,184,0.10); color:#e2e8f0; border:1px solid rgba(148,163,184,0.14);}
.ret-neg {background: rgba(251,113,133,0.12); color:#ffe4e6; border:1px solid rgba(251,113,133,0.18);}
.ret-weak {background: rgba(244,63,94,0.20); color:#fff1f2; border:1px solid rgba(244,63,94,0.24);}

hr {
    border: none;
    border-top: 1px solid rgba(255,255,255,0.08);
    margin: 20px 0;
}
</style>
""",
    unsafe_allow_html=True,
)

st.markdown(
    f"""
<div class="hero-wrap">
  <div class="mini-kicker">MOMENTUM WAR ROOM</div>
  <div class="hero-title">{APP_TITLE}</div>
  <div class="hero-sub">{APP_SUBTITLE}</div>
</div>
""",
    unsafe_allow_html=True,
)

with st.container():
    st.markdown('<div class="glass-row">', unsafe_allow_html=True)
    cfg1, cfg2, cfg3 = st.columns([1.2, 1.2, 1.0])
    with cfg1:
        is_test = st.toggle("🔥 放寬標準模式", value=False, help="降低篩選標準，方便看到更多可能的機會。")
    with cfg2:
        use_bloodline = st.toggle("🛡️ 連續大漲篩選", value=True, help="只挑選過去曾經連續大漲的股票，避開表現平庸的。")
    with cfg3:
        only_tse = False
    min_board = DEFAULT_MIN_BOARD
    hold_days = DEFAULT_HOLD_DAYS
    st.markdown('</div>', unsafe_allow_html=True)

st.markdown('<div class="glass-row">', unsafe_allow_html=True)
launch_col, api_col = st.columns([1.5, 1.2])
with launch_col:
    launch = st.button("🚀 取得最新市場資料 / 建立快速資料庫")
with api_col:
    api_key = get_api_key()
    if api_key:
        st.success("✅ 已偵測到 Fugle API Key")
    else:
        st.warning("⚠️ 尚未偵測到 Fugle API Key，將無法抓取最新官方資料。")
st.markdown('</div>', unsafe_allow_html=True)

st.markdown('<div class="glass-row">', unsafe_allow_html=True)
search_col, search_btn_col = st.columns([3.6, 1.2])
with search_col:
    search_query = st.text_input(
        "獨立搜尋",
        value=st.session_state.get("independent_search_query", ""),
        placeholder="輸入股票代號或名稱，例如 8299、群聯、華邦電",
        help="不受清單限制，直接指定一支股票來算算看它的分數。",
        label_visibility="collapsed",
    )
with search_btn_col:
    search_launch = st.button("🔎 搜尋個股評分", use_container_width=True)
st.markdown('</div>', unsafe_allow_html=True)

if search_launch:
    st.session_state["independent_search_query"] = search_query
    api_key_search = get_api_key()
    meta_search, meta_errors = get_stock_list()
    if not api_key_search:
        st.session_state["independent_search_result"] = {
            "ok": False,
            "kind": "not_found",
            "message": "找不到 Fugle API Key，無法執行獨立搜尋評分。",
            "matches": [],
        }
    elif not meta_search:
        st.session_state["independent_search_result"] = {
            "ok": False,
            "kind": "not_found",
            "message": "股票清單讀取失敗，請稍後再試。",
            "matches": [],
        }
    else:
        try:
            with st.status("🔎 搜尋指定股票並套用同一套評分模型...", expanded=False):
                st.session_state["independent_search_result"] = evaluate_single_search(
                    query=search_query,
                    meta_dict=meta_search,
                    api_key=api_key_search,
                    now_ts=now_taipei(),
                    is_test=is_test,
                    use_bloodline=use_bloodline,
                    min_board=min_board,
                    vault=st.session_state.get("raw_data_vault_v12"),
                )
        except Exception as e:
            st.session_state["independent_search_result"] = {
                "ok": False,
                "kind": "not_found",
                "message": f"搜尋評分失敗：{e}",
                "matches": [],
            }

search_result = st.session_state.get("independent_search_result")
if search_result:
    render_search_result_box(search_result)

now_epoch = time.time()
last_run = st.session_state.get("last_run_ts", 0)

if launch:
    if not api_key:
        st.error("🚨 找不到 Fugle API Key，請先設定後再啟動。")
    elif now_epoch - last_run < DEFAULT_COOLDOWN_SECONDS:
        remain = int(DEFAULT_COOLDOWN_SECONDS - (now_epoch - last_run))
        st.warning(f"⏳ 保護機制啟動中，請約 {remain} 秒後再重新抓取資料。")
    else:
        st.session_state["last_run_ts"] = now_epoch
        base_diag = diag_init()
        t_all = time.perf_counter()

        with st.status("⚡ 準備整理最新市場資訊...", expanded=True) as status:
            t0 = time.perf_counter()
            meta, meta_errors = get_stock_list()
            base_diag["t_meta"] = time.perf_counter() - t0
            base_diag["meta_count"] = len(meta)
            for e in meta_errors:
                diag_err(base_diag, Exception(e), "META")

            candidate_df = pd.DataFrame()
            ranked_codes = []

            try:
                status.update(label="🌐 優先嘗試抓取官方全市場資料...", state="running")
                candidate_df, ranked_codes = fetch_market_snapshot_and_rank(meta, api_key, base_diag, status)
            except Exception as e:
                diag_err(base_diag, e, "SNAPSHOT_PRIMARY")
                status.update(label="🟡 官方快照無法使用，切換到網路排行榜並一檔一檔抓資料...", state="running")
                candidate_df, ranked_codes = fetch_candidate_rows_by_public_rank(meta, api_key, base_diag, status)

            if candidate_df.empty:
                status.update(label="❌ 無法取得股票資料，請檢查網路連線或 API 設定。", state="error")
                st.stop()

            feature_cache, raw_daily = compute_feature_cache(candidate_df, meta, base_diag, status, period=f"{RAW_HISTORY_DAYS}d")

            now_ts = now_taipei()
            pre_res, _, pre_diag = apply_dynamic_filters(
                raw_df=candidate_df,
                feature_cache=feature_cache,
                now_ts=now_ts,
                is_test=is_test,
                use_bloodline=use_bloodline,
                only_tse=only_tse,
                min_board=min_board,
                base_diag=base_diag,
            )

            enrich_codes = stable_unique(
                (pre_res["代號"].head(FINAL_ENRICH_LIMIT).tolist() if not pre_res.empty else [])
                + candidate_df.sort_values(["dist", "vol_sh"], ascending=[True, False])["code"].head(FINAL_ENRICH_LIMIT).tolist()
            )[:FINAL_ENRICH_LIMIT]
            if enrich_codes:
                status.update(label="🧠 補強重點候選名單的買賣排隊狀況...", state="running")
                t_enrich = time.perf_counter()
                session = make_retry_session()
                enrich_map = enrich_quotes_for_codes(session, api_key, enrich_codes, base_diag)
                base_diag["t_enrich"] = time.perf_counter() - t_enrich
                if enrich_map:
                    for k, v in enrich_map.items():
                        for field, value in v.items():
                            candidate_df.loc[candidate_df["code"] == k, field] = value
            else:
                base_diag["t_enrich"] = 0.0

            base_diag["total"] = time.perf_counter() - t_all
            status.update(label="✅ 資料庫已建立完成。之後切換開關不需要重抓，會直接用現有資料運算。", state="complete")

        st.session_state["raw_data_vault_v12"] = {
            "meta": meta,
            "candidate_df": candidate_df,
            "feature_cache": feature_cache,
            "raw_daily": raw_daily,
            "ranked_codes": ranked_codes,
            "base_diag": base_diag,
            "ts": now_taipei(),
        }

if "raw_data_vault_v12" in st.session_state:
    vault = st.session_state["raw_data_vault_v12"]
    t_filter = time.perf_counter()
    res, stats, final_diag = apply_dynamic_filters(
        raw_df=vault["candidate_df"],
        feature_cache=vault["feature_cache"],
        now_ts=vault["ts"],
        is_test=is_test,
        use_bloodline=use_bloodline,
        only_tse=only_tse,
        min_board=min_board,
        base_diag=vault["base_diag"],
    )
    final_diag["t_filter"] = time.perf_counter() - t_filter

    bt_t0 = time.perf_counter()
    bt_universe = pick_backtest_universe(vault["candidate_df"], top_n=16)
    bt_df, bt_stats = run_surrogate_backtest(
        raw_daily=vault["raw_daily"],
        universe_codes=bt_universe,
        meta_dict=vault["meta"],
        lookback_days=126,
        hold_days=hold_days,
        use_bloodline=use_bloodline,
        min_board=min_board,
    )
    final_diag["t_backtest"] = time.perf_counter() - bt_t0

    ts = vault["ts"]
    state_str = f"放寬模式 {'開啟' if is_test else '關閉'} ｜ 連續大漲篩選 {'開啟' if use_bloodline else '關閉'}"
    st.markdown(
        f"<div class='soft-note'>資料時間：{ts.strftime('%Y-%m-%d %H:%M:%S')}（台灣時間）｜{state_str}｜重新篩選只花：{final_diag['t_filter']:.3f}秒</div>",
        unsafe_allow_html=True,
    )

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("初始清單數量", f"{final_diag.get('candidate_count', 0)} 檔", f"資料來源：{final_diag.get('rank_src', '未知')}")
    m2.metric("通過嚴格標準", f"{len(res)} 檔", f"成功取得即時資料數：{final_diag.get('snapshot_ok', 0)}")
    coverage = f"{final_diag.get('feature_ok', 0)} / {final_diag.get('candidate_count', 0)}"
    m3.metric("歷史資料庫完整度", coverage, f"成功下載過去資料數：{final_diag.get('yf_returned', 0)}")
    m4.metric("歷史模擬勝率", f"{bt_stats['win_rate']}%", f"過去出現過的機會：{bt_stats['signals']} 次")

    with st.expander("⚙️ 系統檢查與除錯面板", expanded=False):
        d1, d2, d3, d4 = st.columns(4)
        d1.metric("讀取總股票數", final_diag.get("meta_count", 0))
        d2.metric("最新資料庫連線成功", final_diag.get("snapshot_market_ok", 0))
        d3.metric("排隊買賣資訊", f"{final_diag.get('quote_enrich_ok', 0)} / {final_diag.get('quote_enrich_ok', 0) + final_diag.get('quote_enrich_fail', 0)}")
        d4.metric("運算耗時", f"{final_diag['t_filter']:.3f}秒 / {final_diag['t_backtest']:.2f}秒")
        st.caption(
            f"耗時分布：股票清單 {final_diag['t_meta']:.2f}秒 ｜ 最新資料 {final_diag['t_rank']:.2f}秒 ｜ 過去表現 {final_diag['t_features']:.2f}秒 ｜ 排隊資訊 {final_diag['t_enrich']:.2f}秒 ｜ 總共 {final_diag['total']:.2f}秒"
        )
        st.caption(
            f"下載過去資料分段成功 {final_diag.get('yf_parts_ok', 0)} ｜ 失敗 {final_diag.get('yf_parts_fail', 0)} ｜ 處理失敗 {final_diag.get('feature_fail', 0)} ｜ 其他錯誤 {final_diag.get('other_err', 0)}"
        )
        if final_diag.get("last_errors"):
            render_error_panel(list(final_diag["last_errors"]))

    with st.expander("🎯 未符合條件的股票名單", expanded=True):
        for reason, items in stats.items():
            if isinstance(items, list) and items:
                st.markdown(f"**{reason}**")
                st.markdown('<div class="fail-bag">' + ''.join([f'<span class="fail-tag">{x}</span>' for x in items]) + '</div>', unsafe_allow_html=True)

    st.markdown("<hr>", unsafe_allow_html=True)
    st.subheader("強勢焦點股")
    if not res.empty:
        cols = st.columns(4)
        for i, row in res.iterrows():
            with cols[i % 4]:
                st.markdown(
                    f"""
<div class="card">
  <div class="card-stage">{row['階段']}</div>
  <div class="card-code">{row['代號']}</div>
  <div class="card-name">{row['名稱']} ｜ {row['市場']}</div>
  <div class="card-price">{row['現價']:.2f}</div>
  <div class="card-status">{row['狀態']}</div>
  <div class="card-stars-wrap">
    <div class="card-stars">{row['推薦指數']}</div>
    <div class="card-stars-badge">推薦 {int(row['推薦星等'])}/5</div>
  </div>
  <div class="card-grid">
    <div class="stat-pill"><div class="stat-k">今日分數</div><div class="stat-v">{row['今日表現分數']:.2f}</div></div>
    <div class="stat-pill"><div class="stat-k">交易熱度</div><div class="stat-v">{row['交易熱度']:.2f}x</div></div>
    <div class="stat-pill"><div class="stat-k">距最高價</div><div class="stat-v">{row['距離最高價%']:.2f}%</div></div>
    <div class="stat-pill"><div class="stat-k">接近最高點</div><div class="stat-v">{row['接近一年最高價%']:.1f}%</div></div>
  </div>
</div>
""",
                    unsafe_allow_html=True,
                )
    else:
        st.warning("⚠️ 目前條件設定比較嚴格，沒有股票入選。你可以先打開上方的『放寬標準模式』，看看原本可能符合的機會。")

    with st.expander("🧪 歷史模擬測試 (過去126天)", expanded=False):
        st.caption("這個功能是拿過去 126 天的資料來算算看，如果照這套嚴格標準來找股票勝率如何。這只是模擬，不保證未來一定賺錢喔。")
        b1, b2, b3, b4, b5 = st.columns(5)
        b1.metric("出現機會數", bt_stats["signals"])
        b2.metric("模擬勝率", f"{bt_stats['win_rate']}%")
        b3.metric("平均獲利", f"{bt_stats['avg_return']}%")
        b4.metric("中位數獲利", f"{bt_stats['median_return']}%")
        b5.metric("最佳 / 最差表現", f"{bt_stats['best']}% / {bt_stats['worst']}%")
        if not bt_df.empty:
            bt_show = make_backtest_display(bt_df)
            render_backtest_table(bt_show)
            st.caption("表格調整為方便閱讀的深色模式，數字靠右對齊、漲跌用顏色區分，看久了眼睛比較不會累。")
        else:
            st.info("過去 126 天內，這些股票沒有發生符合你所設定條件的情況。可能你的條件訂得太嚴格了，或是選到的清單剛好近期表現平淡。")

else:
    st.info("請先點擊上方按鈕建立最新的資料庫！之後如果想要調整條件，只要切換上方的開關，系統就會用原有的資料瞬間重新幫你計算。")
