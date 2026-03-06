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
APP_SUBTITLE = "v12.0 真瞬切版｜官方快照優先｜雙榜狙擊｜內建驗證"
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
                        "market": "TSE" if ex == "tse" else "OTC",
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
    """將各種時間索引安全轉成台北日期物件。"""
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
    return meta_dict.get(code, {}).get("market", "TSE")


def market_label(m):
    return "上市" if m == "TSE" else "上櫃" if m == "OTC" else m


def copy_diag(diag):
    d = deepcopy(diag)
    if not isinstance(d.get("last_errors"), deque):
        d["last_errors"] = deque(d.get("last_errors", []), maxlen=12)
    return d


# ============================================================
# Fugle API
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

    diag["rank_src"] = "Yahoo / WantGoo 備援榜單"
    return ordered[:MAX_CANDIDATES]


# ============================================================
# 官方快照優先：直接抓全市場快照後本地雙榜聯集
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
            # change/close異常時，先略過，避免漲停價錯算
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
    for market in ("TSE", "OTC"):
        status_placeholder.update(label=f"⚡ 讀取 {market_label(market)} 官方快照中...", state="running")
        try:
            snap = snapshot_quotes_market(session, api_key, market, diag)
            quotes_frames.append(build_quotes_from_snapshot(snap, market, meta_dict))
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

    diag["rank_src"] = "Fugle Snapshot 全市場快照（官方）"
    diag["rank_count"] = len(ranked_codes)
    diag["candidate_count"] = len(candidate_df)
    diag["t_rank"] = max(diag.get("t_rank", 0.0), time.perf_counter() - t0)
    return candidate_df, ranked_codes


# ============================================================
# 備援：公開榜單 + 逐檔 quote
# ============================================================
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
                stage = "⚡ 快掃主力熱區"
            elif idx <= 80:
                sleep_sec = 0.18
                stage = "🛰️ 穩定擴掃中段候選"
            else:
                sleep_sec = 0.30
                stage = "🛡️ 節流保護掃描尾段"
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
# 歷史特徵預先計算（真正瞬切關鍵）
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
        status_placeholder.update(label=f"📚 預載歷史特徵中... ({min(i + len(part), len(syms))}/{len(syms)})", state="running")
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
# 濾網（完全吃 vault，不重抓）
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
    """把通過濾網的候選股轉成 1~5 顆星推薦指數。"""
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

    if "鎖板" in str(status_text):
        bonus += 0.5

    stars = int(round(stars + bonus * 0.5))
    return max(1, min(5, stars))


def render_star_bar(stars):
    stars = max(1, min(5, int(stars)))
    return "★" * stars + "☆" * (5 - stars)


def apply_dynamic_filters(raw_df, feature_cache, now_ts, is_test, use_bloodline, only_tse, min_board, base_diag):
    diag = copy_diag(base_diag)
    stats = {"候選總數": 0, "爆量不足": [], "回落過大": [], "收盤太弱": [], "血統不足": [], "資訊不足": []}
    if raw_df is None or raw_df.empty:
        return pd.DataFrame(), stats, diag

    th = get_thresholds(now_ts, is_test=is_test)
    work = raw_df.copy()
    if only_tse:
        work = work[work["market"] == "TSE"].copy()

    work = work[(work["dist"] <= th["dist_limit"]) & (work["vol_sh"] >= th["vol_limit"])].copy()
    stats["候選總數"] = len(work)
    if work.empty:
        diag["final_count"] = 0
        return pd.DataFrame(), stats, diag

    frac = intraday_progress_fraction(now_ts)
    out = []

    for _, r in work.iterrows():
        code = r["code"]
        feat = feature_cache.get(code)
        if not feat:
            stats["資訊不足"].append(f"{code} {r['name']}")
            diag["yf_fail"] += 1
            continue

        vol_ma20 = safe_float(feat.get("vol_ma20"), 0.0)
        board_streak = safe_int(feat.get("board_streak"), 0)
        high_52w = safe_float(feat.get("high_52w"), 0.0)
        ret5 = safe_float(feat.get("ret5"), 0.0)
        ret20 = safe_float(feat.get("ret20"), 0.0)

        if vol_ma20 <= 0:
            stats["資訊不足"].append(f"{code} {r['name']}")
            diag["yf_fail"] += 1
            continue

        vol_ratio_live = r["vol_sh"] / max(vol_ma20 * (1.0 if is_test else frac), 1e-9)
        if vol_ratio_live < th["vol_ratio_min"]:
            stats["爆量不足"].append(f"{code} {r['name']}")
            continue

        rng = max(r["high"] - r["low"], 0.0)
        pullback = (r["high"] - r["last"]) / max(r["high"], 1e-9)
        close_pos = 1.0 if rng < 1e-9 else (r["last"] - r["low"]) / max(rng, 1e-9)

        if pullback > th["pullback_lim"]:
            stats["回落過大"].append(f"{code} {r['name']}")
            continue
        if close_pos < th["close_pos_min"] and rng > max(0.1, r["last"] * 0.002):
            stats["收盤太弱"].append(f"{code} {r['name']}")
            continue
        if use_bloodline and not is_test and board_streak < min_board:
            stats["血統不足"].append(f"{code} {r['name']}")
            continue

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

        status = "🔒 鎖板排隊" if hard_locked else "🟣 板上臨界" if near_limit else "⚡ 強攻發動"
        out.append(
            {
                "代號": code,
                "名稱": r["name"],
                "市場": market_label(r["market"]),
                "現價": r["last"],
                "距漲停%": r["dist"],
                "爆量": vol_ratio_live,
                "日內強度": signal_score,
                "推薦星等": score_to_star_count(
                    signal_score=signal_score,
                    dist_pct=r["dist"],
                    vol_ratio=vol_ratio_live,
                    board_streak=board_streak,
                    close_pos=close_pos,
                    proximity_52w=proximity_52w,
                    status_text=status,
                ),
                "推薦指數": render_star_bar(score_to_star_count(
                    signal_score=signal_score,
                    dist_pct=r["dist"],
                    vol_ratio=vol_ratio_live,
                    board_streak=board_streak,
                    close_pos=close_pos,
                    proximity_52w=proximity_52w,
                    status_text=status,
                )),
                "狀態": status,
                "階段": f"歷史連板 {board_streak} 天",
                "board_val": board_streak,
                "close_pos": close_pos,
                "pullback": pullback,
                "52w接近%": proximity_52w,
                "近5日%": ret5,
                "近20日%": ret20,
                "best_bid": bid_price,
                "best_bid_size": bid_size,
            }
        )

    res = pd.DataFrame(out)
    if not res.empty:
        res = res.sort_values(["推薦星等", "日內強度", "board_val", "爆量", "距漲停%"], ascending=[False, False, False, False, True]).reset_index(drop=True)
    diag["final_count"] = len(res)
    return res, stats, diag


# ============================================================
# 日線替身驗證（誠實標示：非盤中逐秒真回測）
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

        # 替身條件：接近你盤中戰情室的精神，但只用日線可取得欄位
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
                    "exit": round(exit_, 2),
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
            "code": "代號",
            "name": "名稱",
            "signal_date": "訊號日",
            "entry_date": "進場日",
            "exit_date": "出場日",
            "entry": "進場價",
            "exit": "出場價",
            "return_pct": "報酬率%",
            "board_streak": "連板血統",
            "vol_ratio": "爆量倍率",
        }
    ).copy()

    display_df = display_df[["代號", "名稱", "訊號日", "進場日", "出場日", "進場價", "出場價", "報酬率%", "連板血統", "爆量倍率"]]
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
        ret = float(row["報酬率%"]) if pd.notna(row["報酬率%"]) else 0.0
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
            cls = "num" if col in ["進場價", "出場價", "連板血統", "爆量倍率"] else ""
            if col == "報酬率%":
                val_html = f"<span class='ret-chip {ret_class}'>{ret:+.2f}%</span>"
                cls = "num"
            elif col in ["進場價", "出場價"]:
                val_html = f"{float(val):.2f}"
            elif col == "爆量倍率":
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


# ============================================================
# UI
# ============================================================
st.set_page_config(page_title=APP_TITLE, page_icon="⚡", layout="wide", initial_sidebar_state="collapsed")

st.markdown(
    """
<style>
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
        is_test = st.toggle("🔥 寬鬆測試模式", value=False, help="關閉多數硬門檻，方便觀察候選池。")
    with cfg2:
        use_bloodline = st.toggle("🛡️ 連板血統濾網", value=True, help="要求歷史連板血統，減少雜訊股。")
    with cfg3:
        only_tse = False
    min_board = DEFAULT_MIN_BOARD
    hold_days = DEFAULT_HOLD_DAYS
    st.markdown('</div>', unsafe_allow_html=True)

st.markdown('<div class="glass-row">', unsafe_allow_html=True)
launch_col, api_col = st.columns([1.5, 1.2])
with launch_col:
    launch = st.button("🚀 啟動官方快照狙擊 / 重建資料金庫")
with api_col:
    api_key = get_api_key()
    if api_key:
        st.success("✅ 已偵測到 Fugle API Key")
    else:
        st.warning("⚠️ 尚未偵測到 Fugle API Key，將無法使用官方快照。")
st.markdown('</div>', unsafe_allow_html=True)

now_epoch = time.time()
last_run = st.session_state.get("last_run_ts", 0)

if launch:
    if not api_key:
        st.error("🚨 找不到 Fugle API Key，請先設定後再啟動。")
    elif now_epoch - last_run < DEFAULT_COOLDOWN_SECONDS:
        remain = int(DEFAULT_COOLDOWN_SECONDS - (now_epoch - last_run))
        st.warning(f"⏳ 冷卻保護中，請約 {remain} 秒後再重建資料金庫。")
    else:
        st.session_state["last_run_ts"] = now_epoch
        base_diag = diag_init()
        t_all = time.perf_counter()

        with st.status("⚡ 準備系統與市場結構資料...", expanded=True) as status:
            t0 = time.perf_counter()
            meta, meta_errors = get_stock_list()
            base_diag["t_meta"] = time.perf_counter() - t0
            base_diag["meta_count"] = len(meta)
            for e in meta_errors:
                diag_err(base_diag, Exception(e), "META")

            candidate_df = pd.DataFrame()
            ranked_codes = []

            try:
                status.update(label="🌐 優先嘗試 Fugle 官方全市場快照...", state="running")
                candidate_df, ranked_codes = fetch_market_snapshot_and_rank(meta, api_key, base_diag, status)
            except Exception as e:
                diag_err(base_diag, e, "SNAPSHOT_PRIMARY")
                status.update(label="🟡 官方快照不可用，切換到公開榜單 + 逐檔 quote 備援...", state="running")
                candidate_df, ranked_codes = fetch_candidate_rows_by_public_rank(meta, api_key, base_diag, status)

            if candidate_df.empty:
                status.update(label="❌ 無法取得候選資料，請檢查 API 權限或網路狀態。", state="error")
                st.stop()

            feature_cache, raw_daily = compute_feature_cache(candidate_df, meta, base_diag, status, period=f"{RAW_HISTORY_DAYS}d")

            # 先用當前開關跑一次，挑 finalists 再補 quote 五檔資訊
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
                status.update(label="🧠 補強 finalists 五檔委買委賣資訊...", state="running")
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
            status.update(label="✅ 資料金庫建立完成。往後切換開關只重跑本地濾網，不重抓外部資料。", state="complete")

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
    state_str = f"測試 {'ON' if is_test else 'OFF'} ｜ 血統 {'ON' if use_bloodline else 'OFF'}"
    st.markdown(
        f"<div class='soft-note'>資料時間：{ts.strftime('%Y-%m-%d %H:%M:%S')}（Asia/Taipei）｜{state_str}｜濾網瞬切：{final_diag['t_filter']:.3f}s</div>",
        unsafe_allow_html=True,
    )

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("候選池", f"{final_diag.get('candidate_count', 0)} 檔", f"來源：{final_diag.get('rank_src', '未知')}")
    m2.metric("最終入選", f"{len(res)} 檔", f"快照成功：{final_diag.get('snapshot_ok', 0)}")
    coverage = f"{final_diag.get('feature_ok', 0)} / {final_diag.get('candidate_count', 0)}"
    m3.metric("歷史特徵覆蓋", coverage, f"YF 回傳：{final_diag.get('yf_returned', 0)}")
    m4.metric("替身驗證勝率", f"{bt_stats['win_rate']}%", f"訊號：{bt_stats['signals']}")

    with st.expander("⚙️ 系統診斷 / 白盒監控", expanded=False):
        d1, d2, d3, d4 = st.columns(4)
        d1.metric("Meta 檔數", final_diag.get("meta_count", 0))
        d2.metric("快照市場成功", final_diag.get("snapshot_market_ok", 0))
        d3.metric("五檔補強", f"{final_diag.get('quote_enrich_ok', 0)} / {final_diag.get('quote_enrich_ok', 0) + final_diag.get('quote_enrich_fail', 0)}")
        d4.metric("濾網 / 驗證耗時", f"{final_diag['t_filter']:.3f}s / {final_diag['t_backtest']:.2f}s")
        st.caption(
            f"耗時分布：Meta {final_diag['t_meta']:.2f}s ｜ Snapshot+Rank {final_diag['t_rank']:.2f}s ｜ 歷史特徵 {final_diag['t_features']:.2f}s ｜ 五檔補強 {final_diag['t_enrich']:.2f}s ｜ Total {final_diag['total']:.2f}s"
        )
        st.caption(
            f"YF 分段成功 {final_diag.get('yf_parts_ok', 0)} ｜ 失敗 {final_diag.get('yf_parts_fail', 0)} ｜ Feature fail {final_diag.get('feature_fail', 0)} ｜ Other err {final_diag.get('other_err', 0)}"
        )
        if final_diag.get("last_errors"):
            render_error_panel(list(final_diag["last_errors"]))

    with st.expander("🎯 戰損與淘汰名單", expanded=True):
        for reason, items in stats.items():
            if isinstance(items, list) and items:
                st.markdown(f"**{reason}**")
                st.markdown('<div class="fail-bag">' + ''.join([f'<span class="fail-tag">{x}</span>' for x in items]) + '</div>', unsafe_allow_html=True)

    st.markdown("<hr>", unsafe_allow_html=True)
    st.subheader("主力候選卡")
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
    <div class="stat-pill"><div class="stat-k">日內強度</div><div class="stat-v">{row['日內強度']:.2f}</div></div>
    <div class="stat-pill"><div class="stat-k">爆量倍率</div><div class="stat-v">{row['爆量']:.2f}x</div></div>
    <div class="stat-pill"><div class="stat-k">距漲停</div><div class="stat-v">{row['距漲停%']:.2f}%</div></div>
    <div class="stat-pill"><div class="stat-k">52W 接近</div><div class="stat-v">{row['52w接近%']:.1f}%</div></div>
  </div>
</div>
""",
                    unsafe_allow_html=True,
                )
    else:
        st.warning("⚠️ 當前設定下沒有標的通過濾網。你可以先打開『寬鬆測試模式』看候選池，再反推門檻。")

    with st.expander("🧪 替身驗證面板（近 126 交易日）", expanded=False):
        st.caption("這裡是日線替身驗證，不是盤中逐秒真回測。用途是檢查你的濾網精神在近期市場是否有訊號品質，而不是保證實盤績效。")
        b1, b2, b3, b4, b5 = st.columns(5)
        b1.metric("訊號數", bt_stats["signals"])
        b2.metric("勝率", f"{bt_stats['win_rate']}%")
        b3.metric("平均報酬", f"{bt_stats['avg_return']}%")
        b4.metric("中位數", f"{bt_stats['median_return']}%")
        b5.metric("最佳 / 最差", f"{bt_stats['best']}% / {bt_stats['worst']}%")
        if not bt_df.empty:
            bt_show = make_backtest_display(bt_df)
            render_backtest_table(bt_show)
            st.caption("表格已改成桌機友善的高對比深色版，欄位固定、數字靠右、報酬率用色塊顯示，長時間盯盤會比預設白底表格舒服很多。")
        else:
            st.info("目前替身驗證沒有產生足夠訊號，常見原因是血統濾網太嚴、候選池太窄，或近 126 日這批股票沒有足夠符合條件的事件。")

else:
    st.info("先按上方按鈕建立資料金庫，之後切換濾網才會進入真正的瞬切模式。")
