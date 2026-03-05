import streamlit as st
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

st.set_page_config(page_title="每日飆股自動雷達", page_icon="🎯", layout="wide")
st.title("🎯 每日飆股自動雷達")
st.write("一打開網頁，為您自動掃描台股精選 300 檔，找出今天符合「爆量2.5倍 + 突破20日高點 + 實體長紅」的潛力股！")

# --- 1. 內建台股精選高流動性名單 (300多檔) ---
@st.cache_data 
def get_all_twse_stocks():
    all_stocks_str = (
        "1101,1102,1210,1215,1216,1301,1303,1304,1308,1314,1326,1402,1434,1476,1503,1504,1513,1514,1519,1590,1605,"
        "2002,2014,2027,2049,2105,2201,2207,2301,2303,2308,2313,2317,2324,2330,2345,2352,2353,2356,2357,2371,2379,"
        "2382,2383,2385,2395,2408,2409,2412,2449,2454,2489,2603,2609,2610,2615,2618,2801,2880,2881,2882,2883,2884,"
        "2885,2886,2887,2888,2889,2890,2891,2892,2912,3008,3017,3034,3037,3044,3045,3231,3443,3450,3481,3661,3711,"
        "4904,4938,4958,5871,5880,6669,9910,2362,3013,3328,6223,6274,8050,8150,6187,6285,8081,3529,3653,5269,6414,"
        "6531,8436,1560,2049,2360,3376,3406,3592,4919,6196,6269,6412,6451,6452,1568,1582,1597,1609,1611,1612,1717,"
        "1722,1802,1904,1907,2006,2015,2028,2031,2062,2103,2104,2106,2204,2206,2233,2302,2312,2314,2323,2337,2338,"
        "2340,2344,2347,2351,2354,2368,2373,2376,2377,2387,2393,2401,2404,2420,2421,2428,2439,2441,2451,2458,2464,"
        "2474,2481,2492,2498,2504,2515,2520,2539,2542,2545,2548,2605,2607,2633,2634,2637,2707,2723,2727,2731,2812,"
        "2834,2838,2845,2850,2851,2852,2855,2903,2915,2929,2939,3005,3010,3014,3015,3019,3023,3026,3035,3041,3051,"
        "3189,3338,3406,3454,3504,3532,3533,3545,3576,3583,3605,3653,3665,3701,3702,3704,3706,4915,4961,4968,5225,"
        "5288,5522,5534,6115,6139,6153,6176,6191,6202,6213,6239,6269,6271,6278,6282,6409,6415,6443,6456,6505,6582,"
        "6592,6605,6770,6781,8016,8028,8039,8046,8081,8112,8131,8215,8454,8464,8499,8926,8996,9904,9907,9914,9921,"
        "9933,9938,9939,9941,9945,9958"
    )
    return all_stocks_str.split(",")

stock_list = get_all_twse_stocks()

# --- 2. 核心掃描邏輯 ---
@st.cache_data(ttl=3600) # 快取 1 小時，如果你1小時內重複開網頁，直接秒秀結果不用重跑
def scan_market(tickers):
    results = []
    start_date = datetime.today() - timedelta(days=60)
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i, ticker in enumerate(tickers):
        status_text.text(f"🚀 自動雷達掃描中: {ticker} ({i+1}/{len(tickers)})... 請稍候")
        progress_bar.progress((i + 1) / len(tickers))
        
        try:
            df = yf.download(f"{ticker}.TW", start=start_date, progress=False)
            if df.empty or len(df) < 21:
                continue
                
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.droplevel(1)
                
            df['Vol_MA20'] = df['Volume'].rolling(window=20).mean()
            df['Price_Max20'] = df['Close'].rolling(window=20).max()
            df['Daily_Return'] = df['Close'].pct_change()
            
            last_row = df.iloc[-1]
            prev_row = df.iloc[-2]
            
            # 條件 A: 爆量2.5倍
            cond_vol = last_row['Volume'] > (prev_row['Vol_MA20'] * 2.5)
            # 條件 B: 突破區間高點
            cond_price = last_row['Close'] >= prev_row['Price_Max20']
            # 條件 C: 實體紅K，漲幅 > 4%
            cond_red_candle = last_row['Daily_Return'] > 0.04
            
            if cond_vol and cond_price and cond_red_candle:
                results.append({
                    "股票代號": ticker,
                    "最新收盤價": round(float(last_row['Close']), 2),
                    "單日漲跌幅(%)": f"{round(float(last_row['Daily_Return']) * 100, 2)}%",
                    "今日成交量(張)": int(last_row['Volume'] / 1000),
                    "爆量倍數": round(float(last_row['Volume'] / prev_row['Vol_MA20']), 1)
                })
        except Exception:
            continue
            
    # 掃描結束清空進度條文字
    status_text.empty() 
    progress_bar.empty()
    return pd.DataFrame(results)

# --- 3. 一開網頁直接自動執行，不要按鈕了！ ---
with st.spinner("系統正在努力幫您抓取全市場數據，大約需要 30 秒，請稍候..."):
    # 網頁載入到這裡時，就會自動呼叫 scan_market 函數
    scan_results_df = scan_market(stock_list)
    
    st.subheader("📊 今日符合『爆量突破』條件的股票清單")
    
    if not scan_results_df.empty:
        st.dataframe(scan_results_df, use_container_width=True)
        st.success(f"太棒了！為您抓出了 {len(scan_results_df)} 檔像瑞軒當初起漲的潛力股。")
    else:
        st.info("今天市場比較平淡，沒有符合條件的標的喔！")

# 給你一個手動重新整理的按鈕（如果你等不及快取的1小時，想硬刷新的話）
st.divider()
if st.button("🔄 強制重新掃描最新數據"):
    st.cache_data.clear()
    st.rerun()
