import streamlit as st
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

# --- 網頁基本設定 (開啟寬螢幕模式) ---
st.set_page_config(page_title="每日飆股自動雷達", page_icon="🚀", layout="wide")

# --- 自訂 CSS 樣式 (注入美化靈魂) ---
st.markdown("""
<style>
    .main-title {
        font-size: 48px;
        font-weight: 900;
        background: -webkit-linear-gradient(45deg, #ff4b4b, #ff8f00);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 0px;
        padding-top: 20px;
    }
    .sub-title {
        text-align: center;
        color: #888888;
        font-size: 18px;
        margin-bottom: 40px;
        letter-spacing: 2px;
    }
    .stProgress > div > div > div > div {
        background-color: #ff4b4b; /* 把進度條變成熱血的紅色 */
    }
</style>
""", unsafe_allow_html=True)

# --- 標題區 ---
st.markdown('<div class="main-title">🚀 每日飆股戰情室</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-title">全自動掃描 ｜ 爆量 2.5 倍 ｜ 突破 20 日高點 ｜ 第一根實體長紅</div>', unsafe_allow_html=True)

# --- 1. 內建台股精選高流動性名單 ---
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

# --- 2. 核心掃描邏輯 (加入季線濾網，過濾假突破) ---
@st.cache_data(ttl=3600) 
def scan_market(tickers):
    results = []
    # 【重要修改 1】：因為要算 60 日季線，我們抓資料的時間要拉長到 120 天前（確保有足夠的交易日可算）
    start_date = datetime.today() - timedelta(days=120) 
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        status_text = st.empty()
        progress_bar = st.progress(0)
    
    for i, ticker in enumerate(tickers):
        status_text.markdown(f"<p style='text-align: center; color: #666;'>📡 正在接收市場訊號：掃描 <b>{ticker}</b> ({i+1}/{len(tickers)})</p>", unsafe_allow_html=True)
        progress_bar.progress((i + 1) / len(tickers))
        
        try:
            df = yf.download(f"{ticker}.TW", start=start_date, progress=False)
            if df.empty or len(df) < 65: continue # 確保資料夠算季線
            if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.droplevel(1)
                
            df['Vol_MA20'] = df['Volume'].rolling(window=20).mean()
            df['Price_Max20'] = df['Close'].rolling(window=20).max()
            df['MA60'] = df['Close'].rolling(window=60).mean() # 【重要修改 2】：計算 60 日季線
            df['Daily_Return'] = df['Close'].pct_change()
            
            last_row = df.iloc[-1]
            prev_row = df.iloc[-2]
            
            cond_vol = last_row['Volume'] > (prev_row['Vol_MA20'] * 2.5)
            cond_price = last_row['Close'] >= prev_row['Price_Max20']
            cond_red_candle = last_row['Daily_Return'] > 0.04
            
            # 【重要修改 3】：防假突破濾網！收盤價必須大於季線，且季線最好是上揚的
            cond_trend = last_row['Close'] > last_row['MA60'] 
            
            # 把趨勢濾網 cond_trend 也加進去判斷
            if cond_vol and cond_price and cond_red_candle and cond_trend:
                results.append({
                    "股票代號": ticker,
                    "最新收盤價": float(last_row['Close']),
                    "單日漲跌幅": float(last_row['Daily_Return']) * 100,
                    "今日成交量": int(last_row['Volume'] / 1000),
                    "爆量倍數": float(last_row['Volume'] / prev_row['Vol_MA20'])
                })
        except Exception:
            continue
            
    status_text.empty() 
    progress_bar.empty()
    return pd.DataFrame(results)

# --- 3. 執行與精美畫面渲染 ---
with st.spinner(" "): # 隱藏預設 spinner，使用我們自訂的進度條
    scan_results_df = scan_market(stock_list)
    
    if not scan_results_df.empty:
        st.success(f"🎯 鎖定目標！今日共發現 **{len(scan_results_df)}** 檔符合起漲型態的潛力股。")
        st.divider()
        
        # 建立美觀的卡片區塊 (每排顯示 3 到 4 張卡片)
        cols = st.columns(min(len(scan_results_df), 4))
        
        for index, row in scan_results_df.iterrows():
            col = cols[index % len(cols)]
            with col:
                # 使用 st.metric 創造卡片感，delta_color="inverse" 會讓正數(上漲)顯示為紅色，符合台股習慣
                st.metric(
                    label=f"🔥 代號：{row['股票代號']}", 
                    value=f"{row['最新收盤價']:.2f}", 
                    delta=f"漲幅 {row['單日漲跌幅']:.2f}%",
                    delta_color="inverse"
                )
                st.caption(f"成交量: {row['今日成交量']} 張 | 爆量: {row['爆量倍數']:.1f} 倍")
        
        st.divider()
        
        # 下方保留完整的原始數據表，並將數字格式化得更漂亮
        st.write("📋 完整數據報表：")
        st.dataframe(
            scan_results_df.style.format({
                "最新收盤價": "{:.2f}",
                "單日漲跌幅": "{:.2f}%",
                "今日成交量": "{:,} 張",
                "爆量倍數": "{:.1f} 倍"
            }).background_gradient(subset=['單日漲跌幅', '爆量倍數'], cmap='Reds'),
            use_container_width=True
        )

    else:
        st.info("📉 今日市場資金動能較弱，沒有符合「爆量第一根」條件的標的，建議多看少做！")

# --- 4. 底部重整按鈕 ---
st.markdown("<br><br>", unsafe_allow_html=True)
col1, col2, col3 = st.columns([2, 1, 2])
with col2:
    if st.button("🔄 重新掃描最新數據", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

