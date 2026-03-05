import streamlit as st
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

# --- 網頁基本設定 ---
st.set_page_config(page_title="每日飆股戰情室", page_icon="🚀", layout="wide")

# --- 自訂 CSS 樣式 ---
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
        font-size: 16px;
        margin-bottom: 20px;
        letter-spacing: 1px;
    }
    .golden-time-box {
        background-color: #fff3e0;
        border-left: 5px solid #ff9800;
        padding: 15px 20px;
        border-radius: 5px;
        margin-bottom: 30px;
        color: #333;
    }
    .stProgress > div > div > div > div {
        background-color: #ff4b4b;
    }
</style>
""", unsafe_allow_html=True)

# --- 標題與提示區 ---
st.markdown('<div class="main-title">🚀 每日飆股戰情室</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-title">全自動掃描 ｜ 爆量 2.5 倍 ｜ 突破 20 日高點 ｜ 過濾避雷針 ｜ 流動性大於 2000 張</div>', unsafe_allow_html=True)

st.markdown('''
<div class="golden-time-box">
    <b>💡 實戰黃金 10 分鐘提示：</b><br>
    建議在每天 <b>13:15 ~ 13:25</b> 點開網頁自動掃描。此時今日 K 線長相已大致底定，若出現「👼 浪子回頭」標的，即可在 13:30 收盤前提前卡位，避免隔天開盤跳空買不到！
</div>
''', unsafe_allow_html=True)

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

# --- 2. 核心掃描邏輯 ---
@st.cache_data(ttl=3600) 
def scan_market(tickers):
    results = []
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
            if df.empty or len(df) < 65: continue 
            if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.droplevel(1)
                
            df['Vol_MA20'] = df['Volume'].rolling(window=20).mean()
            df['Price_Max20'] = df['Close'].rolling(window=20).max()
            df['MA20'] = df['Close'].rolling(window=20).mean() 
            df['MA60'] = df['Close'].rolling(window=60).mean() 
            df['Daily_Return'] = df['Close'].pct_change()
            
            last_row = df.iloc[-1]
            prev_row = df.iloc[-2]
            
            # --- 原始核心條件 ---
            cond_vol = last_row['Volume'] > (prev_row['Vol_MA20'] * 2.5)
            cond_price = last_row['Close'] >= prev_row['Price_Max20']
            cond_red_candle = last_row['Daily_Return'] > 0.04
            cond_above_ma60 = last_row['Close'] > last_row['MA60'] 
            
            # --- 新增防禦機制 1：流動性防禦 (大於 2000 張) ---
            # yfinance 台股 Volume 單位是股數，2000 張 = 2,000,000 股
            cond_min_vol = last_row['Volume'] >= 2_000_000 
            
            # --- 新增防禦機制 2：避雷針過濾 (上影線長度不得超過當天波動的 30%) ---
            daily_range = last_row['High'] - last_row['Low']
            upper_shadow = last_row['High'] - last_row['Close']
            # 如果當天有波動，上影線比例必須小於等於 0.3；若當天一字漲停無波動 (daily_range=0)，則直接通過
            cond_shadow = (upper_shadow <= daily_range * 0.3) if daily_range > 0 else True
            
            # 必須滿足所有條件才放行
            if cond_vol and cond_price and cond_red_candle and cond_above_ma60 and cond_min_vol and cond_shadow:
                
                # 型態分類器
                if last_row['MA20'] > last_row['MA60']:
                    pattern_tag = "🔥 多頭強勢"
                else:
                    pattern_tag = "👼 浪子回頭"
                    
                results.append({
                    "股票代號": ticker,
                    "型態": pattern_tag,
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
with st.spinner(" "): 
    scan_results_df = scan_market(stock_list)
    
    if not scan_results_df.empty:
        st.success(f"🎯 鎖定目標！今日共發現 **{len(scan_results_df)}** 檔符合高勝率起漲型態的潛力股。")
        st.divider()
        
        cols = st.columns(min(len(scan_results_df), 4))
        
        for index, row in scan_results_df.iterrows():
            col = cols[index % len(cols)]
            with col:
                st.metric(
                    label=f"{row['型態']}：{row['股票代號']}", 
                    value=f"{row['最新收盤價']:.2f}", 
                    delta=f"漲幅 {row['單日漲跌幅']:.2f}%",
                    delta_color="inverse"
                )
                st.caption(f"成交量: {row['今日成交量']} 張 | 爆量: {row['爆量倍數']:.1f} 倍")
        
        st.divider()
        
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
        st.info("📉 今日掃描完畢。市場資金動能較弱，或個股留有長上影線，建議多看少做！")

# --- 4. 底部重整按鈕 ---
st.markdown("<br><br>", unsafe_allow_html=True)
col1, col2, col3 = st.columns([2, 1, 2])
with col2:
    if st.button("🔄 重新掃描最新數據", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
