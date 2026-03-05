import streamlit as st
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

st.set_page_config(page_title="每日飆股自動掃描器", page_icon="🎯", layout="wide")
st.title("🎯 每日飆股自動掃描器")
st.write("一鍵掃描全市場，找出今天符合「爆量2.5倍 + 突破20日高點 + 實體長紅」的潛力股！")

# 1. 設定要掃描的股票池
# 實務上這裡可以放幾百檔台股代碼，為了示範速度，這裡先放一些熱門股與近期波動股
# 你可以隨時在這個陣列裡面新增你想觀察的台股代號
DEFAULT_STOCK_LIST = [
    '2330', '2317', '2454', '2382', '2489', '2371', '2362', '3231', '3017', '2301',
    '2603', '2609', '2615', '1519', '1514', '3450', '3443', '3037', '2368', '3008'
]

stock_input = st.text_area("要掃描的股票代碼 (請用半形逗號分隔)", value=",".join(DEFAULT_STOCK_LIST))
stock_list = [s.strip() for s in stock_input.split(',')]

# 2. 核心掃描邏輯
@st.cache_data(ttl=3600) # 快取 1 小時，避免重複點擊時一直重新下載
def scan_market(tickers):
    results = []
    # 只需要抓過去 40 天的資料就足夠計算 20 日均線和昨天的 20 日高點了
    start_date = datetime.today() - timedelta(days=60)
    
    # 建立進度條，讓你知道掃描到哪裡
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i, ticker in enumerate(tickers):
        status_text.text(f"正在掃描: {ticker} ({i+1}/{len(tickers)})")
        progress_bar.progress((i + 1) / len(tickers))
        
        try:
            # 抓取資料
            df = yf.download(f"{ticker}.TW", start=start_date, progress=False)
            if df.empty or len(df) < 21:
                continue
                
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.droplevel(1)
                
            # 計算指標
            df['Vol_MA20'] = df['Volume'].rolling(window=20).mean()
            df['Price_Max20'] = df['Close'].rolling(window=20).max()
            df['Daily_Return'] = df['Close'].pct_change()
            
            # 取得「最新一天」的資料與「前一天」的資料
            last_row = df.iloc[-1]
            prev_row = df.iloc[-2]
            
            # 判斷條件
            # A: 今日成交量 > 昨天算出的20日均量 * 2.5
            cond_vol = last_row['Volume'] > (prev_row['Vol_MA20'] * 2.5)
            # B: 今日收盤價 >= 昨天算出的過去20日最高價
            cond_price = last_row['Close'] >= prev_row['Price_Max20']
            # C: 實體紅K，今日漲幅 > 4%
            cond_red_candle = last_row['Daily_Return'] > 0.04
            
            if cond_vol and cond_price and cond_red_candle:
                results.append({
                    "股票代號": ticker,
                    "最新收盤價": round(float(last_row['Close']), 2),
                    "單日漲跌幅(%)": f"{round(float(last_row['Daily_Return']) * 100, 2)}%",
                    "今日成交量(張)": int(last_row['Volume'] / 1000), # yfinance的台股Volume通常是股數，除以1000變張數
                    "爆量倍數": round(float(last_row['Volume'] / prev_row['Vol_MA20']), 1)
                })
        except Exception as e:
            continue # 遇到下市或抓不到的股票就跳過
            
    status_text.text("掃描完成！")
    return pd.DataFrame(results)

# 3. 觸發掃描與顯示結果
if st.button("🚀 立即掃描今日飆股", type="primary"):
    with st.spinner("系統正在努力幫您掃描市場中，請稍候..."):
        scan_results_df = scan_market(stock_list)
        
        st.divider()
        st.subheader("📊 今日符合『爆量突破』條件的股票清單")
        
        if not scan_results_df.empty:
            # 顯示結果表格
            st.dataframe(scan_results_df, use_container_width=True)
            st.success(f"太棒了！在 {len(stock_list)} 檔股票中，為您抓出了 {len(scan_results_df)} 檔潛力股。")
        else:
            st.info("今天市場比較平淡，您設定的股票池中沒有符合條件的標的喔！")
            st.image("https://images.unsplash.com/photo-1611974789855-9c2a0a7236a3?auto=format&fit=crop&q=80&w=800", caption="耐心等待好球進壘，才是獲利的關鍵。")
