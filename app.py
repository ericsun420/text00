import streamlit as st
import yfinance as yf
import pandas as pd
import requests
from datetime import datetime, timedelta

st.set_page_config(page_title="每日飆股自動掃描器", page_icon="🎯", layout="wide")
st.title("🎯 每日飆股自動掃描器")
st.write("一鍵掃描全市場，找出今天符合「爆量2.5倍 + 突破20日高點 + 實體長紅」的潛力股！")

# --- 1. 自動抓取全台股代碼的功能 (改良版) ---
@st.cache_data(ttl=86400) 
def get_all_twse_stocks():
    try:
        # 加入 User-Agent 偽裝成一般瀏覽器，避免被證交所防火牆阻擋
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
        res = requests.get(url, headers=headers, timeout=10)
        data = res.json()
        
        codes = [item['Code'] for item in data if len(item['Code']) == 4]
        return codes
    except Exception as e:
        st.warning("目前被證交所防火牆阻擋連線，已為您自動載入精選熱門股名單。")
        # 準備一份夠長的備用清單（包含權值股、重電變壓器、AI 伺服器、航運等熱門股）
        fallback_list = [
            '2330', '2317', '2454', '2382', '2489', '2371', '3231', '3017', '2603', '2609',
            '1519', '1514', '1503', '3450', '3443', '3037', '2368', '3008', '2301', '2308',
            '2881', '2882', '2891', '2356', '2324', '3293', '2383', '3044', '2313', '2352'
        ]
        return fallback_list

# --- 2. 畫面 UI：讓使用者選擇模式 ---
scan_mode = st.radio(
    "請選擇掃描範圍：", 
    ["📈 自動抓取全市場 (約 1000 檔上市股票，掃描需 1~3 分鐘)", "✏️ 自訂 / 觀察名單 (手動輸入)"]
)

if scan_mode.startswith("📈"):
    # 自動模式
    stock_list = get_all_twse_stocks()
    st.info(f"✅ 已自動從證交所載入 {len(stock_list)} 檔上市股票代碼！您可以直接點擊下方按鈕開始掃描。")
else:
    # 自訂模式
    DEFAULT_STOCK_LIST = ['2330', '2317', '2454', '2382', '2489', '2371', '3231', '3017', '2603', '1519']
    stock_input = st.text_area("請輸入要掃描的股票代碼 (請用半形逗號分隔)", value=",".join(DEFAULT_STOCK_LIST))
    stock_list = [s.strip() for s in stock_input.split(',') if s.strip()]

st.divider()

# --- 3. 核心掃描邏輯 ---
@st.cache_data(ttl=3600)
def scan_market(tickers):
    results = []
    start_date = datetime.today() - timedelta(days=60)
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i, ticker in enumerate(tickers):
        # 更新進度條
        status_text.text(f"正在掃描: {ticker} ({i+1}/{len(tickers)})")
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
            
            cond_vol = last_row['Volume'] > (prev_row['Vol_MA20'] * 2.5)
            cond_price = last_row['Close'] >= prev_row['Price_Max20']
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
            
    status_text.text(f"掃描完成！共掃描了 {len(tickers)} 檔股票。")
    return pd.DataFrame(results)

# --- 4. 執行按鈕 ---
if st.button("🚀 立即掃描今日飆股", type="primary"):
    if len(stock_list) == 0:
        st.error("股票清單不能為空！")
    else:
        with st.spinner("系統正在努力幫您抓取全市場數據，這可能需要幾分鐘，請喝杯水稍候..."):
            scan_results_df = scan_market(stock_list)
            
            st.subheader("📊 今日符合『爆量突破』條件的股票清單")
            
            if not scan_results_df.empty:
                st.dataframe(scan_results_df, use_container_width=True)
                st.success(f"太棒了！為您抓出了 {len(scan_results_df)} 檔潛力股。")
            else:
                st.info("今天市場比較平淡，沒有符合條件的標的喔！")

