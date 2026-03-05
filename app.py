import streamlit as st
import yfinance as yf
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta

# --- 1. 網頁基本設定 ---
st.set_page_config(page_title="第一批飆股雷達與回測", page_icon="📈", layout="wide")
st.title("🎯 突破爆量飆股：回測與掃描系統")

# --- 2. 側邊欄：使用者輸入區 ---
st.sidebar.header("設定回測參數")
# 預設股票代號為大同 (2371)
ticker_input = st.sidebar.text_input("輸入台股代號 (純數字)", value="2371")
ticker = f"{ticker_input}.TW"

# 設定預設日期 (抓取過去 8 個月的資料，確保有足夠時間計算均線與回測)
end_date_default = datetime.today()
start_date_default = end_date_default - timedelta(days=240) 

start_date = st.sidebar.date_input("開始日期", value=start_date_default)
end_date = st.sidebar.date_input("結束日期", value=end_date_default)

# --- 3. 核心功能：抓取資料與計算 ---
@st.cache_data # 使用快取，避免每次操作網頁都重新下載資料
def load_data(ticker, start, end):
    df = yf.download(ticker, start=start, end=end, progress=False)
    if df.empty:
        return None
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(1)
    
    # 技術指標計算
    df['Vol_MA20'] = df['Volume'].rolling(window=20).mean()
    df['Price_Max20'] = df['Close'].rolling(window=20).max()
    df['MA10'] = df['Close'].rolling(window=10).mean()
    df['Daily_Return'] = df['Close'].pct_change()
    
    # 進場條件：爆量2.5倍 + 突破20日高點 + 實體紅K(漲幅>4%)
    cond_vol = df['Volume'] > (df['Vol_MA20'].shift(1) * 2.5)
    cond_price = df['Close'] >= df['Price_Max20'].shift(1)
    cond_red_candle = df['Daily_Return'] > 0.04
    df['Buy_Signal'] = cond_vol & cond_price & cond_red_candle
    
    return df

# --- 4. 執行回測邏輯 ---
def run_backtest(df):
    in_position = False
    buy_price = 0
    trades = []
    
    for i in range(20, len(df)):
        current_date = df.index[i]
        close_price = df['Close'].iloc[i]
        
        # 出場：跌破10日線
        if in_position:
            if close_price < df['MA10'].iloc[i]:
                profit_pct = (close_price - buy_price) / buy_price * 100
                trades.append({
                    '買進日期': buy_date.strftime('%Y-%m-%d'),
                    '賣出日期': current_date.strftime('%Y-%m-%d'),
                    '報酬率(%)': round(profit_pct, 2)
                })
                in_position = False
        # 進場：出現訊號
        elif not in_position and df['Buy_Signal'].iloc[i]:
            buy_price = close_price
            buy_date = current_date
            in_position = True
            
    return pd.DataFrame(trades)

# --- 5. 畫面渲染與圖表繪製 ---
if st.sidebar.button("開始回測"):
    with st.spinner('正在抓取資料與計算中...'):
        df = load_data(ticker, start_date, end_date)
        
        if df is None:
            st.error("找不到該檔股票的資料，請確認代號是否正確。")
        else:
            trades_df = run_backtest(df)
            
            # 頂部數據卡片 (直覺呈現績效)
            st.subheader(f"📊 {ticker_input} 回測績效總結")
            col1, col2, col3 = st.columns(3)
            
            total_trades = len(trades_df)
            if total_trades > 0:
                win_rate = len(trades_df[trades_df['報酬率(%)'] > 0]) / total_trades * 100
                total_return = trades_df['報酬率(%)'].sum()
                
                col1.metric("總交易次數", f"{total_trades} 次")
                col2.metric("策略勝率", f"{win_rate:.1f} %")
                col3.metric("累積報酬率", f"{total_return:.2f} %", delta=f"{total_return:.2f}%", delta_color="normal" if total_return > 0 else "inverse")
            else:
                st.info("這段期間內沒有出現符合條件的進場訊號。")
            
            st.divider()
            
            # 互動式 K 線圖 (使用 Plotly)
            st.subheader("📈 股價走勢與 10 日均線")
            fig = go.Figure()
            # 畫 K 線
            fig.add_trace(go.Candlestick(x=df.index, open=df['Open'], high=df['High'], low=df['Low'], close=df['Close'], name='K線'))
            # 畫 10 日均線 (出場防守線)
            fig.add_trace(go.Scatter(x=df.index, y=df['MA10'], mode='lines', name='10日均線', line=dict(color='blue', width=1.5)))
            
            # 標註進場點 (出現 Buy_Signal 的地方)
            buy_signals = df[df['Buy_Signal']]
            fig.add_trace(go.Scatter(x=buy_signals.index, y=buy_signals['Low'] * 0.95, mode='markers', name='進場訊號', marker=dict(symbol='triangle-up', size=12, color='red')))
            
            fig.update_layout(height=500, xaxis_rangeslider_visible=False, template="plotly_white")
            st.plotly_chart(fig, use_container_width=True)
            
            # 交易明細表格
            if total_trades > 0:
                st.subheader("📝 交易明細紀錄")
                st.dataframe(trades_df, use_container_width=True)
