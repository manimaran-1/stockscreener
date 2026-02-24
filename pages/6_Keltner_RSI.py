import streamlit as st
import pandas as pd
import sys
import os
import pytz

# Allow sibling imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import keltner_data_loader as data_loader
import keltner_scanner as scanner

# Page Config
st.set_page_config(
    page_title="Keltner + RSI Scanner",
    page_icon="📈",
    layout="wide"
)

st.title("Keltner Channels + RSI Reversal Scanner")
st.markdown("""
This independent scanner identifies **mean-reversion** opportunities by combining Keltner Channels (Volatility) and RSI (Momentum). 
* **Bullish Entry:** RSI is Oversold (< 30) AND the candle's Low price touches or drops below the Lower Keltner Channel.
* **Bearish Entry:** RSI is Overbought (> 70) AND the candle's High price touches or crosses the Upper Keltner Channel.
""")

# --- Sidebar Controls ---
st.sidebar.header("Scanner Settings")

# 1. Select Universe
# 1. Index Selection
try:
    indices = data_loader.INDICES_SLUGS
except Exception:
    indices = data_loader.get_all_indices_dict()

# Market Stats Categories
market_stats = [
    "Top Gainers", 
    "Top Losers", 
    "Most Active (Value)", 
    "Most Active (Volume)", 
    "52 Week High", 
    "52 Week Low"
]

st.sidebar.subheader("1A. Scan Mode")
scan_mode = st.sidebar.radio("Mode", ["Full Index Scan", "Pre-Filter (Market Movers)"], index=0, help="Full Index computes every stock. Pre-Filter isolates only the highest volume/moving stocks today.", horizontal=True)

# 1B. Base Universe Selection
index_options = ["Custom List"] + market_stats + list(indices.keys())
selected_index = st.sidebar.selectbox("Select Base Universe", index_options, index=0)

# Caching logic for rapid loads (using Streamlit Session State for progress bars instead of deep cache locks)
if 'nifty500_stats' not in st.session_state:
    st.session_state['nifty500_stats'] = None

def get_nifty500_stats_with_progress():
    if st.session_state['nifty500_stats'] is not None:
        return st.session_state['nifty500_stats']

    st.info("Fetching market data (Fresh Load)...")
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    def update_progress(current, total):
        progress = min(float(current) / max(float(total), 1.0), 1.0)
        progress_bar.progress(min(float(progress), 1.0))
        status_text.text(f"Fetching: Batch {current} of {total}...")
    
    df = data_loader.fetch_nifty500_stats(progress_callback=update_progress)
    st.session_state['nifty500_stats'] = df
    
    progress_bar.empty()
    status_text.empty()
    return df

if selected_index in market_stats:
    import uuid
    if st.sidebar.button("🔄 Force Fetch New Data", key=f"refresh_btn_{uuid.uuid4().hex[:8]}"):
        st.session_state['nifty500_stats'] = None
        st.toast("Cache cleared! Fetching new data...", icon="🔄")
        st.rerun()

custom_symbols = ""
if selected_index == "Custom List":

    custom_input = st.sidebar.text_area("Enter Symbols (comma separated)", "RELIANCE.NS, TCS.NS, INFY.NS")
    if custom_input:
        custom_symbols = [s.strip().upper() for s in custom_input.split(',')]
        # Ensure .NS suffix if not provided and it's a typical Indian stock format
        custom_symbols = [s if s.endswith('.NS') or s.endswith('.BO') else f"{s}.NS" for s in custom_symbols]

# 2. Select Timeframe
st.sidebar.subheader("2. Timeframe")
timeframe_options = ['1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h', '1d', '5d', '1wk', '1mo', '3mo']
active_timeframe = st.sidebar.selectbox("Interval", timeframe_options, index=8) # Default 1d

# 3. Keltner & RSI Parameters
st.sidebar.subheader("3. Indicator Settings")

kc_length = st.sidebar.number_input("Keltner length", min_value=1, max_value=200, value=20)
kc_mult = st.sidebar.number_input("Keltner Multiplier", min_value=0.1, max_value=10.0, value=1.0, step=0.1)
kc_bands_style = st.sidebar.selectbox("Bands Style", ["True Range", "Average True Range"], index=0)
kc_atr_length = st.sidebar.number_input("ATR Length", min_value=1, max_value=200, value=10)
rsi_length = st.sidebar.number_input("RSI Length", min_value=1, max_value=100, value=14)

# 4. Filtering Options
st.sidebar.subheader("4. Data Filters")
show_all = st.sidebar.checkbox("Show All Stocks (Ignore Signal Rules)", value=False)
st.sidebar.caption("Check this to display indicator values for every stock in the universe regardless of if they fired an alert.")

use_date_filter = st.sidebar.checkbox("Filter by Date Range", value=True)
start_date = None
end_date = None
if use_date_filter:
    import datetime
    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_date = st.date_input("Start Date", datetime.date.today() - datetime.timedelta(days=30))
    with col2:
        end_date = st.date_input("End Date", datetime.date.today())

st.sidebar.markdown("---")
force_refresh = st.sidebar.checkbox("🔄 Force Refresh Data", help="Check this to clear the cache and download fresh live data from Yahoo Finance. Otherwise, data gets cached for 30 minutes to allow extremely fast timeframe switching.")

# --- Main Logic ---

if st.button("Run Scan"):
    symbols = []
    
    # Generate a cache busting token if checked
    force_refresh_token = datetime.now().timestamp() if force_refresh else None
    
    # Fetch Base Universe Symbols
    if selected_index == "Custom List":
        custom_val = custom_symbols if custom_symbols else custom_input
        if isinstance(custom_val, list):
            symbols = [s.strip().upper() for s in custom_val if s.strip()]
        else:
            symbols = [s.strip().upper() for s in custom_val.split(',') if s.strip()]
        if not symbols:
             st.warning("Please enter at least one symbol.")
             st.stop()
    elif selected_index in market_stats:
        with st.spinner(f"Fetching market movers: {selected_index}..."):
            df_stats = get_nifty500_stats_with_progress()
            if df_stats is not None and not df_stats.empty:
                 symbols = data_loader.get_market_movers(selected_index, df_stats)
            else:
                 symbols = []
    elif selected_index == "Nifty 500":
        with st.spinner("Fetching Nifty 500 symbol list..."):
             symbols = data_loader.get_nifty500_symbols()
    elif selected_index == "Nifty 200":
        with st.spinner("Fetching Nifty 200 symbol list..."):
             symbols = data_loader.get_nifty200_symbols()
    elif selected_index == "Nifty 50":
        with st.spinner("Fetching Nifty 50 symbol list..."):
             symbols = data_loader.get_nifty200_symbols()[:50]
    elif selected_index == "Total Market (All Stocks)":
        with st.spinner("Fetching Total Market..."):
             symbols = data_loader.get_index_constituents("Total Market")
             if not symbols: symbols = data_loader.get_nifty500_symbols()
    else:
        try:
             with st.spinner(f"Fetching {selected_index} symbols..."):
                  symbols = data_loader.get_index_constituents(selected_index)
        except Exception:
             symbols = []
             
    # Automatically apply Market Mover intersection if 'Pre-Filter' mode is active AND a generic index was selected
    if scan_mode == "Pre-Filter (Market Movers)" and symbols and selected_index not in market_stats and selected_index != "Custom List":
        with st.spinner(f"Applying auto-Market Movers Pre-Filter to {selected_index}..."):
            df_stats = get_nifty500_stats_with_progress()
            if df_stats is not None and not df_stats.empty:
                 vol_movers = data_loader.get_market_movers("Most Active (Volume)", df_stats)
                 gainers = data_loader.get_market_movers("Top Gainers", df_stats)
                 losers = data_loader.get_market_movers("Top Losers", df_stats)
                 all_movers = set(vol_movers + gainers + losers)
                 
                 base_set = {s.split('.')[0] for s in symbols}
                 filtered_symbols = []
                 for m in all_movers:
                     m_base = m.split('.')[0]
                     if m_base in base_set:
                         filtered_symbols.append(m)
                 
                 symbols = filtered_symbols
                 if not symbols:
                      st.warning(f"No stocks in '{selected_index}' qualified as top market movers today.")
                      st.stop()
    if symbols:
        results = []
        total_symbols = len(symbols)
        completed = 0
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        def update_scan_progress(current, total, msg=None):
            progress = min(float(current) / max(float(total), 1.0), 1.0)
            progress_bar.progress(min(float(progress), 1.0))
            if msg:
                status_text.text(msg)
            else:
                status_text.text(f"Scanning: {current} of {total} symbols completed...")
        
        # removed pre-filter logic to scan all stocks per user request

        settings = {
            'kc_length': kc_length,
            'kc_mult': kc_mult,
            'kc_bands_style': kc_bands_style,
            'kc_atr_length': kc_atr_length,
            'rsi_length': rsi_length
        }
        
        # We delegate to scanner.scan_market which handles the massive bulk data fetch optimization automatically
        with st.spinner("Processing..."):
            results_df = scanner.scan_market(symbols, active_timeframe, settings, start_date, end_date, show_all, force_refresh_token=force_refresh_token, progress_callback=update_scan_progress)
        
        progress_bar.empty()
        status_text.empty()

        st.session_state['res_6_Keltner_RSI.py'] = results_df
        st.session_state['ctx_6_Keltner_RSI.py'] = {'index': selected_index, 'tf': active_timeframe}

if 'res_6_Keltner_RSI.py' in st.session_state and st.session_state['res_6_Keltner_RSI.py'] is not None:
    results_df = st.session_state['res_6_Keltner_RSI.py'].copy()
    context = st.session_state.get('ctx_6_Keltner_RSI.py', {'index': 'Custom', 'tf': '1d'})

    if results_df is not None and not results_df.empty:
        if not show_all:
            results_df = results_df[results_df['Signal Type'] != "None"]
            
    if results_df is not None and not results_df.empty:
        st.success(f"Scan complete! Found {len(results_df)} stocks.")
        
        # Format UI rendering
        def format_signal(val):
            color = "green" if val == "Bullish" else "red" if val == "Bearish" else "gray"
            return f"color: {color}"
        
        
        def style_dataframe(df):
            def highlight_signal(val):
                if val in ['Buy', 'Bullish', 'Long Entry', 'Supertrend Buy']:
                    return 'color: #00FF00; font-weight: bold; background-color: rgba(0, 255, 0, 0.1)'
                elif val in ['Sell', 'Bearish', 'Short Entry', 'Supertrend Sell']:
                    return 'color: #FF0000; font-weight: bold; background-color: rgba(255, 0, 0, 0.1)'
                return ''
            
            def highlight_trend(val):
                if val == 'Uptrend':
                    return 'color: #00FF00'
                elif val == 'Downtrend':
                    return 'color: #FF0000'
                return ''
                
            return df.style.map(highlight_signal, subset=['Signal Type']).map(highlight_trend, subset=['Trend']) if 'Trend' in df.columns else df.style.map(highlight_signal, subset=['Signal Type'])

        styled_df = style_dataframe(results_df)

        st.dataframe(
            styled_df,
            use_container_width=True,
            hide_index=True,
            height=500
        )

        
        # Export
        csv = results_df.to_csv(index=False)
        st.download_button(
            label="Download Results as CSV",
            data=csv,
            file_name=f"keltner_rsi_scan_{context['index'].replace(' ', '_')}_{context['tf']}.csv",
            mime="text/csv"
        )
    else:
        if not show_all:
            st.warning(f"No signals found for {context['index']} on {context['tf']} timeframe within the selected dates.")
        else:
            st.warning(f"No data retrieved for {context['index']}.")

# --- Explanation Section ---
st.markdown("---")
st.subheader("💡 How This Scanner Works")

try:
    st.image("keltner_rsi_strategy.png", use_container_width=True)
except Exception:
    pass
    
st.markdown("""
This scanner identifies **Mean Reversion** setups based on the standard **Keltner Channel & RSI** strategy. It looks for moments where a stock's price has stretched too far out of its normal bounds (like a rubber band) and is mathematically likely to snap back.

### 🎯 Signal Generation
*   **🟢 Bullish Signal (Buy):** The stock's price touches or dips below the **Lower Keltner Band**, AND the RSI is strictly **<= 30** (Oversold). The strategy anticipates the price will bounce back up towards the Middle Band.
*   **🔴 Bearish Signal (Sell/Short):** The stock's price touches or breaks above the **Upper Keltner Band**, AND the RSI is strictly **>= 70** (Overbought). The strategy anticipates the price will drop back down towards the Middle Band.

### 📊 Understanding the Table Columns
*   **Signal:** The Mean-Reversion trade entry triggered by the strategy logic (Bullish=Buy, Bearish=Short).
*   **Trend (EMA 21):** The overall long-term direction of the stock. It is completely independent of the Signal. If the Last Traded Price (LTP) is higher than its 21-period Exponential Moving Average, the underlying momentum trend is *Bullish*. If the LTP is lower, it is *Bearish*. A setup where the *Signal* contradicts the *Trend* (e.g., Bearish Signal during a Bullish Trend) implies you are betting on a temporary pullback.
*   **KC Lower / Middle / Upper:** The real-time values of the Keltner Channels. The Middle line acts as a standard **Take Profit** target for Mean Reversion entries.
*   **Pivot (Best SL/TP):** Uses the recent high/low nearest to the entry point to recommend extreme physiological Stop Loss and Take Profit bounds.
*   **EMA 21 SL:** A trailing stop-loss strategy utilizing the 21-period momentum moving average.
*   **KC+ATR (SL/TP):** A volatility-adjusted stop mechanism pushing the bounds just outside the Keltner extremes to prevent wicks from stopping you out.
""")
