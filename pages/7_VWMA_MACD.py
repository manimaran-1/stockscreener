import streamlit as st
import pandas as pd
import datetime

import vwma_macd_data_loader as data_loader
import vwma_macd_scanner as scanner

# Page Config
st.set_page_config(page_title="VWMA + MACD Scanner", page_icon="📊", layout="wide")

# Header
st.title("📊 VWMA + MACD Momentum Scanner")
st.markdown("Scan Nifty/BSE markets for VWMA and MACD Breakout momentum trades based on Fingrad VWMA crossover strategies.")

st.info("Looking for the Bollinger Bands + MACD strategy instead? 👉 [Go to BB + MACD Scanner](/BB_MACD)")

# Sidebar Settings
st.sidebar.header("Scanner Settings")

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

    custom_symbols = st.sidebar.text_area("Enter Stock Symbols (comma separated, NSE/BSE supported)", "TCS.NS, INFY.NS, RELIANCE.NS")
    st.sidebar.caption("E.g., TCS.NS, RELIANCE.NS, COFORGE.BO")

# 2. Timeframe Selection
timeframe_options = ['1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h', '1d', '5d', '1wk', '1mo', '3mo']
active_timeframe = st.sidebar.selectbox("Interval", timeframe_options, index=8) # Default 1d

# 3. Strategy Configuration
st.sidebar.markdown("---")
st.sidebar.subheader("3. Indicator Settings")

vwma_length = st.sidebar.number_input("VWMA Length", min_value=1, max_value=200, value=20)
st.sidebar.markdown("---")
macd_fast = st.sidebar.number_input("MACD Fast Length", min_value=1, max_value=200, value=12)
macd_slow = st.sidebar.number_input("MACD Slow Length", min_value=1, max_value=200, value=26)
macd_signal = st.sidebar.number_input("MACD Signal Length", min_value=1, max_value=200, value=9)

# 4. Filtering Options
st.sidebar.subheader("4. Data Filters")
show_all = st.sidebar.checkbox("Show All Stocks (Ignore Signal Rules)", value=False)
st.sidebar.caption("Check this to display indicator values for every stock in the universe regardless of if they fired an alert.")

# 5. Date Range Filtering
st.sidebar.subheader("5. Date Range Filter")
filter_by_date = st.sidebar.checkbox("Filter by Signal Date", value=True)
start_date, end_date = None, None
if filter_by_date:
    col1, col2 = st.sidebar.columns(2)
    start_date = col1.date_input("Start Date", datetime.date.today() - datetime.timedelta(days=30))
    end_date = col2.date_input("End Date", datetime.date.today())

st.sidebar.markdown("---")
force_refresh = st.sidebar.checkbox("🔄 Force Refresh Data", help="Check this to clear the cache and download fresh live data from Yahoo Finance. Otherwise, data gets cached for 30 minutes to allow extremely fast timeframe switching.")

# --- RUN SCAN ---
submit_scan = st.button("Run VWMA/MACD Scan", type="primary")

if submit_scan:
    symbols = []
    
    # Generate a cache busting token if checked
    force_refresh_token = datetime.datetime.now().timestamp() if force_refresh else None
    
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

    if not symbols:
        st.error("Could not fetch symbols for the selected index.")
    else:
        status_text = st.empty()
        progress_bar = st.progress(0)
        
        def update_scan_progress(current, total, msg=None):
            progress_bar.progress(min(float(current) / max(float(total), 1.0), 1.0))
            if msg:
                status_text.text(msg)
            elif current == total:
                status_text.text("Consolidating signals...")
            else:
                status_text.text(f"Scanning: {current} of {total} symbols completed...")

        # removed pre-filter logic to scan all stocks per user request

        settings = {
            'vwma_length': vwma_length,
            'macd_fast': macd_fast,
            'macd_slow': macd_slow,
            'macd_signal': macd_signal
        }
        
        with st.spinner("Processing..."):
            results_df = scanner.scan_market(symbols, active_timeframe, settings, start_date, end_date, show_all, force_refresh_token=force_refresh_token, progress_callback=update_scan_progress)
        
        progress_bar.empty()
        status_text.empty()
        
        st.session_state['vwma_macd_results'] = results_df
        st.session_state['vwma_macd_context'] = {'index': selected_index, 'tf': active_timeframe}

# --- DISPLAY RESULTS ---
if 'vwma_macd_results' in st.session_state and st.session_state['vwma_macd_results'] is not None:
    results_df = st.session_state['vwma_macd_results'].copy()
    context = st.session_state.get('vwma_macd_context', {'index': 'Custom Data', 'tf': '1d'})
    
    if not results_df.empty:
        if not show_all:
            results_df = results_df[results_df['Signal Type'] != "None"]
            
    if not results_df.empty:
        st.success(f"Scan complete! Found {len(results_df)} momentum setup(s).")
        
        # User-friendly styling
        def style_dataframe(df):
            def highlight_signal(val):
                if val == 'Bullish':
                    return 'color: #00FF00; font-weight: bold; background-color: rgba(0, 255, 0, 0.1)'
                elif val == 'Bearish':
                    return 'color: #FF0000; font-weight: bold; background-color: rgba(255, 0, 0, 0.1)'
                return ''
            
            def highlight_trend(val):
                if val == 'Uptrend':
                    return 'color: #00FF00'
                elif val == 'Downtrend':
                    return 'color: #FF0000'
                return ''
                
            return df.style.map(highlight_signal, subset=['Signal Type']).map(highlight_trend, subset=['Trend'])

        styled_df = style_dataframe(results_df)

        st.dataframe(
            styled_df,
            use_container_width=True,
            hide_index=True,
            height=500,
            column_config={
                "Stock": st.column_config.TextColumn("Stock"),
                "LTP": st.column_config.NumberColumn("LTP", format="₹%.2f"),
                "Volume": st.column_config.NumberColumn("Volume 📊", format="%d"),
                "Signal Type": st.column_config.TextColumn("Signal Type 🎯"),
                "VWMA": st.column_config.NumberColumn("VWMA", format="%.2f"),
                "Signal Price": st.column_config.NumberColumn("Signal Price", format="₹%.2f"),
                "Trend": st.column_config.TextColumn("Trend 📈")
            }
        )            
        # Export
        csv = results_df.to_csv(index=False)
        st.download_button(
            label="Download Results as CSV",
            data=csv,
            file_name=f"vwma_macd_scan_{selected_index.replace(' ', '_')}_{active_timeframe}.csv",
            mime="text/csv"
        )
    else:
        if not show_all:
            st.warning(f"No signals found for {selected_index} on {active_timeframe} timeframe within the selected dates.")
        else:
            st.warning(f"No data retrieved for {selected_index}.")

# --- Explanation Section ---
st.markdown("---")
st.subheader("💡 How This Scanner Works")
st.warning("🚨 **IMPORTANT NOTE:** For the MACD + VWMA strategy, utilizing exactly **Signal Price ± ATR** is the mathematically optimal setup. The recommended Stop Loss and Take profit levels are generated using this specific 1:1 Volatility ratio.")

try:
    st.image("/home/mohan/.gemini/antigravity/scratch/dmi_scanner/vwma_macd_strategy.png", use_container_width=True)
except Exception:
    pass
    
st.markdown("""
This scanner identifies **Momentum and Trend-Following** setups based on the **Volume Weighted Moving Average (VWMA) & MACD** strategy. It isolates robust trend continuations by marrying volume density with momentum crossings.

### 🎯 Signal Generation
*   **🟢 Bullish Signal (Buy/Long):** The **MACD Line** crosses *above* the **Signal Line**, AND the closing price of the stock is strictly **above** the **VWMA**. The strategy identifies that heavy buying volume supports the underlying upward momentum crossover.
*   **🔴 Bearish Signal (Sell/Short):** The **MACD Line** crosses *below* the **Signal Line**, AND the closing price of the stock is strictly **below** the **VWMA**. The strategy identifies that heavy selling volume supports the underlying downward momentum crossover.

### 📊 Understanding the Table Columns
*   **Signal:** The entry triggered by the strategy momentum crossover (Bullish=Buy, Bearish=Short).
*   **Trend (EMA 21):** The overall long-term direction of the stock. It is completely independent of the VWMA setup. A VWMA signal that *matches* the Trend direction is considered significantly stronger.
*   **MACD / Signal:** The raw numeric values of the MACD convergence envelope.
*   **VWMA:** The Volume Weighted Moving Average baseline.
*   **Pivot (Best SL/TP):** Uses the recent high/low nearest to the entry point to recommend physical Stop Loss and Take Profit bounds.
*   **EMA 21 SL:** A trailing stop-loss strategy utilizing the 21-period momentum moving average.
*   **Best Method (Signal ± ATR):** The mathematically superior strategy recommending a Stop Loss and Take Profit calculated exactly using `Signal Price ± ATR`.
""")
