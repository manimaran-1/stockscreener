import streamlit as st
import pandas as pd
import datetime

import adx_sar_data_loader as data_loader
import adx_sar_scanner as scanner

# Page Config
st.set_page_config(page_title="ADX + SAR Scanner", page_icon="📊", layout="wide")

# Header
st.title("📊 ADX + Parabolic SAR Momentum Scanner")
st.markdown("Scan Nifty/BSE markets for ADX and PSAR momentum trades based on Fingrad ADX & Parabolic SAR strategy.")

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

# 3. Settings
st.sidebar.subheader("3. Indicator Settings")
adx_length = st.sidebar.number_input("ADX Length", min_value=1, max_value=200, value=14)
st.sidebar.markdown("---")
psar_af = st.sidebar.number_input("PSAR Initial/Step AF", min_value=0.001, max_value=0.5, value=0.02, step=0.01)
psar_max_af = st.sidebar.number_input("PSAR Max AF", min_value=0.01, max_value=1.0, value=0.2, step=0.01)

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
if st.button("Run ADX/SAR Scan", type="primary"):
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

        settings = {
            'adx_length': adx_length,
            'psar_af': psar_af,
            'psar_max_af': psar_max_af
        }
        
        with st.spinner("Processing..."):
            results_df = scanner.scan_market(symbols, active_timeframe, settings, start_date, end_date, show_all, force_refresh_token=force_refresh_token, progress_callback=update_scan_progress)
        
        progress_bar.empty()
        status_text.empty()

        st.session_state['res_11_ADX_SAR.py'] = results_df
        st.session_state['ctx_11_ADX_SAR.py'] = {'index': selected_index, 'tf': active_timeframe}

if 'res_11_ADX_SAR.py' in st.session_state and st.session_state['res_11_ADX_SAR.py'] is not None:
    results_df = st.session_state['res_11_ADX_SAR.py'].copy()
    context = st.session_state.get('ctx_11_ADX_SAR.py', {'index': 'Custom', 'tf': '1d'})

    if results_df is not None and not results_df.empty:
        if not show_all:
            results_df = results_df[results_df['Signal Type'] != "None"]
            
    if results_df is not None and not results_df.empty:
        st.success(f"Scan complete! Found {len(results_df)} momentum setup(s).")
        
        
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
            file_name=f"adx_sar_scan_{context['index'].replace(' ', '_')}_{context['tf']}.csv",
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
    st.image("/home/mohan/.gemini/antigravity/scratch/dmi_scanner/adx_sar_strategy.jpg", use_container_width=True)
except Exception:
    pass

st.markdown("""
This scanner identifies **Momentum and Trend-Following** setups based on the **ADX Indicator & Parabolic SAR** strategy. 

### 🎯 Signal Generation
*   **🟢 Bullish Signal (Buy/Long):** **SAR Dots** are below the candle, AND **ADX** crosses above 25.
*   **🔴 Bearish Signal (Sell/Short):** **SAR Dots** are above the candle, AND **ADX** crosses above 25.

### 📊 Understanding the Table Columns
*   **Signal:** The entry triggered by the strategy (Bullish=Buy, Bearish=Short).
*   **Trend (EMA 21):** The overall long-term direction of the stock.
*   **ADX:** Average Directional Index value (Strength of trend).
*   **PSAR:** The Parabolic Stop and Reverse value. This should be used as the trailing stop loss.
*   **Best Method (PSAR SL):** Recommends the PSAR as the immediate Stop Loss for the trade.
""")
