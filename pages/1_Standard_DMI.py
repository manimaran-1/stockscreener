import streamlit as st
import pandas as pd
import scanner
import data_loader
import concurrent.futures
from datetime import datetime

st.set_page_config(page_title="DMI Scanner Dashboard", layout="wide")

st.title("Directional Movement Index (DMI) Scanner 🧭")
st.markdown("Scan NSE stocks for DMI Crossovers (+DI / -DI) with recent activity indicators, S/R levels, and multi-timeframe support.")

# Sidebar
st.sidebar.header("Configuration")

# 1. Universe Selection
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
    if st.sidebar.button("🔄 Force Fetch New Data", key="refresh_btn_dmi"):
        st.session_state['nifty500_stats'] = None
        st.toast("Cache cleared! Fetching new data...", icon="🔄")
        st.rerun()

# Custom List Inputs
custom_symbols = []
if selected_index == "Custom List":
    custom_input = st.sidebar.text_area("Enter symbols (comma separated)", "RELIANCE.NS, TCS.NS, INFY.NS")
    if custom_input:
        raw_symbols = [s.strip() for s in custom_input.split(",") if s.strip()]
        custom_symbols = [f"{s}.BO" if s.isdigit() and len(s) == 6 else s for s in raw_symbols]

# 2. Timeframe Selection
st.sidebar.markdown("---")
st.sidebar.subheader("Select Timeframe")

# Predefined common timeframes supported by yfinance
timeframes = ["1m", "5m", "15m", "30m", "1h", "1d", "1wk", "1mo"]
selected_timeframe = st.sidebar.selectbox("Standard Timeframe", timeframes, index=timeframes.index("1d"))

use_custom_timeframe = st.sidebar.checkbox("Use Custom Timeframe")
if use_custom_timeframe:
    custom_tf_input = st.sidebar.text_input("Custom Timeframe (e.g., '90m', '2h')", value="90m")
    active_timeframe = custom_tf_input
else:
    active_timeframe = selected_timeframe

# Date Range Filter
st.sidebar.markdown("---")
st.sidebar.subheader("Filter Settings")
default_start = datetime.now() - pd.Timedelta(days=30) # default to last 30 days
default_end = datetime.now()

start_date = st.sidebar.date_input("Start Date", default_start)
end_date = st.sidebar.date_input("End Date", default_end)

# 3. Strategy Configuration
st.sidebar.markdown("---")
st.sidebar.subheader("DMI Settings")

dmi_length = st.sidebar.number_input("DMI/ADX Length", min_value=1, max_value=50, value=14)
show_all = st.sidebar.checkbox("Show All Stocks (Not just crossovers)", value=False)

st.sidebar.markdown("---")
force_refresh = st.sidebar.checkbox("🔄 Force Refresh Data", help="Check this to clear the cache and download fresh live data from Yahoo Finance. Otherwise, data gets cached for 30 minutes to allow extremely fast timeframe switching.")

if st.button("Run Scan"):
    symbols = []
    
    # Generate a cache busting token if checked
    from datetime import datetime
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

        # We delegate to scanner.scan_market which handles the massive bulk data fetch optimization automatically
        with st.spinner("Processing..."):
            results_df = scanner.scan_market(symbols, active_timeframe, start_date, end_date, show_all, force_refresh_token=force_refresh_token, progress_callback=update_scan_progress)
        
        progress_bar.empty()
        status_text.empty()

        st.session_state['res_1_Standard_DMI.py'] = results_df
        st.session_state['ctx_1_Standard_DMI.py'] = {'index': selected_index, 'tf': active_timeframe}

if 'res_1_Standard_DMI.py' in st.session_state and st.session_state['res_1_Standard_DMI.py'] is not None:
    results_df = st.session_state['res_1_Standard_DMI.py'].copy()
    context = st.session_state.get('ctx_1_Standard_DMI.py', {'index': 'Custom', 'tf': '1d'})

    if results_df is not None and not results_df.empty:
        if not show_all:
            results_df = results_df[results_df['Signal Type'] != "None"]
            
    if results_df is not None and not results_df.empty:
        st.success(f"Scan complete! Found {len(results_df)} stocks.")
        
        # Format UI rendering
        def format_signal(val):
            color = "green" if val == "Buy" else "red" if val == "Sell" else "gray"
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

        
        # CSV Download
        csv = results_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            "Download Results (CSV)",
            csv,
            "dmi_scan_results.csv",
            "text/csv",
            key='download-csv'
        )
    else:
        if show_all:
            st.warning("No data found for the selected date range. Ensure the market was open or expand the dates.")
        else:
            st.warning(f"0 crossovers found for {context['tf']} timeframe between {start_date} and {end_date}. Try expanding the dates or checking 'Show All Stocks'.")


st.markdown("---")
with st.expander("📚 Detailed Stop Loss & Take Profit Calculations"):
    st.markdown("""
    **Understanding your Risk Columns & Mathematical Calculations:**
    
    ⚙️ **Pivot (Best SL/TP):** The most reliable structural stop loss based on recent price extremes.
    * **Buy Calculation:** `SL = Low of Signal Candle` | `TP = Entry + ((Entry - SL) * 2)`
    * **Sell Calculation:** `SL = High of Signal Candle` | `TP = Entry - ((SL - Entry) * 2)`
    
    📈 **EMA SL:** A dynamic 'Trailing Stop' based on the 21-period Exponential Moving Average. 
    * **Calculation:** Follows the exact `EMA 21` value on the selected timeframe. 
    * *Note:* If you see a **⏳** symbol, it mathematically means the price hasn't actually crossed the moving average yet. Wait for the cross before using it as a trailing stop.
        
    📊 **ATR (SL/TP):** A volatility-based stop loss reacting to market noise.
    * **Buy Calculation:** `SL = Entry - ATR(14)` | `TP = Entry + (ATR(14) * 2)`
    * **Sell Calculation:** `SL = Entry + ATR(14)` | `TP = Entry - (ATR(14) * 2)`
        
    🛡️ **BB+ATR (SL/TP):** An extreme-volatility "safe" stop outside the standard distribution curve.
    * **Buy Calculation:** `SL = BB Lower Band(20, 2) - ATR(14)` | `TP = BB Upper Band(20, 2) + ATR(14)`
    * **Sell Calculation:** `SL = BB Upper Band(20, 2) + ATR(14)` | `TP = BB Lower Band(20, 2) - ATR(14)`
    
    *ℹ️ All values dynamically recalculate and adapt to whatever timeframe you select in the sidebar!*
    """)
