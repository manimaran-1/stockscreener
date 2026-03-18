import streamlit as st
import pandas as pd
import reversal_scanner as scanner
import data_loader
import concurrent.futures

# --- SECURITY & UI CONFIG ---
hide_st_style = '''
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}
</style>
'''
st.markdown(hide_st_style, unsafe_allow_html=True)
import hmac


# -----------------------------

st.title("Reversal Detection Pro v3.0 Scanner 🚀")
st.markdown("Scan NSE stocks for non-repainting reversal signals based on V3 logic.")

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

# Put Custom List first, then Stats, then Indices
st.sidebar.subheader("1A. Scan Mode")
scan_mode = st.sidebar.radio("Mode", ["Full Index Scan", "Pre-Filter (Market Movers)"], index=0, help="Full Index computes every stock. Pre-Filter isolates only the highest volume/moving stocks today.", horizontal=True)

# 1B. Base Universe Selection
index_options = ["Custom List"] + market_stats + list(indices.keys())
selected_index = st.sidebar.selectbox("Select Base Universe", index_options, index=0)

# Caching Logic with Session State (to support Progress Bar)
if 'nifty500_stats' not in st.session_state:
    st.session_state['nifty500_stats'] = None
if 'nifty500_fetch_time' not in st.session_state:
    st.session_state['nifty500_fetch_time'] = None

def get_nifty500_stats_with_progress():
    # Check cache validity (Infinite until manual refresh)
    if st.session_state['nifty500_stats'] is not None:
        return st.session_state['nifty500_stats']
    # Cache Miss or Expired: Fetch with Progress
    st.info("Fetching market data (Fresh Load)...")
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    def update_progress(current, total):
        progress = min(float(current) / max(float(total), 1.0), 1.0)
        progress_bar.progress(min(float(progress), 1.0))
        status_text.text(f"Fetching: Batch {current} of {total}...")
    
    df = data_loader.fetch_nifty500_stats(progress_callback=update_progress)
    
    # Update Cache
    st.session_state['nifty500_stats'] = df
    from datetime import datetime
    st.session_state['nifty500_fetch_time'] = datetime.now()
    
    progress_bar.empty()
    status_text.empty()
    return df

# Force Refresh Option - visible if Market Stats selected
if selected_index in market_stats:
    if st.sidebar.button("🔄 Force Fetch New Data", key="refresh_btn_stats_v4"):
        st.session_state['nifty500_stats'] = None
        st.toast("Cache cleared! Fetching new data...", icon="🔄")
        st.rerun()

# 2. Timeframe Selection
timeframes = ["1d", "1wk", "1mo", "1m", "5m", "15m", "30m", "1h"]
selected_timeframe = st.sidebar.selectbox("Select Timeframe", timeframes, index=0)

# Custom Search / List inputs
custom_symbols = []
source_url = "N/A"
if selected_index == "Custom List":
    custom_input = st.sidebar.text_area("Enter symbols (comma separated)", "RELIANCE.NS, ALKEM.NS, INFY.NS")
    if custom_input:
        raw_symbols = [s.strip() for s in custom_input.split(",") if s.strip()]
        custom_symbols = [f"{s}.BO" if s.isdigit() and len(s) == 6 else s for s in raw_symbols]

# Quick Search Override
st.sidebar.markdown("---")
quick_search = st.sidebar.text_input("Quick Search Stock (Overrides Selection)", placeholder="e.g. TATASTEEL.NS")

st.sidebar.markdown("---")
st.sidebar.subheader("Strategy Settings")

# 3. Sensitivity
sensitivity = st.sidebar.select_slider(
    "Sensitivity Preset", 
    options=["Very High", "High", "Medium", "Low", "Very Low", "Custom"],
    value="Medium"
)

custom_settings = {}
if sensitivity == "Custom":
    with st.sidebar.expander("Custom Settings"):
        custom_settings['atr_mult'] = st.number_input("ATR Multiplier", 0.1, 10.0, 2.0)
        custom_settings['pct_threshold'] = st.number_input("Percentage Threshold", 0.001, 1.0, 0.1)
        custom_settings['fixed_reversal'] = st.number_input("Fixed Reversal Amount", 0.0, 100.0, 0.05)
        custom_settings['atr_length'] = st.number_input("ATR Length", 1, 50, 5)
        custom_settings['avg_length'] = st.number_input("Average Length", 1, 50, 5)

# 4. Calculation Method
calc_method = st.sidebar.selectbox("Calculation Method", ["average", "high_low"])
from datetime import datetime, timedelta

# ── Period info lookup ──
_PERIOD_MAP = {
    '1m': ('5 days', 'Minute'),
    '2m': ('1 month', '2-Minute'),
    '5m': ('1 month', '5-Minute'),
    '15m': ('1 month', '15-Minute'),
    '30m': ('1 month', '30-Minute'),
    '60m': ('1 month', '60-Minute'),
    '90m': ('1 month', '90-Minute'),
    '1h': ('1 month', 'Hourly'),
    '1d': ('1 year', 'Daily'),
    '5d': ('1 year', '5-Day'),
    '1wk': ('1 year', 'Weekly'),
    '1mo': ('5 years', 'Monthly'),
    '3mo': ('5 years', 'Quarterly'),
}

# 5. Date Range Filter
st.sidebar.subheader("Filter Settings")
default_start = datetime.now() - timedelta(days=30)
default_end = datetime.now()

start_date = st.sidebar.date_input("Start Date", default_start)
end_date = st.sidebar.date_input("End Date", default_end)

# Run Button
if st.button("Run Scan"):
    symbols = []
    
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
    
        source_url = "TradingView (Live Market Data)"
    elif selected_index == "Nifty 500":
        with st.spinner("Fetching Nifty 500 symbol list..."):
             symbols, source_url = data_loader.get_nifty500_symbols(return_source=True)
    elif selected_index == "Nifty 200":
        with st.spinner("Fetching Nifty 200 symbol list..."):
             symbols, source_url = data_loader.get_nifty200_symbols(return_source=True)
    elif selected_index == "Nifty 50":
        with st.spinner("Fetching Nifty 50 symbol list..."):
             symbols, source_url = data_loader.get_nifty200_symbols(return_source=True)
             symbols = symbols[:50]
    elif selected_index == "Total Market (All Stocks)":
        with st.spinner("Fetching Total Market..."):
             symbols = data_loader.get_index_constituents("Total Market")
             if not symbols: symbols = data_loader.get_nifty500_symbols()
             symbols, source_url = data_loader.get_index_constituents("Total Market", return_source=True)
             if not symbols: 
                 symbols, source_url = data_loader.get_nifty500_symbols(return_source=True)
    else:
        try:
             with st.spinner(f"Fetching {selected_index} symbols..."):
                  symbols, source_url = data_loader.get_index_constituents(selected_index, return_source=True)
        except Exception:
             symbols = []
             source_url = "Error"
             
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
        if not quick_search and selected_index != "Custom List":
             st.error("Could not fetch symbols. Try another index.")
    else:
        # Prepare Settings
        scan_settings = {
            "sensitivity": sensitivity,
            "calculation_method": calc_method,
            "custom_settings": custom_settings if sensitivity == "Custom" else None,
            "start_date": start_date,
            "end_date": end_date
        }
        
        # Progress Bar Logic
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        def update_scan_progress(current, total, msg=None):
            progress = min(float(current) / max(float(total), 1.0), 1.0)
            progress_bar.progress(min(float(progress), 1.0))
            if msg:
                status_text.text(msg)
            else:
                status_text.text(f"Scanning: {current} of {total} symbols completed...")
        
        # Execute Bulk Scan
        with st.spinner("Processing..."):
            results_df = scanner.scan_market(symbols, selected_timeframe, scan_settings, progress_callback=update_scan_progress)
        
        status_text.empty()
        progress_bar.empty()
        
        if results_df is not None and not results_df.empty:
            st.success(f"Found {len(results_df)} signals!")
            
            # Formatting (Updated for v3)
            st.dataframe(
                results_df,
                column_config={
                    "Stock": "Stock",
                    "Reversal Time": st.column_config.DatetimeColumn("Reversal Time", format="D MMM YYYY, HH:mm Z"),
                    "LTP": st.column_config.NumberColumn("LTP", format="₹ %.2f"),
                    "Type": "Signal Type",
                    "Signal Price": st.column_config.NumberColumn("Signal Price", format="₹ %.2f"),
                    "Pivot (SL/TP)": "Pivot (Best SL/TP)",
                    "EMA SL": st.column_config.TextColumn("EMA 21 SL", help="⚠️ When you see ⏳, the price has not crossed the Moving Average yet. Wait for the stock to cross the EMA line before you can mathematically use it as a Trailing Stop Loss!"),
                    "ATR (SL/TP)": "ATR (SL / TP)",
                    "BB+ATR (SL/TP)": "BB+ATR (SL / TP)",
                    "ATR": st.column_config.NumberColumn("ATR (14)", format="%.2f"),
                    "BB Lower": st.column_config.NumberColumn("BB Lower", format="₹ %.2f"),
                    "BB Upper": st.column_config.NumberColumn("BB Upper", format="₹ %.2f"),
                    "Trend": "Trend",
                    "EMA9": st.column_config.NumberColumn("EMA 9", format="%.2f"),
                    "EMA21": st.column_config.NumberColumn("EMA 21", format="%.2f"),
                    "Volume": st.column_config.NumberColumn("Volume", format="%d"),
                },
                hide_index=True,
                width="stretch" # width="stretch" corresponds to use_container_width=True in newer versions optionally
            )
            
            # Download
            csv = results_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "Download CSV",
                csv,
                "reversal_scan_results.csv",
                "text/csv",
                key='download-csv'
            )
        else:
            st.warning("No signals found matching criteria.")
            
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
