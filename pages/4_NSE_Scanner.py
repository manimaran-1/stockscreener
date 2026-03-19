import streamlit as st
import pandas as pd
import nse_scanner as scanner
import data_loader
import pytz
from datetime import datetime

# --- SECURITY & UI CONFIG ---
hide_st_style = '''
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
    .stDeployButton, [data-testid="stAppDeployButton"] {display: none !important;}
    .stGithubButton, [data-testid="stToolbarActionButton"] {display: none !important;}
</style>
'''
st.markdown(hide_st_style, unsafe_allow_html=True)
import hmac


# -----------------------------

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
st.title("Main NSE Scanner 📊")
st.markdown("Filter stocks based on custom EMA, Stoch RSI, SMI, and MACD criteria.")
# Sidebar Controls
st.sidebar.header("Configuration")
# Universe Selection
# aggregated list of index names
indices_dict = data_loader.get_all_indices_dict()
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
universe_options = ["Custom List"] + market_stats + list(indices_dict.keys())
selected_universe = st.sidebar.selectbox("Select Stock Universe", universe_options, index=0)
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
if selected_universe in market_stats:
    import uuid
    if st.sidebar.button("🔄 Force Fetch New Data", key=f"refresh_btn_{uuid.uuid4().hex[:8]}"):
        st.session_state['nifty500_stats'] = None
        st.toast("Cache cleared! Fetching new data...", icon="🔄")
        st.rerun()
# Timeframe Selection
# Expanded list: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo
timeframe_options = ["1d", "1wk", "1mo", "1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h"]
selected_timeframe = st.sidebar.selectbox("Select Timeframe", timeframe_options)
st.sidebar.markdown("---")
st.sidebar.info("**Timezone**: IST (Asia/Kolkata)")
st.sidebar.info("**Data Source**: Yahoo Finance (IST Optimized)")
st.sidebar.info("**Note**: Intraday scans show all signals from today.")
# Custom List Inputs
custom_input = ""
if selected_universe == "Custom List":
    custom_input = st.sidebar.text_area("Enter symbols (comma separated)", "RELIANCE.NS, ALKEM.NS, INFY.NS")
# Load Symbols based on selection
symbols = []
# Fetch Base Universe Symbols
source_url = "N/A"
if selected_universe == "Custom List":
    source_url = "User Input"
    raw_symbols = [s.strip() for s in custom_input.split(",") if s.strip()]
    symbols = [f"{s}.BO" if s.isdigit() and len(s) == 6 else s.upper() for s in raw_symbols]
    if not symbols:
         st.warning("Please enter at least one symbol.")
         st.stop()
elif selected_universe in market_stats:
    source_url = "TradingView (Live Market Data)"
    with st.spinner(f"Fetching market movers: {selected_universe}..."):
        df_stats = get_nifty500_stats_with_progress()
        if df_stats is not None and not df_stats.empty:
             symbols = data_loader.get_market_movers(selected_universe, df_stats)
        else:
             symbols = []
elif selected_universe == "Nifty 500":
    with st.spinner("Fetching Nifty 500 symbol list..."):
         symbols, source_url = data_loader.get_nifty500_symbols(return_source=True)
elif selected_universe == "Nifty 200":
    with st.spinner("Fetching Nifty 200 symbol list..."):
         symbols, source_url = data_loader.get_nifty200_symbols(return_source=True)
elif selected_universe == "Nifty 50":
    with st.spinner("Fetching Nifty 50 symbol list..."):
         symbols, source_url = data_loader.get_nifty200_symbols(return_source=True)
         symbols = symbols[:50]
elif selected_universe == "Total Market (All Stocks)":
    with st.spinner("Fetching Total Market..."):
         symbols, source_url = data_loader.get_index_constituents("Total Market", return_source=True)
         if not symbols: 
             symbols, source_url = data_loader.get_nifty500_symbols(return_source=True)
else:
    try:
         with st.spinner(f"Fetching {selected_universe} symbols..."):
              symbols, source_url = data_loader.get_index_constituents(selected_universe, return_source=True)
    except Exception:
         symbols = []
         source_url = "Error"
         
# Automatically apply Market Mover intersection if 'Pre-Filter' mode is active AND a generic index was selected
if scan_mode == "Pre-Filter (Market Movers)" and symbols and selected_universe not in market_stats and selected_universe != "Custom List":
    with st.spinner(f"Applying auto-Market Movers Pre-Filter to {selected_universe}..."):
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
                  st.warning(f"No stocks in '{selected_universe}' qualified as top market movers today.")
                  st.stop()
st.write(f"**Universe:** {selected_universe} ({len(symbols)} symbols)")
st.write(f"**Timeframe:** {selected_timeframe}")
st.info(f"✅ **Universe Loaded:** {selected_universe} ({len(symbols)} symbols dynamically fetched from [{source_url}]({source_url}))")
# Run Scan Button
if st.button("Run Scanner"):
    if not symbols:
        st.error("No symbols selected.")
    else:
        st.write(f"Scanning {len(symbols)} stocks... This may take a while.")
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        def update_scan_progress(current, total, msg=None):
            progress = min(float(current) / max(float(total), 1.0), 1.0)
            progress_bar.progress(min(float(progress), 1.0))
            if msg:
                status_text.text(msg)
            else:
                status_text.text(f"Scanning: {current} of {total} symbols completed...")
        
        with st.spinner("Processing..."):
            results_df = scanner.scan_market(symbols, interval=selected_timeframe, progress_callback=update_scan_progress)
        
        status_text.empty()
        progress_bar.empty()
        
        if not results_df.empty:
            st.success(f"Found {len(results_df)} signal(s)!")
            
            # Sort by Signal Time descending
            results_df = results_df.sort_values(by='Signal Time', ascending=False)
            
            # Display Main Table
            st.dataframe(
                results_df,
                column_config={
                    "Stock Name": "Stock",
                    "LTP": st.column_config.NumberColumn("LTP", format="₹ %.2f"),
                    "Signal Time": "Time (IST)",
                    "Signal Price": st.column_config.NumberColumn("Signal Price", format="₹ %.2f"),
                    "Pivot (Best SL/TP)": "Pivot (Best SL/TP)",
                    "EMA SL": st.column_config.TextColumn("EMA 21 SL", help="⚠️ When you see ⏳, the price has not crossed the Moving Average yet. Wait for the stock to cross the EMA line before you can mathematically use it as a Trailing Stop Loss!"),
                    "ATR (SL/TP)": "ATR (SL / TP)",
                    "BB+ATR (SL/TP)": "BB+ATR (SL / TP)",
                    "ATR": st.column_config.NumberColumn("ATR (14)", format="%.2f"),
                    "BB Lower": st.column_config.NumberColumn("BB Lower", format="₹ %.2f"),
                    "BB Upper": st.column_config.NumberColumn("BB Upper", format="₹ %.2f"),
                    "Volume": st.column_config.NumberColumn("Volume", format="%d"),
                    "EMA5": st.column_config.NumberColumn("EMA 5", format="%.2f"),
                    "EMA9": st.column_config.NumberColumn("EMA 9", format="%.2f"),
                    "EMA21": st.column_config.NumberColumn("EMA 21", format="%.2f"),
                    "Stoch RSI K": st.column_config.NumberColumn("Stoch RSI K", format="%.2f"),
                    "SMI": st.column_config.NumberColumn("SMI", format="%.2f"),
                    "MACD": st.column_config.NumberColumn("MACD", format="%.2f"),
                },
                hide_index=True,
                width="stretch"
            )
            
            # Download option
            csv = results_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "Download CSV",
                csv,
                f"scan_results_{selected_universe}_{selected_timeframe}.csv",
                "text/csv",
                key='download-csv'
            )
        else:
            st.warning("No stocks matched the criteria/timeframe conditions.")
with st.expander("View Logic Details"):
    st.markdown("""
    **Buy Conditions:**
    1. **EMA (Short)**: Price > EMA 5
    2. **EMA (Mid)**: Price > EMA 9
    3. **EMA (Long)**: Price > EMA 21
    4. **Stoch RSI K**: K (14,14,3,3) > 70
    5. **SMI**: SMI (10,3) > 30
    6. **MACD**: MACD Line (12,26,9) > 0.75
    """)
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