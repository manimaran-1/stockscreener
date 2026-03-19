import streamlit as st
import pandas as pd
import datetime
import data_loader
import vortex_alligator_scanner as scanner

# --- SECURITY & UI CONFIG ---
hide_st_style = '''
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
.stDeployButton {display:none;}
</style>
'''
st.markdown(hide_st_style, unsafe_allow_html=True)
import hmac


# -----------------------------

st.title("📈 Vortex Indicator & SMA Crossover Scanner")
st.markdown("Scan NSE/BSE markets for trend setups using Vortex confirming Fast/Slow SMA breakouts.")
st.sidebar.header("Scanner Settings")
try: indices = data_loader.INDICES_SLUGS
except Exception: indices = data_loader.get_all_indices_dict()
market_stats = ["Top Gainers", "Top Losers", "Most Active (Value)", "Most Active (Volume)", "52 Week High", "52 Week Low"]
scan_mode = st.sidebar.radio("Mode", ["Full Index Scan", "Pre-Filter (Market Movers)"], index=0, horizontal=True)
index_options = ["Custom List"] + market_stats + list(indices.keys())
selected_index = st.sidebar.selectbox("Select Base Universe", index_options, index=0)
custom_symbols = ""
source_url = "N/A"
if selected_index == "Custom List":
    custom_symbols = st.sidebar.text_area("Enter Stock Symbols (comma separated)", "RELIANCE.NS, TCS.NS")
timeframe_options = ['1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h', '1d', '5d', '1wk', '1mo']
active_timeframe = st.sidebar.selectbox("Interval", timeframe_options, index=8)
st.sidebar.subheader("3. Indicator Settings")
slow_len = st.sidebar.number_input("Slow SMA Length", min_value=1, value=13)
medium_len = st.sidebar.number_input("Medium SMA Length", min_value=1, value=8)
fast_len = st.sidebar.number_input("Fast SMA Length", min_value=1, value=5)
vortex_len = st.sidebar.number_input("Vortex Length", min_value=1, value=14)
show_all = st.sidebar.checkbox("Show All Stocks (Ignore Signal Rules)", value=False)
filter_by_date = st.sidebar.checkbox("Filter by Signal Date", value=True)
start_date, end_date = None, None
if filter_by_date:
    col1, col2 = st.sidebar.columns(2)
    start_date = col1.date_input("Start Date", datetime.date.today() - datetime.timedelta(days=30))
    end_date = col2.date_input("End Date", datetime.date.today())
force_refresh = st.sidebar.checkbox("🔄 Force Refresh Data")
if st.button("Run Vortex SMA Scan", type="primary"):
    symbols = []
    force_refresh_token = datetime.datetime.now().timestamp() if force_refresh else None
    
    if selected_index == "Custom List":
        symbols = [s.strip().upper() for s in custom_symbols.split(',') if s.strip()]
        if not symbols:
             st.warning("Please enter at least one symbol.")
             st.stop()
    elif selected_index in market_stats:
        with st.spinner(f"Fetching market movers: {selected_index}..."):
            df_stats = data_loader.fetch_nifty500_stats()
            symbols = data_loader.get_market_movers(selected_index, df_stats)
            source_url = "TradingView"
    elif selected_index == "Nifty 500":
        with st.spinner("Fetching Nifty 500 symbol list..."):
             symbols, source_url = data_loader.get_nifty500_symbols(return_source=True)
    elif selected_index == "Nifty 200":
        with st.spinner("Fetching Nifty 200 symbol list..."):
             symbols, source_url = data_loader.get_nifty200_symbols(return_source=True)
    else:
        with st.spinner(f"Fetching {selected_index} symbols..."):
             symbols, source_url = data_loader.get_index_constituents(selected_index, return_source=True)
             
    if not symbols:
        st.error("Could not fetch symbols for the selected index.")
    else:
        st.info(f"✅ **Universe Loaded:** {selected_index} — **{len(symbols)} symbols**")
        status_text = st.empty()
        progress_bar = st.progress(0)
        
        def update_scan_progress(current, total, msg=None):
            progress_bar.progress(min(float(current) / max(float(total), 1.0), 1.0))
            status_text.text(msg if msg else f"Scanning: {current} of {total} completed...")
        settings = {'slow': slow_len, 'medium': medium_len, 'fast': fast_len, 'vortex_length': vortex_len}
        
        with st.spinner("Processing..."):
            results_df = scanner.scan_market(symbols, active_timeframe, settings, start_date, end_date, show_all, force_refresh_token, update_scan_progress)
        
        progress_bar.empty()
        status_text.empty()
        st.session_state['res_18_VortexAlligator'] = results_df
if 'res_18_VortexAlligator' in st.session_state and st.session_state['res_18_VortexAlligator'] is not None:
    results_df = st.session_state['res_18_VortexAlligator'].copy()
    if not show_all and not results_df.empty: results_df = results_df[results_df['Signal Type'] != "None"]
    
    if not results_df.empty:
        st.success(f"Scan complete! Found {len(results_df)} setup(s).")
        def style_dataframe(df):
            def highlight_signal(val):
                if val in ['Long Entry']: return 'color: #00FF00; font-weight: bold; background-color: rgba(0, 255, 0, 0.1)'
                return ''
            return df.style.map(highlight_signal, subset=['Signal Type'])
        st.dataframe(style_dataframe(results_df), width="stretch", hide_index=True)
    else:
        st.warning("No signals found.")
st.markdown("---")
st.subheader("💡 Strategy Logic Overview")
try:
    st.image("assets/vortex_alligator_strategy.jpg", width="stretch")
except Exception: pass
st.markdown("""
**Trading with Vortex & SMA Crossover - Long Position**
*   **Entry Rule:** When the Fast SMA (5) crosses above the Medium SMA (8) & Slow SMA (13) lines AND the +VI (Vortex) crosses above the -VI line.
*   **Exit Rule:** When the Fast SMA crosses below the Medium & Slow SMA lines, OR when +VI crosses below -VI line.
""")
