import streamlit as st
import pandas as pd
import datetime
import data_loader
import pivot_stochrsi_scanner as scanner
st.set_page_config(page_title="Pivot Points + StochRSI Scanner", page_icon="📊", layout="wide")

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

_PERIOD_MAP = {
    '1m': ('5 days', 'Minute'), '2m': ('1 month', '2-Minute'), '5m': ('1 month', '5-Minute'),
    '15m': ('1 month', '15-Minute'), '30m': ('1 month', '30-Minute'), '60m': ('1 month', '60-Minute'),
    '90m': ('1 month', '90-Minute'), '1h': ('1 month', 'Hourly'), '1d': ('1 year', 'Daily'),
    '5d': ('1 year', '5-Day'), '1wk': ('1 year', 'Weekly'), '1mo': ('5 years', 'Monthly'),
    '3mo': ('5 years', 'Quarterly'),
}
st.title("📈 Pivot Points & Stochastic RSI Scanner")
st.markdown("Scan NSE/BSE markets for Pullback momentum trades using Pivot R1 Breakouts confirmed by Stochastic RSI crossovers.")
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
rsi_len = st.sidebar.number_input("RSI Length", min_value=1, max_value=100, value=14)
stoch_len = st.sidebar.number_input("Stochastic Length", min_value=1, max_value=100, value=14)
k_len = st.sidebar.number_input("%K Length", min_value=1, max_value=100, value=3)
d_len = st.sidebar.number_input("%D Length", min_value=1, max_value=100, value=3)
show_all = st.sidebar.checkbox("Show All Stocks (Ignore Signal Rules)", value=False)
filter_by_date = st.sidebar.checkbox("Filter by Signal Date", value=True)
start_date, end_date = None, None
if filter_by_date:
    col1, col2 = st.sidebar.columns(2)
    start_date = col1.date_input("Start Date", datetime.date.today() - datetime.timedelta(days=30))
    end_date = col2.date_input("End Date", datetime.date.today())
force_refresh = st.sidebar.checkbox("🔄 Force Refresh Data")
if st.button("Run Pivot StochRSI Scan", type="primary"):
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
        settings = {'rsi_length': rsi_len, 'stoch_length': stoch_len, 'k': k_len, 'd': d_len}
        
        with st.spinner("Processing..."):
            results_df = scanner.scan_market(symbols, active_timeframe, settings, start_date, end_date, show_all, force_refresh_token, update_scan_progress)
        
        progress_bar.empty()
        status_text.empty()
        st.session_state['res_16_PivotStochRSI'] = results_df
if 'res_16_PivotStochRSI' in st.session_state and st.session_state['res_16_PivotStochRSI'] is not None:
    results_df = st.session_state['res_16_PivotStochRSI'].copy()
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
    st.image("assets/pivot_stochrsi_strategy.jpg", width="stretch")
except Exception: pass
st.markdown("""
**Trading with Pivot Points & Stochastic RSI - Long Position**
*   **Entry Rule:** When the exact price closes *above Resistance 1 (R1)* AND the `%K` crosses *above* `%D` line on Stochastic RSI.
*   **Exit Rule:** When `%K` crosses *below* `%D` line.
""")
