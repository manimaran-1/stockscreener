import streamlit as st
import pandas as pd
import scanner
import importlib
importlib.reload(scanner)
import data_loader
import concurrent.futures
from datetime import datetime

st.set_page_config(page_title="Arbitrage Scanner Dashboard", layout="wide")

st.title("NSE/BSE Arbitrage Scanner ⚖️")
st.markdown("Scan stocks to capture live prices on both NSE and BSE. Evaluates the percentage difference to find arbitrage trading opportunities.")

# Sidebar
st.sidebar.header("Configuration")

# 1. Universe Selection
indices = data_loader.get_all_indices_dict()

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

# Build the combined unified list
index_options = ["Total Market (All Stocks)", "Custom List"] + list(indices.keys())

# Let the user pick from the massive combined list regardless of mode, but handle the mode logically below
selected_index = st.sidebar.selectbox("Select Base Universe", index_options, index=0)

# 1C. Pre-Filter Settings (Conditional)
if scan_mode == "Pre-Filter (Market Movers)":
    st.sidebar.markdown("---")
    st.sidebar.subheader("Pre-Filter Settings")
    selected_filter = st.sidebar.selectbox("Filter By", market_stats, index=0)
else:
    selected_filter = None

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
    if st.sidebar.button("🔄 Force Fetch New Data", key="refresh_btn_arb"):
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

timeframes = ["1m", "5m", "15m", "30m", "1h", "1d"]
selected_timeframe = st.sidebar.selectbox("Standard Timeframe", timeframes, index=timeframes.index("1m"))

st.sidebar.markdown("---")
assumed_capital = st.sidebar.number_input("Assumed Capital (₹) for Calc", min_value=1000, value=100000, step=10000)
min_diff = st.sidebar.number_input("Minimum Difference (%) to Show", min_value=0.0, max_value=100.0, value=0.05, step=0.01, format="%.2f")

if st.button("Run Arbitrage Scan"):
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
        # Strip extension for Arbitrage specifically
        base_symbols = [s.split('.')[0] if '.' in s else s for s in symbols]
        
        # Deduplicate
        base_symbols = list(set(base_symbols))
        
        total_symbols = len(base_symbols)
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        def update_progress(current, total, msg):
            # Calculate percentage, maxing at 1.0 (100%)
            pct = current / total if total > 0 else 0
            pct = min(1.0, pct)
            progress_bar.progress(pct)
            status_text.text(msg)
            
        st.info(f"Scanning {total_symbols} stocks using real-time synchronized broker APIs...")
        
        # New Blazing Fast TradingView API Method with Loading Animation
        results_df = scanner.scan_market_arbitrage(base_symbols, selected_timeframe, min_diff=min_diff, progress_callback=update_progress)
        
        progress_bar.empty()
        status_text.empty()
        
        if results_df is not None and not results_df.empty:
            # Filter by min_diff
            results_df = results_df[results_df["Diff (%)"] >= min_diff]
        # (Data is already a DF, just handling logical check)
        if results_df is not None and not results_df.empty:
            results_df = results_df.sort_values(by="Diff (%)", ascending=False)
            
            # Technical Arbitrage Math
            results_df['Buy Price'] = results_df[['NSE Price', 'BSE Price']].min(axis=1)
            results_df['Est. Qty'] = (assumed_capital // results_df['Buy Price']).astype(int)
            # Cannot buy if quantity is 0
            results_df = results_df[results_df['Est. Qty'] > 0]
            
            results_df['Gross Profit (₹)'] = results_df['Est. Qty'] * results_df['Diff (₹)']
            results_df['Investment (₹)'] = results_df['Est. Qty'] * results_df['Buy Price']
            
            st.success(f"Scan complete! Found {len(results_df)} stocks with >= {min_diff}% difference.")
            
            def format_exchange(val):
                if val == "NSE": return "color: green; font-weight: bold"
                elif val == "BSE": return "color: blue; font-weight: bold"
                else: return "color: gray"
                
            def format_circuit(val):
                if val == "Yes": return "color: white; background-color: red; font-weight: bold"
                return ""
            
            # Show Table
            styled_df = results_df.style.map(format_exchange, subset=['Higher Exchange'])
            styled_df = styled_df.map(format_circuit, subset=['NSE Circuit', 'BSE Circuit'])
            
            st.dataframe(
                styled_df,
                column_config={
                    "Stock": "Stock",
                    "Date": "Date",
                    "Time": "Time",
                    "NSE Circuit": "NSE Circuit",
                    "BSE Circuit": "BSE Circuit",
                    "NSE Price": st.column_config.NumberColumn("NSE Price", format="₹ %.2f"),
                    "BSE Price": st.column_config.NumberColumn("BSE Price", format="₹ %.2f"),
                    "Higher Exchange": "Higher Exchange",
                    "Diff (₹)": st.column_config.NumberColumn("Diff (₹)", format="₹ %.2f"),
                    "Diff (%)": st.column_config.NumberColumn("Difference %", format="%.2f%%"),
                    "Est. Qty": st.column_config.NumberColumn("Est. Qty", format="%d"),
                    "Investment (₹)": st.column_config.NumberColumn("Investment (₹)", format="₹ %d"),
                    "Gross Profit (₹)": st.column_config.NumberColumn("Gross Profit (₹)", format="₹ %.2f"),
                    "NSE Volume": st.column_config.NumberColumn("NSE Volume", format="%d"),
                    "BSE Volume": st.column_config.NumberColumn("BSE Volume", format="%d"),
                    "Buy Price": None # Hide intermediate column
                },
                use_container_width=True,
                hide_index=True
            )
            
            # CSV Download
            csv = results_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "Download Results (CSV)",
                csv,
                "arbitrage_scan_results.csv",
                "text/csv",
                key='download-csv-arb'
            )
        else:
            st.warning(f"No stocks found with >= {min_diff}% difference. Try lowering the threshold or checking market hours.")
    else:
        st.error("Could not fetch symbols to scan. Check index.")