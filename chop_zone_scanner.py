import pandas as pd
import chop_zone_indicators as indicators
import chop_zone_data_loader as data_loader
import concurrent.futures
import pytz
import numpy as np

IST = pytz.timezone('Asia/Kolkata')

def scan_symbol_prefetched(symbol, df, settings=None, start_date=None, end_date=None, show_all=False):
    """
    Scans a single symbol for Chop Zone color.
    """
    try:
        if settings is None:
            settings = {}
            
        if df is None or df.empty or len(df) < 50:
            return []
            
        # Extract Settings
        chop_length = settings.get('chop_length', 14)
        target_color = settings.get('target_color', 'All')
            
        # Apply Indicators
        df = indicators.apply_all_indicators(df, chop_length=chop_length)
        
        if df.empty or 'Chop' not in df.columns:
             return []
             
        # Add Signal Logging columns
        df['Signal_Type'] = "None"
        df['Signal'] = 0
        df['Signal_Price'] = 0.0
        
        # Map colors for every bar so we can filter based on historical dates too
        df['Chop_Color'] = df['Chop'].apply(indicators.get_chop_color)
        
        # A signal occurs if it matches the target color, or if target is All
        if target_color == "All":
             # All rows are "signals" in terms of capturing their color
             df['Signal_Type'] = df['Chop_Color']
             df['Signal'] = 1
        else:
             cond = df['Chop_Color'].str.contains(target_color)
             df.loc[cond, 'Signal_Type'] = df.loc[cond, 'Chop_Color']
             df.loc[cond, 'Signal'] = 1
        
        df['Signal_Price'] = df['close']

        current_bar = df.iloc[-1]
        
        is_live_scan = False
        if start_date is None and end_date is None:
            is_live_scan = True
            
        filtered_df = df.copy()
        if is_live_scan:
             filtered_df = filtered_df.iloc[[-1]]
        elif start_date and end_date:
            try:
                from datetime import datetime, time
                s_dt = IST.localize(datetime.combine(start_date, time.min))
                e_dt = IST.localize(datetime.combine(end_date, time.max))
                filtered_df = filtered_df[(filtered_df.index >= s_dt) & (filtered_df.index <= e_dt)]
            except Exception as e:
                pass

        if filtered_df.empty:
             return []
             
        signal_rows = filtered_df[filtered_df['Signal'] != 0]
        results_for_symbol = []
        
        if not signal_rows.empty:
            for idx, row in signal_rows.iterrows():
                signal_price = row['Signal_Price']
                signal_type_str = row['Signal_Type']
                atr_val = row.get('ATR', np.nan)
                
                if pd.isna(atr_val): atr_val = 0
                
                chop_val = row.get('Chop', 50)
                ema21 = row.get('EMA21', np.nan)
                if pd.isna(ema21): ema21 = 0
                
                results_for_symbol.append({
                    "Stock": symbol,
                    "LTP": round(current_bar['close'], 2),
                    "Signal Time": idx.strftime('%Y-%m-%d %H:%M'),
                    "Signal Type": signal_type_str,
                    "Signal Price": signal_price,
                    "Chop Zone": round(chop_val, 2),
                    "Trend": row.get('Trend', 'N/A'),
                    "EMA 21": round(ema21, 2),
                    "ATR": round(atr_val, 2),
                    "Volume": int(row.get('volume', 0))
                })
        
        if show_all and not results_for_symbol:
            if not pd.isna(current_bar.get('Chop')):
                chop_state = indicators.get_chop_color(current_bar.get('Chop', 50))
                results_for_symbol.append({
                    "Stock": symbol,
                    "LTP": round(current_bar['close'], 2),
                    "Signal Type": "None",
                    "Signal Time": "N/A",
                    "Signal Price": 0.0,
                    "Chop Zone": round(current_bar.get('Chop', 50), 2),
                    "Trend": current_bar.get('Trend', 'N/A'),
                    "EMA 21": round(current_bar.get('EMA21', 0), 2),
                    "ATR": round(current_bar.get('ATR', 0), 2),
                    "Volume": int(current_bar.get('volume', 0))
                })

        return results_for_symbol
    except Exception as e:
        return []

def scan_market(symbols, interval='1d', settings=None, start_date=None, end_date=None, show_all=False, force_refresh_token=None, progress_callback=None):
    results = []
    if settings is None:
        settings = {}
    
    bulk_data_dict = data_loader.fetch_bulk_data(symbols, interval=interval, force_refresh_token=force_refresh_token)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_symbol = {
            executor.submit(scan_symbol_prefetched, sym, bulk_data_dict.get(sym), settings, start_date, end_date, show_all): sym 
            for sym in symbols
        }
        
        completed = 0
        total = len(symbols)
        for future in concurrent.futures.as_completed(future_to_symbol):
            res_list = future.result()
            if res_list:
                results.extend(res_list)
            completed += 1
            if progress_callback:
                progress_callback(completed, total, f"Analyzing {completed}/{total} symbols...")
                
    return pd.DataFrame(results)
