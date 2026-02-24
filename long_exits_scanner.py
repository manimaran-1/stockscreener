import pandas as pd
import long_exits_indicators as indicators
import long_exits_data_loader as data_loader
import concurrent.futures
import pytz
import numpy as np

IST = pytz.timezone('Asia/Kolkata')

def scan_symbol_prefetched(symbol, df, settings=None, start_date=None, end_date=None, show_all=False):
    """
    Scans a single symbol for exit signals using pre-fetched DataFrame.
    """
    try:
        if settings is None:
            settings = {}
            
        if df is None or df.empty or len(df) < 50:
            return []
            
        # Extract Settings
        ma_short_len = settings.get('ma_short_len', 9)
        ma_long_len = settings.get('ma_long_len', 21)
            
        # Apply Indicators
        df = indicators.apply_all_indicators(
            df, 
            ma_short_len=ma_short_len,
            ma_long_len=ma_long_len
        )
        
        if df.empty or 'MA_Short' not in df.columns or 'MA_Long' not in df.columns:
             return []
             
        # Create Shifted columns for crossover detection
        df['MA_Short_Prev'] = df['MA_Short'].shift(1)
        df['MA_Long_Prev'] = df['MA_Long'].shift(1)
        df['Close_Prev'] = df['close'].shift(1)
        df['Structure_Prev'] = df['Swing_Low_10'].shift(1)
             
        # Add Signal Logging columns
        df['Signal_Type'] = "None"
        df['Signal'] = 0
        df['Signal_Price'] = 0.0
        
        # 1. Exit (MA Crossover)
        ma_cross_down = (df['MA_Short_Prev'] >= df['MA_Long_Prev']) & (df['MA_Short'] < df['MA_Long'])
        df.loc[ma_cross_down, 'Signal_Type'] = "Exit (MA Cross)"
        df.loc[ma_cross_down, 'Signal'] = -1
        df.loc[ma_cross_down, 'Signal_Price'] = df['close']
        
        # 2. Exit (Price Close Below MA)
        price_below_ma = (df['Close_Prev'] >= df['MA_Long_Prev']) & (df['close'] < df['MA_Long'])
        df.loc[price_below_ma, 'Signal_Type'] = "Exit (Price < MA)"
        df.loc[price_below_ma, 'Signal'] = -1
        df.loc[price_below_ma, 'Signal_Price'] = df['close']
        
        # 3. Exit (Structure Break - Lower Lows)
        structure_break = (df['Close_Prev'] >= df['Structure_Prev']) & (df['close'] < df['Swing_Low_10'])
        df.loc[structure_break, 'Signal_Type'] = "Exit (Structure)"
        df.loc[structure_break, 'Signal'] = -1
        df.loc[structure_break, 'Signal_Price'] = df['close']

        # Since multiple exits trigger at once sometimes, we just take the last overriding one if checking same row.
        # But we don't use 'recent' rolling windows here because an exit should be instantaneous on trigger day.

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
                
                ma_s = row.get('MA_Short', 0)
                ma_l = row.get('MA_Long', 0)
                sl10 = row.get('Swing_Low_10', 0)
                
                results_for_symbol.append({
                    "Stock": symbol,
                    "LTP": round(current_bar['close'], 2),
                    "Signal Time": idx.strftime('%Y-%m-%d %H:%M'),
                    "Signal Type": signal_type_str,
                    "Signal Price": signal_price,
                    "Short MA": round(ma_s, 2),
                    "Long MA": round(ma_l, 2),
                    "Trailing SL": round(sl10, 2),
                    "Trend": row.get('Trend', 'N/A'),
                    "ATR": round(atr_val, 2),
                    "Volume": int(row.get('volume', 0))
                })
        
        if show_all and not results_for_symbol:
            if not pd.isna(current_bar.get('MA_Long')):
                results_for_symbol.append({
                    "Stock": symbol,
                    "LTP": round(current_bar['close'], 2),
                    "Signal Type": "None",
                    "Signal Time": "N/A",
                    "Signal Price": 0.0,
                    "Short MA": round(current_bar.get('MA_Short', 0), 2),
                    "Long MA": round(current_bar.get('MA_Long', 0), 2),
                    "Trailing SL": round(current_bar.get('Swing_Low_10', 0), 2),
                    "Trend": current_bar.get('Trend', 'N/A'),
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
