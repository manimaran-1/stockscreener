import pandas as pd
import obv_supertrend_indicators as indicators
import obv_supertrend_data_loader as data_loader
import concurrent.futures
import pytz
import numpy as np

IST = pytz.timezone('Asia/Kolkata')

def scan_symbol_prefetched(symbol, df, settings=None, start_date=None, end_date=None, show_all=False):
    """
    Scans a single symbol for OBV + Supertrend momentum signals using pre-fetched DataFrame.
    """
    try:
        if settings is None:
            settings = {}
            
        if df is None or df.empty or len(df) < 50:
            return []
            
        # Extract Settings
        st_length = settings.get('supertrend_length', 10)
        st_multiplier = settings.get('supertrend_multiplier', 3.0)
            
        # Apply Indicators
        df = indicators.apply_all_indicators(
            df, 
            supertrend_length=st_length,
            supertrend_multiplier=st_multiplier
        )
        
        if df.empty or 'Supertrend_Direction' not in df.columns:
             return []
             
        # Create Shifted columns for crossover detection
        df['ST_Dir_Prev'] = df['Supertrend_Direction'].shift(1)
        df['OBV_Prev'] = df['OBV'].shift(1)
             
        # Add Signal Logging columns
        df['Signal_Type'] = "None"
        df['Signal'] = 0
        df['Signal_Price'] = 0.0
        
        # Bullish Entry: Supertrend Turns Green AND OBV is rising
        df['ST_Turns_Green'] = (df['ST_Dir_Prev'] < 0) & (df['Supertrend_Direction'] > 0)
        st_green_recent = df['ST_Turns_Green'].rolling(window=3).max().fillna(0).astype(bool)
        obv_rising = df['OBV'] > df['OBV_Prev']
        
        bullish_cond = st_green_recent & obv_rising
        df.loc[bullish_cond, 'Signal_Type'] = "Bullish"
        df.loc[bullish_cond, 'Signal'] = 1
        df.loc[bullish_cond, 'Signal_Price'] = df['close']
        
        # Bearish Entry: Supertrend Turns Red AND OBV is falling
        df['ST_Turns_Red'] = (df['ST_Dir_Prev'] > 0) & (df['Supertrend_Direction'] < 0)
        st_red_recent = df['ST_Turns_Red'].rolling(window=3).max().fillna(0).astype(bool)
        obv_falling = df['OBV'] < df['OBV_Prev']
        
        bearish_cond = st_red_recent & obv_falling
        df.loc[bearish_cond, 'Signal_Type'] = "Bearish"
        df.loc[bearish_cond, 'Signal'] = -1
        df.loc[bearish_cond, 'Signal_Price'] = df['close']

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
                
                # Default 0 for nan ATR
                if pd.isna(atr_val): atr_val = 0
                
                obv_val = row.get('OBV', 0)
                st_val = row.get('Supertrend', 0)
                pivot_high = row.get('high', signal_price)
                pivot_low = row.get('low', signal_price)
                ema21 = row.get('EMA21', np.nan)
                if pd.isna(ema21): ema21 = 0
                
                # SL/TP Logic Estimation
                if signal_type_str == "Bullish":
                    best_sl = signal_price - atr_val
                    best_tp = signal_price + atr_val
                    pivot_sl = pivot_low
                    pivot_tp = signal_price + max(signal_price - pivot_sl, 0.01) * 2
                    ema_sl_str = f"₹{round(ema21, 2)}" if ema21 < signal_price and ema21 != 0 else f"₹{round(ema21, 2)} ⏳"
                else:
                    best_sl = signal_price + atr_val
                    best_tp = signal_price - atr_val
                    pivot_sl = pivot_high
                    pivot_tp = signal_price - max(pivot_sl - signal_price, 0.01) * 2
                    ema_sl_str = f"₹{round(ema21, 2)}" if ema21 > signal_price and ema21 != 0 else f"₹{round(ema21, 2)} ⏳"
                    
                results_for_symbol.append({
                    "Stock": symbol,
                    "LTP": round(current_bar['close'], 2),
                    "Signal Time": idx.strftime('%Y-%m-%d %H:%M'),
                    "Signal Type": signal_type_str,
                    "Signal Price": signal_price,
                    "OBV": int(obv_val),
                    "Supertrend": round(st_val, 2),
                    "Trend": row.get('Trend', 'N/A'),
                    "Pivot (Best SL/TP)": f"₹{round(pivot_sl, 2)} / ₹{round(pivot_tp, 2)}",
                    "EMA SL": ema_sl_str,
                    "Best Method (Signal ± ATR)": f"₹{round(best_sl, 2)} / ₹{round(best_tp, 2)}",
                    "ATR": round(atr_val, 2),
                    "Volume": int(row.get('volume', 0))
                })
        
        if show_all and not results_for_symbol:
            if not pd.isna(current_bar.get('Supertrend')):
                results_for_symbol.append({
                    "Stock": symbol,
                    "LTP": round(current_bar['close'], 2),
                    "Signal Type": "None",
                    "Signal Time": "N/A",
                    "Signal Price": 0.0,
                    "OBV": int(current_bar.get('OBV', 0)),
                    "Supertrend": round(current_bar.get('Supertrend', 0), 2),
                    "Trend": current_bar.get('Trend', 'N/A'),
                    "Pivot (Best SL/TP)": "N/A",
                    "EMA SL": "N/A",
                    "Best Method (Signal ± ATR)": "N/A",
                    "ATR": round(current_bar.get('ATR', 0), 2),
                    "Volume": int(current_bar.get('volume', 0))
                })

        return results_for_symbol
    except Exception as e:
        return []

def scan_market(symbols, interval='1d', settings=None, start_date=None, end_date=None, show_all=False, force_refresh_token=None, progress_callback=None):
    """
    Parallel bulk scan of a list of symbols using pre-fetched block data.
    """
    results = []
    
    if settings is None:
        settings = {}
    
    # Pre-fetch all data simultaneously using chunks and progress bar
    bulk_data_dict = data_loader.fetch_bulk_data(symbols, interval=interval, force_refresh_token=force_refresh_token)
    
    # Calculate indicators using CPU threads since data is already loaded
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
