import pandas as pd
import sma_rsi_indicators as indicators
import sma_rsi_data_loader as data_loader
import concurrent.futures
import pytz
import numpy as np

IST = pytz.timezone('Asia/Kolkata')

def scan_symbol_prefetched(symbol, df, settings=None, start_date=None, end_date=None, show_all=False):
    """
    Scans a single symbol for 9/21 SMA + RSI signals using pre-fetched DataFrame.
    """
    try:
        if settings is None:
            settings = {}
            
        if df is None or df.empty or len(df) < 50:
            return []
            
        # Extract Settings
        sma_fast = settings.get('sma_fast', 9)
        sma_slow = settings.get('sma_slow', 21)
        rsi_length = settings.get('rsi_length', 14)
        rsi_ob = settings.get('rsi_overbought', 70)
        rsi_os = settings.get('rsi_oversold', 30)
            
        # Apply Indicators
        df = indicators.apply_all_indicators(
            df, 
            sma_fast=sma_fast,
            sma_slow=sma_slow,
            rsi_length=rsi_length
        )
        
        if df.empty or 'SMA_Fast' not in df.columns or 'RSI' not in df.columns:
             return []
             
        # Create Shifted columns for crossover detection
        df['SMA_Fast_Prev'] = df['SMA_Fast'].shift(1)
        df['SMA_Slow_Prev'] = df['SMA_Slow'].shift(1)
        df['RSI_Prev'] = df['RSI'].shift(1)
             
        # Add Signal Logging columns
        df['Signal_Type'] = "None"
        df['Signal'] = 0
        df['Signal_Price'] = 0.0
        
        # --- Bullish Entry (Long) ---
        # 1. 9 SMA crosses ABOVE 21 SMA.
        df['cross_up'] = (df['SMA_Fast_Prev'] <= df['SMA_Slow_Prev']) & (df['SMA_Fast'] > df['SMA_Slow'])
        recent_cross_up = df['cross_up'].rolling(window=3).max() > 0
        
        # 2. RSI is BELOW 30 (Oversold zone) and starts moving UPWARDS.
        rsi_oversold = df['RSI'] < rsi_os
        rsi_moving_up = df['RSI'] > df['RSI_Prev']
        bullish_rsi = rsi_oversold & rsi_moving_up
        
        bullish_cond = recent_cross_up & bullish_rsi
        df.loc[bullish_cond, 'Signal_Type'] = "Bullish"
        df.loc[bullish_cond, 'Signal'] = 1
        df.loc[bullish_cond, 'Signal_Price'] = df['close']
        
        # --- Bearish Entry (Short) ---
        # 1. 9 SMA crosses BELOW 21 SMA.
        df['cross_down'] = (df['SMA_Fast_Prev'] >= df['SMA_Slow_Prev']) & (df['SMA_Fast'] < df['SMA_Slow'])
        recent_cross_down = df['cross_down'].rolling(window=3).max() > 0
        
        # 2. RSI is ABOVE 70 (Overbought zone) and starts moving DOWNWARDS.
        rsi_overbought = df['RSI'] > rsi_ob
        rsi_moving_down = df['RSI'] < df['RSI_Prev']
        bearish_rsi = rsi_overbought & rsi_moving_down
        
        bearish_cond = recent_cross_down & bearish_rsi
        df.loc[bearish_cond, 'Signal_Type'] = "Bearish"
        df.loc[bearish_cond, 'Signal'] = -1
        df.loc[bearish_cond, 'Signal_Price'] = df['close']

        current_bar = df.iloc[-1]
        
        # Determine if we are doing a live scan (last bar only) vs historical range scan
        is_live_scan = False
        if start_date is None and end_date is None:
            is_live_scan = True
            
        # Filter dataframe based on date range if provided
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
                ema21 = row.get('EMA21', np.nan)
                sma9 = row.get('SMA_Fast', np.nan)
                sma21 = row.get('SMA_Slow', np.nan)
                rsi_val = row.get('RSI', np.nan)
                
                pivot_high = row.get('high', signal_price)
                pivot_low = row.get('low', signal_price)
                
                # Default 0 for nan ATR
                if pd.isna(atr_val): atr_val = 0
                if pd.isna(ema21): ema21 = 0
                
                # SL/TP Logic Estimation (Standardized Output for Hub UI)
                if signal_type_str == "Bullish":
                    best_sl = pivot_low
                    ema_sl_str = f"₹{round(ema21, 2)}" if ema21 < signal_price and ema21 != 0 else f"₹{round(ema21, 2)} ⏳"
                else:
                    best_sl = pivot_high
                    ema_sl_str = f"₹{round(ema21, 2)}" if ema21 > signal_price and ema21 != 0 else f"₹{round(ema21, 2)} ⏳"
                    
                results_for_symbol.append({
                    "Stock": symbol,
                    "LTP": round(current_bar['close'], 2),
                    "Signal Time": idx.strftime('%Y-%m-%d %H:%M'),
                    "Signal Type": signal_type_str,
                    "Signal Price": signal_price,
                    "RSI": round(rsi_val, 2),
                    "SMA Fast/Slow": f"{round(sma9, 2)} / {round(sma21, 2)}",
                    "Trend": row.get('Trend', 'N/A'),
                    "EMA SL": ema_sl_str,
                    "Pivot SL": round(best_sl, 2),
                    "ATR": round(atr_val, 2),
                    "Volume": int(row.get('volume', 0))
                })
        
        if show_all and not results_for_symbol:
            if not pd.isna(current_bar['SMA_Fast']):
                results_for_symbol.append({
                    "Stock": symbol,
                    "LTP": round(current_bar['close'], 2),
                    "Signal Type": "None",
                    "Signal Time": "N/A",
                    "Signal Price": 0.0,
                    "RSI": round(current_bar.get('RSI', 0), 2),
                    "SMA Fast/Slow": f"{round(current_bar.get('SMA_Fast', 0), 2)} / {round(current_bar.get('SMA_Slow', 0), 2)}",
                    "Trend": current_bar.get('Trend', 'N/A'),
                    "EMA SL": "N/A",
                    "Pivot SL": 0.0,
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
