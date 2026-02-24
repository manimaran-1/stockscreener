import pandas as pd
import bb_macd_indicators as indicators
import bb_macd_data_loader as data_loader
import concurrent.futures
import pytz
import numpy as np

IST = pytz.timezone('Asia/Kolkata')

def scan_symbol_prefetched(symbol, df, settings=None, start_date=None, end_date=None, show_all=False):
    """
    Scans a single symbol for BB + MACD momentum signals using pre-fetched DataFrame.
    """
    try:
        if settings is None:
            settings = {}
            
        if df is None or df.empty or len(df) < 50:
            return []
            
        # Extract Settings
        macd_fast = settings.get('macd_fast', 12)
        macd_slow = settings.get('macd_slow', 26)
        macd_signal = settings.get('macd_signal', 9)
        bb_length = settings.get('bb_length', 20)
        bb_std = settings.get('bb_std', 2.0)
            
        # Apply Indicators
        df = indicators.apply_all_indicators(
            df, 
            macd_fast=macd_fast,
            macd_slow=macd_slow,
            macd_signal=macd_signal,
            bb_length=bb_length,
            bb_std=bb_std
        )
        
        if df.empty or 'MACD_Line' not in df.columns or 'BB_Lower' not in df.columns:
             return []
             
        # Create Shifted columns for crossover detection
        df['MACD_Line_Prev'] = df['MACD_Line'].shift(1)
        df['MACD_Signal_Prev'] = df['MACD_Signal'].shift(1)
             
        # Add Signal Logging columns
        df['Signal_Type'] = "None"
        df['Signal'] = 0
        df['Signal_Price'] = 0.0
        
        # Bullish Entry:
        # MACD Line crosses above Signal Line AND Price low touches/near Lower BB recently
        
        # 1. MACD cross UP happens within the last 3 bars (or today)
        df['cross_up'] = (df['MACD_Line_Prev'] <= df['MACD_Signal_Prev']) & (df['MACD_Line'] > df['MACD_Signal'])
        recent_macd_cross_up = df['cross_up'].rolling(window=3).max() > 0
        
        # 2. Touch/Near logic: Price low is less than or equal to Lower BB * 1.015 (1.5% buffer) within last 5 bars
        df['Touch_Lower_BB'] = df['low'] <= (df['BB_Lower'] * 1.015)
        recent_lower_bb_touch = df['Touch_Lower_BB'].rolling(window=5).max() > 0
        
        bullish_cond = recent_macd_cross_up & recent_lower_bb_touch
        df.loc[bullish_cond, 'Signal_Type'] = "Bullish"
        df.loc[bullish_cond, 'Signal'] = 1
        df.loc[bullish_cond, 'Signal_Price'] = df['close']
        
        # Bearish Entry: 
        # MACD Line crosses below Signal Line AND Price high touches/near Upper BB recently
        
        # 1. MACD cross DOWN happens within the last 3 bars (or today)
        df['cross_down'] = (df['MACD_Line_Prev'] >= df['MACD_Signal_Prev']) & (df['MACD_Line'] < df['MACD_Signal'])
        recent_macd_cross_down = df['cross_down'].rolling(window=3).max() > 0
        
        # 2. Touch/Near logic: Price high is greater than or equal to Upper BB * 0.985 (1.5% buffer) within last 5 bars
        df['Touch_Upper_BB'] = df['high'] >= (df['BB_Upper'] * 0.985)
        recent_upper_bb_touch = df['Touch_Upper_BB'].rolling(window=5).max() > 0
        
        bearish_cond = recent_macd_cross_down & recent_upper_bb_touch
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
                bb_lower_val = row.get('BB_Lower', 0)
                bb_upper_val = row.get('BB_Upper', 0)
                macd_line = row.get('MACD_Line', 0)
                macd_signal_line = row.get('MACD_Signal', 0)
                pivot_high = row.get('high', signal_price)
                pivot_low = row.get('low', signal_price)
                ema21 = row.get('EMA21', np.nan)
                
                # Default 0 for nan ATR
                if pd.isna(atr_val): atr_val = 0
                if pd.isna(ema21): ema21 = 0
                
                # SL/TP Logic Estimation (Standardized Output for Hub UI)
                if signal_type_str == "Bullish":
                    best_sl = pivot_low
                    best_tp = bb_upper_val
                    ema_sl_str = f"₹{round(ema21, 2)}" if ema21 < signal_price and ema21 != 0 else f"₹{round(ema21, 2)} ⏳"
                else:
                    best_sl = pivot_high
                    best_tp = bb_lower_val
                    ema_sl_str = f"₹{round(ema21, 2)}" if ema21 > signal_price and ema21 != 0 else f"₹{round(ema21, 2)} ⏳"
                    
                results_for_symbol.append({
                    "Stock": symbol,
                    "LTP": round(current_bar['close'], 2),
                    "Signal Time": idx.strftime('%Y-%m-%d %H:%M'),
                    "Signal Type": signal_type_str,
                    "Signal Price": signal_price,
                    "MACD / Signal": f"{round(macd_line, 2)} / {round(macd_signal_line, 2)}",
                    "Trend": row.get('Trend', 'N/A'),
                    "BB Lower": round(bb_lower_val, 2),
                    "BB Upper": round(bb_upper_val, 2),
                    "Target/Stop (BB Strategy)": f"₹{round(best_tp, 2)} / ₹{round(best_sl, 2)}",
                    "EMA SL": ema_sl_str,
                    "ATR": round(atr_val, 2),
                    "Volume": int(row.get('volume', 0))
                })
        
        if show_all and not results_for_symbol:
            if not pd.isna(current_bar['BB_Lower']):
                results_for_symbol.append({
                    "Stock": symbol,
                    "LTP": round(current_bar['close'], 2),
                    "Signal Type": "None",
                    "Signal Time": "N/A",
                    "Signal Price": 0.0,
                    "MACD / Signal": f"{round(current_bar.get('MACD_Line', 0), 2)} / {round(current_bar.get('MACD_Signal', 0), 2)}",
                    "Trend": current_bar.get('Trend', 'N/A'),
                    "BB Lower": round(current_bar.get('BB_Lower', 0), 2),
                    "BB Upper": round(current_bar.get('BB_Upper', 0), 2),
                    "Target/Stop (BB Strategy)": "N/A",
                    "EMA SL": "N/A",
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
