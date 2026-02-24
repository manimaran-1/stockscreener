import pandas as pd
import vwma_macd_indicators as indicators
import vwma_macd_data_loader as data_loader
import concurrent.futures
import pytz
import numpy as np

IST = pytz.timezone('Asia/Kolkata')

def scan_symbol_prefetched(symbol, df, settings=None, start_date=None, end_date=None, show_all=False):
    """
    Scans a single symbol for VWMA + MACD momentum signals using pre-fetched DataFrame.
    """
    try:
        if settings is None:
            settings = {}
            
        if df is None or df.empty or len(df) < 50:
            return []
            
        # Extract Settings
        vwma_length = settings.get('vwma_length', 20)
        macd_fast = settings.get('macd_fast', 12)
        macd_slow = settings.get('macd_slow', 26)
        macd_signal = settings.get('macd_signal', 9)
            
        # Apply Indicators
        df = indicators.apply_all_indicators(
            df, 
            vwma_length=vwma_length, 
            macd_fast=macd_fast,
            macd_slow=macd_slow,
            macd_signal=macd_signal
        )
        
        if df.empty or 'MACD_Line' not in df.columns:
             return []
             
        # Create Shifted columns for crossover detection
        df['MACD_Line_Prev'] = df['MACD_Line'].shift(1)
        df['MACD_Signal_Prev'] = df['MACD_Signal'].shift(1)
             
        # Add Signal Logging columns
        df['Signal_Type'] = "None"
        df['Signal'] = 0
        df['Signal_Price'] = 0.0
        
        # Bullish Entry: MACD Line crossed above Signal Line recently AND Price > VWMA
        df['MACD_Cross_Up'] = (df['MACD_Line_Prev'] <= df['MACD_Signal_Prev']) & (df['MACD_Line'] > df['MACD_Signal'])
        # Look back 3 bars for the crossover to make signal capture more robust
        macd_cross_up_recent = df['MACD_Cross_Up'].rolling(window=3).max().fillna(0).astype(bool)
        price_above_vwma = df['close'] > df['VWMA']
        
        bullish_cond = macd_cross_up_recent & price_above_vwma
        df.loc[bullish_cond, 'Signal_Type'] = "Bullish"
        df.loc[bullish_cond, 'Signal'] = 1
        df.loc[bullish_cond, 'Signal_Price'] = df['close']
        
        # Bearish Entry: MACD Line crossed below Signal Line recently AND Price < VWMA
        df['MACD_Cross_Down'] = (df['MACD_Line_Prev'] >= df['MACD_Signal_Prev']) & (df['MACD_Line'] < df['MACD_Signal'])
        macd_cross_down_recent = df['MACD_Cross_Down'].rolling(window=3).max().fillna(0).astype(bool)
        price_below_vwma = df['close'] < df['VWMA']
        
        bearish_cond = macd_cross_down_recent & price_below_vwma
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
                vwma_val = row.get('VWMA', 0)
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
                    "MACD / Signal": f"{round(macd_line, 2)} / {round(macd_signal_line, 2)}",
                    "Trend": row.get('Trend', 'N/A'),
                    "VWMA": round(vwma_val, 2),
                    "Pivot (Best SL/TP)": f"₹{round(pivot_sl, 2)} / ₹{round(pivot_tp, 2)}",
                    "EMA SL": ema_sl_str,
                    "Best Method (Signal ± ATR)": f"₹{round(best_sl, 2)} / ₹{round(best_tp, 2)}",
                    "ATR": round(atr_val, 2),
                    "Volume": int(row.get('volume', 0))
                })
        
        if show_all and not results_for_symbol:
            if not pd.isna(current_bar['VWMA']):
                results_for_symbol.append({
                    "Stock": symbol,
                    "LTP": round(current_bar['close'], 2),
                    "Signal Type": "None",
                    "Signal Time": "N/A",
                    "Signal Price": 0.0,
                    "MACD / Signal": f"{round(current_bar.get('MACD_Line', 0), 2)} / {round(current_bar.get('MACD_Signal', 0), 2)}",
                    "Trend": current_bar.get('Trend', 'N/A'),
                    "VWMA": round(current_bar.get('VWMA', 0), 2),
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
