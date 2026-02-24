import pandas as pd
import fib_chop_indicators as indicators
import fib_chop_data_loader as data_loader
import concurrent.futures
import pytz
import numpy as np

IST = pytz.timezone('Asia/Kolkata')

def scan_symbol_prefetched(symbol, df, settings=None, start_date=None, end_date=None, show_all=False):
    """
    Scans a single symbol for Fib + Chop Zone pullbacks using pre-fetched DataFrame.
    """
    try:
        if settings is None:
            settings = {}
            
        if df is None or df.empty or len(df) < 50:
            return []
            
        # Extract Settings
        fib_lookback = settings.get('fib_lookback', 50)
        chop_length = settings.get('chop_length', 14)
            
        # Apply Indicators
        df = indicators.apply_all_indicators(
            df, 
            fib_lookback=fib_lookback,
            chop_length=chop_length
        )
        
        if df.empty or 'Fib_50' not in df.columns or 'Chop' not in df.columns:
             return []
             
        # Add Signal Logging columns
        df['Signal_Type'] = "None"
        df['Signal'] = 0
        df['Signal_Price'] = 0.0
        
        # Logic: Chop Zone drops below 38.2 (turns red/strong trend) and price is near 50% Fib
        df['Chop_Turns_Red'] = (df['Chop'].shift(1) >= 38.2) & (df['Chop'] < 38.2)
        chop_red_recent = df['Chop_Turns_Red'].rolling(window=3).max().fillna(0).astype(bool)
        
        # Near 50% Fib (within 1.5% margin)
        near_fib_50 = (abs(df['close'] - df['Fib_50']) / df['close']) < 0.015
        
        # Bullish Pullback: Price is above long term moving average, pulled back to 50% Fib, and trend resumes (Chop Red)
        bullish_cond = near_fib_50 & chop_red_recent & (df['close'] > df['EMA21'])
        df.loc[bullish_cond, 'Signal_Type'] = "Bullish Focus"
        df.loc[bullish_cond, 'Signal'] = 1
        df.loc[bullish_cond, 'Signal_Price'] = df['close']
        
        # Bearish Pullback: Price is below long term moving average, rallied to 50% Fib, and trend resumes (Chop Red)
        bearish_cond = near_fib_50 & chop_red_recent & (df['close'] < df['EMA21'])
        df.loc[bearish_cond, 'Signal_Type'] = "Bearish Focus"
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
                
                if pd.isna(atr_val): atr_val = 0
                
                fib50 = row.get('Fib_50', 0)
                fib618 = row.get('Fib_618', 0)
                chop_val = row.get('Chop', 50)
                ema21 = row.get('EMA21', np.nan)
                if pd.isna(ema21): ema21 = 0
                
                # SL/TP Logic Estimation based on strategy (SL at 61.8 Fib)
                if signal_type_str == "Bullish Focus":
                    best_sl = min(row.get('Fib_618', signal_price - atr_val), row.get('Fib_382', signal_price - atr_val))
                    if best_sl > signal_price: best_sl = signal_price - atr_val
                    best_tp = signal_price + (signal_price - best_sl) * 1.5
                    ema_sl_str = f"₹{round(ema21, 2)}" if ema21 < signal_price and ema21 != 0 else f"₹{round(ema21, 2)} ⏳"
                else:
                    best_sl = max(row.get('Fib_618', signal_price + atr_val), row.get('Fib_382', signal_price + atr_val))
                    if best_sl < signal_price: best_sl = signal_price + atr_val
                    best_tp = signal_price - (best_sl - signal_price) * 1.5
                    ema_sl_str = f"₹{round(ema21, 2)}" if ema21 > signal_price and ema21 != 0 else f"₹{round(ema21, 2)} ⏳"
                    
                results_for_symbol.append({
                    "Stock": symbol,
                    "LTP": round(current_bar['close'], 2),
                    "Signal Time": idx.strftime('%Y-%m-%d %H:%M'),
                    "Signal Type": signal_type_str,
                    "Signal Price": signal_price,
                    "Fib 50": round(fib50, 2),
                    "Chop Zone": round(chop_val, 2),
                    "Trend": row.get('Trend', 'N/A'),
                    "EMA SL": ema_sl_str,
                    "Best Method (Fib SL)": f"₹{round(best_sl, 2)} / ₹{round(best_tp, 2)}",
                    "ATR": round(atr_val, 2),
                    "Volume": int(row.get('volume', 0))
                })
        
        if show_all and not results_for_symbol:
            if not pd.isna(current_bar.get('Chop')):
                # In standard ta.chop, <38.2 is strong trend, >61.8 is choppy
                chop_state = "Choppy (>61.8)" if current_bar.get('Chop', 50) > 61.8 else ("Trending (<38.2)" if current_bar.get('Chop', 50) < 38.2 else "Neutral")
                results_for_symbol.append({
                    "Stock": symbol,
                    "LTP": round(current_bar['close'], 2),
                    "Signal Type": "None",
                    "Signal Time": "N/A",
                    "Signal Price": 0.0,
                    "Fib 50": round(current_bar.get('Fib_50', 0), 2),
                    "Chop Zone": f"{round(current_bar.get('Chop', 50), 2)} ({chop_state})",
                    "Trend": current_bar.get('Trend', 'N/A'),
                    "EMA SL": "N/A",
                    "Best Method (Fib SL)": "N/A",
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
