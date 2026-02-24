import pandas as pd
import keltner_indicators as indicators
import keltner_data_loader as data_loader
import concurrent.futures
import pytz

IST = pytz.timezone('Asia/Kolkata')

def scan_symbol_prefetched(symbol, df, settings=None, start_date=None, end_date=None, show_all=False):
    """
    Scans a single symbol for Keltner + RSI mean-reversion signals using pre-fetched DataFrame.
    """
    try:
        if settings is None:
            settings = {}
            
        if df is None or df.empty or len(df) < 50:
            return []
            
        # Extract Settings
        kc_length = settings.get('kc_length', 20)
        kc_mult = settings.get('kc_mult', 2.0)
        kc_bands_style = settings.get('kc_bands_style', "True Range")
        kc_atr_length = settings.get('kc_atr_length', 10)
        rsi_length = settings.get('rsi_length', 14)
            
        # Apply Indicators
        df = indicators.apply_all_indicators(
            df, 
            rsi_length=rsi_length, 
            kc_length=kc_length, 
            kc_atr_length=kc_atr_length, 
            kc_mult=kc_mult, 
            kc_bands_style=kc_bands_style
        )
        
        if df.empty or 'RSI' not in df.columns:
             return []
             
        # Add Signal Logging columns
        df['Signal_Type'] = "None"
        df['Signal'] = 0
        df['Signal_Price'] = 0.0
        
        # Bullish Entry: RSI <= 30 AND Low touches/dips below Lower KC
        bullish_cond = (df['RSI'] <= 30) & (df['low'] <= df['KC_Lower'])
        df.loc[bullish_cond, 'Signal_Type'] = "Bullish"
        df.loc[bullish_cond, 'Signal'] = 1
        df.loc[bullish_cond, 'Signal_Price'] = df['close']
        
        # Bearish Entry: RSI >= 70 AND High touches/crosses Upper KC
        bearish_cond = (df['RSI'] >= 70) & (df['high'] >= df['KC_Upper'])
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
                atr_val = row.get('ATR', 0)
                kc_middle = row.get('KC_Middle', 0)
                kc_lower = row.get('KC_Lower', 0)
                kc_upper = row.get('KC_Upper', 0)
                rsi_val = row.get('RSI', 0)
                pivot_high = row.get('high', signal_price)
                pivot_low = row.get('low', signal_price)
                ema21 = row.get('EMA21', 0)
                
                # SL/TP Logic Estimation (Standardized Output for Hub UI)
                if signal_type_str == "Bullish":
                    atr_sl = signal_price - atr_val
                    atr_tp = signal_price + (atr_val * 2)
                    bb_atr_sl = kc_lower - atr_val if kc_lower > 0 else atr_sl
                    bb_atr_tp = kc_upper + atr_val if kc_upper > 0 else atr_tp
                    pivot_sl = pivot_low
                    pivot_tp = signal_price + max(signal_price - pivot_sl, 0.01) * 2
                    ema_sl_str = f"₹{round(ema21, 2)}" if ema21 < signal_price else f"₹{round(ema21, 2)} ⏳"
                else:
                    atr_sl = signal_price + atr_val
                    atr_tp = signal_price - (atr_val * 2)
                    bb_atr_sl = kc_upper + atr_val if kc_upper > 0 else atr_sl
                    bb_atr_tp = kc_lower - atr_val if kc_lower > 0 else atr_tp
                    pivot_sl = pivot_high
                    pivot_tp = signal_price - max(pivot_sl - signal_price, 0.01) * 2
                    ema_sl_str = f"₹{round(ema21, 2)}" if ema21 > signal_price else f"₹{round(ema21, 2)} ⏳"
                    
                results_for_symbol.append({
                    "Stock": symbol,
                    "LTP": round(current_bar['close'], 2),
                    "Signal Time": idx.strftime('%Y-%m-%d %H:%M'),
                    "Signal Type": signal_type_str,
                    "Signal Price": signal_price,
                    "RSI": round(rsi_val, 2),
                    "Trend": row.get('Trend', 'N/A'),
                    "KC Lower": round(kc_lower, 2),
                    "KC Middle": round(kc_middle, 2),
                    "KC Upper": round(kc_upper, 2),
                    "Pivot (Best SL/TP)": f"₹{round(pivot_sl, 2)} / ₹{round(pivot_tp, 2)}",
                    "EMA SL": ema_sl_str,
                    "ATR (SL/TP)": f"₹{round(atr_sl, 2)} / ₹{round(atr_tp, 2)}",
                    "KC+ATR (SL/TP)": f"₹{round(bb_atr_sl, 2)} / ₹{round(bb_atr_tp, 2)}",
                    "ATR": round(atr_val, 2),
                    "Volume": int(row.get('volume', 0))
                })
        
        if show_all and not results_for_symbol:
            if not pd.isna(current_bar['RSI']):
                results_for_symbol.append({
                    "Stock": symbol,
                    "LTP": round(current_bar['close'], 2),
                    "Signal Type": "None",
                    "Signal Time": "N/A",
                    "Signal Price": 0.0,
                    "RSI": round(current_bar.get('RSI', 0), 2),
                    "Trend": current_bar.get('Trend', 'N/A'),
                    "KC Lower": round(current_bar.get('KC_Lower', 0), 2),
                    "KC Middle": round(current_bar.get('KC_Middle', 0), 2),
                    "KC Upper": round(current_bar.get('KC_Upper', 0), 2),
                    "Pivot (Best SL/TP)": "N/A",
                    "EMA SL": "N/A",
                    "ATR (SL/TP)": "N/A",
                    "KC+ATR (SL/TP)": "N/A",
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
