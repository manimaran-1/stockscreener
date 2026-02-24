import pandas as pd
import adx_sar_indicators as indicators
import adx_sar_data_loader as data_loader
import concurrent.futures
import pytz
import numpy as np

IST = pytz.timezone('Asia/Kolkata')

def scan_symbol_prefetched(symbol, df, settings=None, start_date=None, end_date=None, show_all=False):
    """
    Scans a single symbol for ADX + Parabolic SAR momentum signals using pre-fetched DataFrame.
    """
    try:
        if settings is None:
            settings = {}
            
        if df is None or df.empty or len(df) < 50:
            return []
            
        # Extract Settings
        adx_length = settings.get('adx_length', 14)
        psar_af = settings.get('psar_af', 0.02)
        psar_max_af = settings.get('psar_max_af', 0.2)
            
        # Apply Indicators
        df = indicators.apply_all_indicators(
            df, 
            adx_length=adx_length,
            psar_af=psar_af,
            psar_max_af=psar_max_af
        )
        
        if df.empty or 'ADX' not in df.columns or 'PSAR_Dir' not in df.columns:
             return []
             
        df['ADX_Prev'] = df['ADX'].shift(1)
             
        # Add Signal Logging columns
        df['Signal_Type'] = "None"
        df['Signal'] = 0
        df['Signal_Price'] = 0.0
        
        # Bullish Entry: ADX crosses above 25 AND SAR dots are below the candle (PSAR_Dir == 1)
        df['ADX_Cross_Up_25'] = (df['ADX_Prev'] < 25) & (df['ADX'] >= 25)
        adx_cross_recent = df['ADX_Cross_Up_25'].rolling(window=3).max().fillna(0).astype(bool)
        
        bullish_cond = adx_cross_recent & (df['PSAR_Dir'] == 1)
        df.loc[bullish_cond, 'Signal_Type'] = "Bullish"
        df.loc[bullish_cond, 'Signal'] = 1
        df.loc[bullish_cond, 'Signal_Price'] = df['close']
        
        # Bearish Entry: ADX crosses above 25 AND SAR dots are above the candle (PSAR_Dir == -1)
        bearish_cond = adx_cross_recent & (df['PSAR_Dir'] == -1)
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
                
                adx_val = row.get('ADX', 0)
                psar_val = row.get('PSAR', 0)
                pivot_high = row.get('high', signal_price)
                pivot_low = row.get('low', signal_price)
                ema21 = row.get('EMA21', np.nan)
                if pd.isna(ema21): ema21 = 0
                
                # SL/TP Logic Estimation
                if signal_type_str == "Bullish":
                    best_sl = psar_val  # Based on strategy, exit is when SAR is above
                    best_tp = signal_price + (signal_price - best_sl) * 1.5 if best_sl < signal_price else signal_price + atr_val
                    ema_sl_str = f"₹{round(ema21, 2)}" if ema21 < signal_price and ema21 != 0 else f"₹{round(ema21, 2)} ⏳"
                else:
                    best_sl = psar_val  # Exit is when SAR is below
                    best_tp = signal_price - (best_sl - signal_price) * 1.5 if best_sl > signal_price else signal_price - atr_val
                    ema_sl_str = f"₹{round(ema21, 2)}" if ema21 > signal_price and ema21 != 0 else f"₹{round(ema21, 2)} ⏳"
                    
                results_for_symbol.append({
                    "Stock": symbol,
                    "LTP": round(current_bar['close'], 2),
                    "Signal Time": idx.strftime('%Y-%m-%d %H:%M'),
                    "Signal Type": signal_type_str,
                    "Signal Price": signal_price,
                    "ADX": round(adx_val, 2),
                    "PSAR": round(psar_val, 2),
                    "Trend": row.get('Trend', 'N/A'),
                    "EMA SL": ema_sl_str,
                    "Best Method (PSAR SL)": f"₹{round(best_sl, 2)} / ₹{round(best_tp, 2)}",
                    "ATR": round(atr_val, 2),
                    "Volume": int(row.get('volume', 0))
                })
        
        if show_all and not results_for_symbol:
            if not pd.isna(current_bar.get('ADX')):
                results_for_symbol.append({
                    "Stock": symbol,
                    "LTP": round(current_bar['close'], 2),
                    "Signal Type": "None",
                    "Signal Time": "N/A",
                    "Signal Price": 0.0,
                    "ADX": round(current_bar.get('ADX', 0), 2),
                    "PSAR": round(current_bar.get('PSAR', 0), 2),
                    "Trend": current_bar.get('Trend', 'N/A'),
                    "EMA SL": "N/A",
                    "Best Method (PSAR SL)": "N/A",
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
