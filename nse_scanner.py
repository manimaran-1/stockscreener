import pandas as pd
import nse_indicators as indicators
import data_loader
import concurrent.futures
import pytz
from datetime import datetime

IST = pytz.timezone('Asia/Kolkata')

def check_conditions(df, symbol):
    """
    Checks if any candle in the relevant period meets the buy criteria.
    For Daily: Checks last candle.
    For Intraday: Checks all candles from today.
    Returns a list of result dictionaries.
    """
    if df is None or not isinstance(df, pd.DataFrame):
        return []
    if df.empty or len(df) < 50: # Need enough data for indicators
        return []
        
    # Calculate Indicators
    close = df['close']
    volume = df['volume']
    
    ema5 = indicators.calculate_ema(df, 5)
    ema9 = indicators.calculate_ema(df, 9)
    ema21 = indicators.calculate_ema(df, 21)
    
    stoch_rsi_k = indicators.calculate_stoch_rsi(df, length=14, rsi_length=14, k=3, d=3)
    smi = indicators.calculate_smi(df, length=10, smooth=3)
    macd_line = indicators.calculate_macd(df, fast=12, slow=26, signal=9)
    
    # Advanced Risk Indicators
    atr = indicators.calculate_atr(df, length=14)
    bb_lower, bb_upper = indicators.calculate_bollinger_bands(df, length=20, std_dev=2.0)
    
    results = []
    
    # Determine range to check
    # If intraday (frequency < 1d), check "today's" candles
    # If daily or above, just check the last candle
    
    # Heuristic for intraday: check if time diff between last two candles is < 1 day
    is_intraday = False
    if len(df) > 1:
        time_diff = df.index[-1] - df.index[-2]
        if time_diff < pd.Timedelta(days=1):
            is_intraday = True

    indices_to_check = []
    if is_intraday:
        # Get today's date in IST
        now_ist = datetime.now(IST)
        today_date = now_ist.date()
        
        # Check last 75 candles (heuristic to cover a day for 5m/15m)
        candidates = df.index[-75:] 
        
        # Filter for today
        today_indices = [idx for idx in candidates if idx.date() == today_date]
        
        if today_indices:
            indices_to_check = today_indices
        else:
            # If no data for "today" (e.g. run at night), use the last available date
            last_date = df.index[-1].date()
            indices_to_check = [idx for idx in candidates if idx.date() == last_date]
    else:
        # Check only the last completed candle
        indices_to_check = [df.index[-1]]
    
    for idx in indices_to_check:
        try:
            # Locate position integer for iloc equivalent
            pos = df.index.get_loc(idx)
            
            # Using .iloc[pos] to get scalar values
            c = close.iloc[pos]
            v = volume.iloc[pos]
            e5 = ema5.iloc[pos]
            e9 = ema9.iloc[pos]
            e21 = ema21.iloc[pos]
            k = stoch_rsi_k.iloc[pos]
            s = smi.iloc[pos]
            m = macd_line.iloc[pos]
            
            # Extract Risk Indicators
            atr_v = atr.iloc[pos] if atr is not None and not atr.empty else 0
            bbl_v = bb_lower.iloc[pos] if bb_lower is not None and not bb_lower.empty else 0
            bbu_v = bb_upper.iloc[pos] if bb_upper is not None and not bb_upper.empty else 0
            high_v = df['high'].iloc[pos]
            low_v = df['low'].iloc[pos]
            
            # Check Conditions
            if (c > e5 and
                c > e9 and
                c > e21 and
                k > 70 and
                s > 30 and
                m > 0.75):
                
                # SL/TP Logic (Main scanner implies Bullish signal)
                atr_sl = c - atr_v
                atr_tp = c + (atr_v * 2)
                bb_atr_sl = bbl_v - atr_v if bbl_v > 0 else atr_sl
                bb_atr_tp = bbu_v + atr_v if bbu_v > 0 else atr_tp
                pivot_sl = low_v
                pivot_tp = c + max(c - pivot_sl, 0.01) * 2
                ema_sl_str = f"₹{round(e21, 2)}" if e21 < c else f"₹{round(e21, 2)} ⏳"
                
                results.append({
                    'Stock Name': symbol,
                    'LTP': round(c, 2),
                    'Signal Time': idx.strftime('%d-%m-%Y %H:%M'),
                    "Pivot (Best SL/TP)": f"₹{round(pivot_sl, 2)} / ₹{round(pivot_tp, 2)}",
                    "EMA SL": ema_sl_str,
                    "ATR (SL/TP)": f"₹{round(atr_sl, 2)} / ₹{round(atr_tp, 2)}",
                    "BB+ATR (SL/TP)": f"₹{round(bb_atr_sl, 2)} / ₹{round(bb_atr_tp, 2)}",
                    "ATR": round(atr_v, 2),
                    "BB Lower": round(bbl_v, 2),
                    "BB Upper": round(bbu_v, 2),
                    'Volume': int(v),
                    'EMA5': round(e5, 2),
                    'EMA9': round(e9, 2),
                    'EMA21': round(e21, 2),
                    'Stoch RSI K': round(k, 2),
                    'SMI': round(s, 2),
                    'MACD': round(m, 2)
                })
        except Exception as e:
            continue
            
    return results

def scan_symbol(symbol, interval):
    """
    Scans a single symbol fetching its own data.
    """
    df = data_loader.fetch_data(symbol, interval=interval)
    return scan_symbol_prefetched(symbol, df)

def scan_symbol_prefetched(symbol, df):
    """
    Scans a single symbol using a pre-fetched DataFrame.
    """
    return check_conditions(df, symbol)

def scan_market(symbols, interval='1d', progress_callback=None):
    """
    Parallel bulk scan of market symbols using pre-fetched block data.
    """
    all_results = []
    
    # Pre-fetch all data simultaneously
    bulk_data_dict = data_loader.fetch_bulk_data(symbols, interval=interval, _progress_callback=progress_callback)
    
    total = len(symbols)
    completed = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(scan_symbol_prefetched, sym, bulk_data_dict.get(sym)): sym 
            for sym in symbols
        }
        
        for future in concurrent.futures.as_completed(futures):
            res_list = future.result()
            completed += 1
            if progress_callback:
                progress_callback(completed, total)
                
            if res_list:
                all_results.extend(res_list)
                
    return pd.DataFrame(all_results)
