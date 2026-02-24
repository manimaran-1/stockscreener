import pandas as pd
import indicators
import data_loader
import concurrent.futures
import pytz

def scan_symbol_dmi(symbol, interval, start_date=None, end_date=None, show_all=False):
    """
    Scans a single symbol fetching its own data.
    """
    df = data_loader.fetch_data(symbol, interval=interval)
    return scan_symbol_dmi_prefetched(symbol, df, start_date, end_date, show_all)

def scan_symbol_dmi_prefetched(symbol, df, start_date=None, end_date=None, show_all=False):
    """
    Scans a single symbol for DMI crossovers using a pre-fetched DataFrame.
    """
    try:
        if df is None or df.empty or len(df) < 50:
            return []
            
        # Apply Indicators
        dmi_length = 14
        df = indicators.apply_all_indicators(df, dmi_length=dmi_length)
        
        if df.empty:
             return []
             
        current_bar = df.iloc[-1]
        
        # Filter dataframe based on date range if provided
        filtered_df = df.copy()
        if start_date and end_date:
            try:
                tz = pytz.timezone('Asia/Kolkata')
                from datetime import datetime, time
                s_dt = tz.localize(datetime.combine(start_date, time.min))
                e_dt = tz.localize(datetime.combine(end_date, time.max))
                filtered_df = filtered_df[(filtered_df.index >= s_dt) & (filtered_df.index <= e_dt)]
            except Exception as e:
                pass

        if filtered_df.empty:
             return []
             
        signal_rows = filtered_df[filtered_df['Signal'] != 0]
        results_for_symbol = []
        
        if not signal_rows.empty:
            for idx, row in signal_rows.iterrows():
                signal_price = row['close']
                signal_type_str = row['Signal_Type']
                atr_val = row.get('ATR', 0)
                bb_lower = row.get('BBL', 0)
                bb_upper = row.get('BBU', 0)
                pivot_high = row.get('high', signal_price)
                pivot_low = row.get('low', signal_price)
                ema21 = row.get('EMA21', 0)
                
                # SL/TP Logic
                if signal_type_str == "Buy":
                    atr_sl = signal_price - atr_val
                    atr_tp = signal_price + (atr_val * 2)
                    bb_atr_sl = bb_lower - atr_val if bb_lower > 0 else atr_sl
                    bb_atr_tp = bb_upper + atr_val if bb_upper > 0 else atr_tp
                    pivot_sl = pivot_low
                    pivot_tp = signal_price + max(signal_price - pivot_sl, 0.01) * 2
                    ema_sl_str = f"₹{round(ema21, 2)}" if ema21 < signal_price else f"₹{round(ema21, 2)} ⏳"
                else:
                    atr_sl = signal_price + atr_val
                    atr_tp = signal_price - (atr_val * 2)
                    bb_atr_sl = bb_upper + atr_val if bb_upper > 0 else atr_sl
                    bb_atr_tp = bb_lower - atr_val if bb_lower > 0 else atr_tp
                    pivot_sl = pivot_high
                    pivot_tp = signal_price - max(pivot_sl - signal_price, 0.01) * 2
                    ema_sl_str = f"₹{round(ema21, 2)}" if ema21 > signal_price else f"₹{round(ema21, 2)} ⏳"

                results_for_symbol.append({
                    "Stock": symbol,
                    "LTP": round(current_bar['close'], 2), 
                    "Signal Type": signal_type_str,
                    "Signal Time": idx.strftime('%Y-%m-%d %H:%M:%S'),
                    "Signal Price": round(signal_price, 2),
                    "Pivot (Best SL/TP)": f"₹{round(pivot_sl, 2)} / ₹{round(pivot_tp, 2)}",
                    "EMA SL": ema_sl_str,
                    "ATR (SL/TP)": f"₹{round(atr_sl, 2)} / ₹{round(atr_tp, 2)}",
                    "BB+ATR (SL/TP)": f"₹{round(bb_atr_sl, 2)} / ₹{round(bb_atr_tp, 2)}",
                    "ATR": round(atr_val, 2),
                    "BB Lower": round(bb_lower, 2),
                    "BB Upper": round(bb_upper, 2),
                    "+DI": round(row['+DI'], 2),
                    "-DI": round(row['-DI'], 2),
                    "ADX": round(row['ADX'], 2),
                    "Support 1": round(row['S1'], 2) if not pd.isna(row['S1']) else "N/A",
                    "Resistance 1": round(row['R1'], 2) if not pd.isna(row['R1']) else "N/A",
                    "Volume": int(row['volume']) if 'volume' in row else 0
                })
        
        if show_all and not results_for_symbol:
            if not pd.isna(current_bar['ADX']):
                results_for_symbol.append({
                    "Stock": symbol,
                    "LTP": round(current_bar['close'], 2),
                    "Signal Type": "None",
                    "Signal Time": "N/A",
                    "Signal Price": 0.0,
                    "+DI": round(current_bar['+DI'], 2),
                    "-DI": round(current_bar['-DI'], 2),
                    "ADX": round(current_bar['ADX'], 2),
                    "Support 1": round(current_bar['S1'], 2) if not pd.isna(current_bar['S1']) else "N/A",
                    "Resistance 1": round(current_bar['R1'], 2) if not pd.isna(current_bar['R1']) else "N/A",
                    "Volume": int(current_bar['volume']) if 'volume' in current_bar else 0
                })

        return results_for_symbol
    except Exception as e:
        return []

def scan_market(symbols, interval='1d', start_date=None, end_date=None, show_all=False, force_refresh_token=None, progress_callback=None):
    """
    Parallel bulk scan of a list of symbols using pre-fetched block data.
    """
    results = []
    
    # Pre-fetch all data simultaneously using chunks and progress bar
    bulk_data_dict = data_loader.fetch_bulk_data(symbols, interval=interval, force_refresh_token=force_refresh_token)
    
    # Calculate indicators using CPU threads since data is already loaded
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_symbol = {
            executor.submit(scan_symbol_dmi_prefetched, sym, bulk_data_dict.get(sym), start_date, end_date, show_all): sym 
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
            completed += 1
            if progress_callback:
                progress_callback(completed, total, f"Analyzing {completed}/{total} symbols...")
                
    return pd.DataFrame(results)

def scan_symbol_dsmi(symbol, interval, start_date=None, end_date=None, show_all=False, length=20, weak_thr=10, neutral_thr=35, strong_thr=45, overheat_thr=55, entry_level=20):
    """
    Scans a single symbol for Modified DSMI logic fetching its own data.
    """
    df = data_loader.fetch_data(symbol, interval=interval)
    return scan_symbol_dsmi_prefetched(symbol, df, start_date, end_date, show_all, length, weak_thr, neutral_thr, strong_thr, overheat_thr, entry_level)

def scan_symbol_dsmi_prefetched(symbol, df, start_date=None, end_date=None, show_all=False, length=20, weak_thr=10, neutral_thr=35, strong_thr=45, overheat_thr=55, entry_level=20):
    """
    Scans a single symbol for Modified DSMI logic using pre-fetched DataFrame.
    """
    try:
        if df is None or df.empty or len(df) < length * 2:
            return []
            
        # Apply Indicators
        df = indicators.apply_dsmi_indicators(df, length, weak_thr, neutral_thr, strong_thr, overheat_thr, entry_level)
        
        if df.empty:
             return []
             
        current_bar = df.iloc[-1]
        
        # Filter dataframe based on date range
        filtered_df = df.copy()
        if start_date and end_date:
            try:
                tz = pytz.timezone('Asia/Kolkata')
                from datetime import datetime, time
                s_dt = tz.localize(datetime.combine(start_date, time.min))
                e_dt = tz.localize(datetime.combine(end_date, time.max))
                filtered_df = filtered_df[(filtered_df.index >= s_dt) & (filtered_df.index <= e_dt)]
            except Exception as e:
                pass

        if filtered_df.empty:
             return []
             
        signal_rows = filtered_df[filtered_df['DSMI_Signal'] != "None"]
        results_for_symbol = []
        
        if not signal_rows.empty:
            for idx, row in signal_rows.iterrows():
                signal_price = row['close']
                signal_type_str = row['DSMI_Signal']
                atr_val = row.get('ATR', 0)
                bb_lower = row.get('BBL', 0)
                bb_upper = row.get('BBU', 0)
                pivot_high = row.get('high', signal_price)
                pivot_low = row.get('low', signal_price)
                ema21 = row.get('EMA21', 0)
                
                # SL/TP Logic
                if signal_type_str == "Buy":
                    atr_sl = signal_price - atr_val
                    atr_tp = signal_price + (atr_val * 2)
                    bb_atr_sl = bb_lower - atr_val if bb_lower > 0 else atr_sl
                    bb_atr_tp = bb_upper + atr_val if bb_upper > 0 else atr_tp
                    pivot_sl = pivot_low
                    pivot_tp = signal_price + max(signal_price - pivot_sl, 0.01) * 2
                    ema_sl_str = f"₹{round(ema21, 2)}" if ema21 < signal_price else f"₹{round(ema21, 2)} ⏳"
                else:
                    atr_sl = signal_price + atr_val
                    atr_tp = signal_price - (atr_val * 2)
                    bb_atr_sl = bb_upper + atr_val if bb_upper > 0 else atr_sl
                    bb_atr_tp = bb_lower - atr_val if bb_lower > 0 else atr_tp
                    pivot_sl = pivot_high
                    pivot_tp = signal_price - max(pivot_sl - signal_price, 0.01) * 2
                    ema_sl_str = f"₹{round(ema21, 2)}" if ema21 > signal_price else f"₹{round(ema21, 2)} ⏳"

                results_for_symbol.append({
                    "Stock": symbol,
                    "LTP": round(current_bar['close'], 2), 
                    "Signal Type": signal_type_str,
                    "Signal Time": idx.strftime('%Y-%m-%d %H:%M:%S'),
                    "Signal Price": round(signal_price, 2),
                    "Pivot (Best SL/TP)": f"₹{round(pivot_sl, 2)} / ₹{round(pivot_tp, 2)}",
                    "EMA SL": ema_sl_str,
                    "ATR (SL/TP)": f"₹{round(atr_sl, 2)} / ₹{round(atr_tp, 2)}",
                    "BB+ATR (SL/TP)": f"₹{round(bb_atr_sl, 2)} / ₹{round(bb_atr_tp, 2)}",
                    "ATR": round(atr_val, 2),
                    "BB Lower": round(bb_lower, 2),
                    "BB Upper": round(bb_upper, 2),
                    "+DS": round(row['+DS'], 2),
                    "-DS": round(row['-DS'], 2),
                    "DSMI": round(row['DSMI'], 2),
                    "Trend Strength": row['Trend_Strength_Text'],
                    "Volume": int(row['volume']) if 'volume' in row else 0
                })
        
        if show_all and not results_for_symbol:
            if not pd.isna(current_bar['DSMI']):
                results_for_symbol.append({
                    "Stock": symbol,
                    "LTP": round(current_bar['close'], 2),
                    "Signal Type": "None",
                    "Signal Time": "N/A",
                    "Signal Price": 0.0,
                    "+DS": round(current_bar['+DS'], 2),
                    "-DS": round(current_bar['-DS'], 2),
                    "DSMI": round(current_bar['DSMI'], 2),
                    "Trend Strength": current_bar['Trend_Strength_Text'],
                    "Volume": int(current_bar['volume']) if 'volume' in current_bar else 0
                })

        return results_for_symbol
    except Exception as e:
        return []

def scan_market_dsmi(symbols, interval='1d', start_date=None, end_date=None, show_all=False, length=20, weak_thr=10, neutral_thr=35, strong_thr=45, overheat_thr=55, entry_level=20, force_refresh_token=None, progress_callback=None):
    """
    Parallel bulk scan of a list of symbols using the Modified DSMI setup and pre-fetched data.
    """
    results = []
    
    # Pre-fetch all data simultaneously (Chunked)
    bulk_data_dict = data_loader.fetch_bulk_data(symbols, interval=interval, force_refresh_token=force_refresh_token)
    
    # Compute using CPU threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_symbol = {
            executor.submit(scan_symbol_dsmi_prefetched, sym, bulk_data_dict.get(sym), start_date, end_date, show_all, length, weak_thr, neutral_thr, strong_thr, overheat_thr, entry_level): sym 
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
            completed += 1
            if progress_callback:
                progress_callback(completed, total, f"Analyzing {completed}/{total} symbols...")
                
    return pd.DataFrame(results)

def scan_symbol_arbitrage(symbol, interval="1m"):
    """
    Scans a single symbol for NSE vs BSE arbitrage opportunities.
    """
    try:
        if symbol.endswith(".NS") or symbol.endswith(".BO"):
            base_symbol = symbol.split('.')[0]
        else:
            base_symbol = symbol
            
        symbol_ns = base_symbol + ".NS"
        symbol_bo = base_symbol + ".BO"
        
        # Determine exact prices and full-day volumes via fast_info
        import yfinance as yf
        
        # NS Info
        ns_price = 0
        ns_vol = 0
        ns_circuit = False
        try:
            ns_info = yf.Ticker(symbol_ns).fast_info
            ns_price = round(ns_info.last_price, 2)
            ns_vol = int(ns_info.last_volume) if ns_info.last_volume else 0
            
            # Circuit Check Approximation
            pc = ns_info.previous_close
            dh = ns_info.day_high
            dl = ns_info.day_low
            if pc and pc > 0:
                pct = (ns_price - pc) / pc * 100
                if (ns_price == round(dh, 2) and pct >= 4.9) or (ns_price == round(dl, 2) and pct <= -4.9):
                    ns_circuit = True
        except Exception:
            pass
            
        # BO Info
        bo_price = 0
        bo_vol = 0
        bo_circuit = False
        try:
            bo_info = yf.Ticker(symbol_bo).fast_info
            bo_price = round(bo_info.last_price, 2)
            bo_vol = int(bo_info.last_volume) if bo_info.last_volume else 0
            
            # Circuit Check Approximation
            pc = bo_info.previous_close
            dh = bo_info.day_high
            dl = bo_info.day_low
            if pc and pc > 0:
                pct = (bo_price - pc) / pc * 100
                if (bo_price == round(dh, 2) and pct >= 4.9) or (bo_price == round(dl, 2) and pct <= -4.9):
                    bo_circuit = True
        except Exception:
            pass
            
        # Get exactly when the last trade happened from the most recent 1-minute bar.
        date_str = "N/A"
        time_str = "N/A"
        
        try:
             # Fast way to grab the exact timestamp of the last 1m tick for accurate trading time
             df_ns = data_loader.fetch_data(symbol_ns, period="1d", interval="1m")
             if df_ns is not None and not df_ns.empty:
                 last_timestamp = df_ns.index[-1]
                 date_str = last_timestamp.strftime('%Y-%m-%d')
                 time_str = last_timestamp.strftime('%H:%M:%S')
                 
                 # If fast_info failed us entirely, fallback to this candle's data
                 if ns_price == 0: ns_price = round(df_ns.iloc[-1]['close'], 2)
                 if ns_vol == 0: ns_vol = int(df_ns['volume'].sum())
        except Exception:
             from datetime import datetime
             current_time = datetime.now()
             date_str = current_time.strftime('%Y-%m-%d')
             time_str = current_time.strftime('%H:%M:%S')
             
        if bo_price == 0:
             try:
                 df_bo = data_loader.fetch_data(symbol_bo, period="1d", interval="1m")
                 if df_bo is not None and not df_bo.empty:
                     bo_price = round(df_bo.iloc[-1]['close'], 2)
                     if bo_vol == 0: bo_vol = int(df_bo['volume'].sum())
             except Exception:
                 pass
        
        if ns_price == 0 or bo_price == 0:
            return None
            
        # Check if either is at a circuit (we still show it, but indicate in the UI).
        ns_circuit_str = "Yes" if ns_circuit else "No"
        bo_circuit_str = "Yes" if bo_circuit else "No"
            
        diff = abs(ns_price - bo_price)
        diff_pct = (diff / min(ns_price, bo_price)) * 100
        
        higher_exchange = "NSE" if ns_price > bo_price else ("BSE" if bo_price > ns_price else "Equal")
            
        return {
            "Stock": base_symbol,
            "Date": date_str,
            "Time": time_str,
            "NSE Circuit": ns_circuit_str,
            "BSE Circuit": bo_circuit_str,
            "NSE Price": ns_price,
            "BSE Price": bo_price,
            "Higher Exchange": higher_exchange,
            "Diff (₹)": round(diff, 2),
            "Diff (%)": round(diff_pct, 2),
            "NSE Volume": ns_vol,
            "BSE Volume": bo_vol
        }
    except Exception as e:
        return None

def scan_market_arbitrage(symbols, interval="1m", min_diff=0.0, progress_callback=None):
    """
    Bulk scans NSE vs BSE arbitrage utilizing TradingView's Scanner API
    for instantaneous, synchronized real-time exact quotes.
    """
    import requests
    import pandas as pd
    from datetime import datetime
    
    results = []
    
    # Ensure symbols are clean (no .NS)
    base_symbols = [s.split('.')[0] if '.' in s else s for s in symbols]
    base_symbols = list(set(base_symbols))
    
    # We query both NSE and BSE concurrently representing 2x symbols
    tickers = []
    for sym in base_symbols:
        # Tradingview maps & to _ for BSE
        bse_sym = sym.replace('&', '_')
        tickers.append(f"NSE:{sym}")
        tickers.append(f"BSE:{bse_sym}")
        
    url = "https://scanner.tradingview.com/india/scan"
    
    # Chunk the payload to avoid massive requests (max 500)
    chunk_size = 200 
    ticker_chunks = [tickers[i:i + chunk_size] for i in range(0, len(tickers), chunk_size)]
    
    tv_data = {}
    
    total_chunks = len(ticker_chunks)
    completed_symbols = 0
    total_symbols = len(tickers)
    
    for i, chunk in enumerate(ticker_chunks):
        if progress_callback:
            # Report progress before fetching
            progress_callback(completed_symbols, total_symbols, f"Fetching live prices... ({completed_symbols}/{total_symbols})")
            
        payload = {
            "symbols": {"tickers": chunk},
            "columns": ["close", "volume", "high", "low", "change", "price_52_week_high", "price_52_week_low", "Perf.Y"]
        }
        try:
            r = requests.post(url, json=payload, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
            if r.status_code == 200:
                data = r.json().get('data', [])
                for item in data:
                    sym_name = item['s']
                    d = item['d']
                    # d[0] = close, d[1] = volume, d[2] = high, d[3] = low, d[4] = change
                    tv_data[sym_name] = {
                        "price": d[0],
                        "volume": d[1],
                        "high": d[2],
                        "low": d[3],
                        "change_pct": d[4]
                    }
                
                completed_symbols += len(chunk)
                if progress_callback:
                    progress_callback(completed_symbols, total_symbols, f"Fetched live prices... ({completed_symbols}/{total_symbols})")
        except Exception as e:
            print("Error querying TradingView API:", e)
            
    # Bulk fetch 1m timestamps for all symbols via yfinance for EXACT "last traded time" per stock
    import yfinance as yf
    import pytz
    
    # The original timestamp fetching block was moved below the filtering loop.
    # This block is now removed as it's redundant and inefficient.
    
    tz = pytz.timezone('Asia/Kolkata')
    fallback_time = datetime.now(tz)
    
    for sym in base_symbols:
        bse_sym_mapped = sym.replace('&', '_')
        nse_key = f"NSE:{sym}"
        bse_key = f"BSE:{bse_sym_mapped}"
        
        if nse_key in tv_data and bse_key in tv_data:
            ns_info = tv_data[nse_key]
            bo_info = tv_data[bse_key]
            
            ns_price = ns_info['price']
            bo_price = bo_info['price']
            ns_vol = int(ns_info['volume']) if ns_info['volume'] else 0
            bo_vol = int(bo_info['volume']) if bo_info['volume'] else 0
            
            if ns_price == 0 or bo_price == 0:
                continue
                
            # Circuit check approximation
            ns_circuit = False
            # TradingView change_pct is already a relative percent (e.g., 5.0 for 5%)
            if (ns_price == ns_info['high'] and ns_info['change_pct'] >= 4.9) or \
               (ns_price == ns_info['low'] and ns_info['change_pct'] <= -4.9):
                ns_circuit = True
                
            bo_circuit = False
            if (bo_price == bo_info['high'] and bo_info['change_pct'] >= 4.9) or \
               (bo_price == bo_info['low'] and bo_info['change_pct'] <= -4.9):
                bo_circuit = True
                
            ns_circuit_str = "Yes" if ns_circuit else "No"
            bo_circuit_str = "Yes" if bo_circuit else "No"
            
            diff = abs(ns_price - bo_price)
            diff_pct = (diff / min(ns_price, bo_price)) * 100
            
            # Filter early to avoid unnecessary timestamp fetching
            if diff_pct < min_diff:
                continue
                
            higher_exchange = "NSE" if ns_price > bo_price else ("BSE" if bo_price > ns_price else "Equal")
            
            results.append({
                "Stock": sym,
                "NSE Circuit": ns_circuit_str,
                "BSE Circuit": bo_circuit_str,
                "NSE Price": round(ns_price, 2),
                "BSE Price": round(bo_price, 2),
                "Higher Exchange": higher_exchange,
                "Diff (₹)": round(diff, 2),
                "Diff (%)": round(diff_pct, 2),
                "NSE Volume": ns_vol,
                "BSE Volume": bo_vol
            })
            
    if progress_callback:
        progress_callback(total_symbols, total_symbols, f"Calculating Arbitrage... Filtered {len(results)} matches.")
            
    # Now that we filtered, fetch timestamps ONLY for the valid arbitrage symbols
    import yfinance as yf
    import pytz
    
    valid_symbols = [r["Stock"] for r in results]
    yf_tickers = [s + ".NS" for s in valid_symbols]
    timestamps = {}
    
    if valid_symbols:
        if progress_callback:
            progress_callback(total_symbols, total_symbols, f"Fetching exact timestamps for {len(valid_symbols)} matches...")
            
        try:
            df_hist = yf.download(yf_tickers, period="1d", interval="1m", progress=False, threads=True)
            if df_hist is not None and not df_hist.empty:
                if len(yf_tickers) == 1:
                    try:
                        last_idx = df_hist['Close'].dropna().index[-1]
                        timestamps[valid_symbols[0]] = last_idx
                    except:
                        pass
                else:
                    for sym, yf_sym in zip(valid_symbols, yf_tickers):
                        try:
                            if 'Close' in df_hist.columns:
                                series = df_hist['Close'][yf_sym].dropna()
                                if not series.empty:
                                    timestamps[sym] = series.index[-1]
                        except:
                            pass
        except Exception as e:
            print("Error fetching timestamps: ", e)
            
    tz = pytz.timezone('Asia/Kolkata')
    fallback_time = datetime.now(tz)
    
    for r in results:
        sym = r["Stock"]
        exact_tz = timestamps.get(sym, fallback_time)
        if exact_tz.tzinfo is None:
            exact_tz = tz.localize(exact_tz)
        else:
            exact_tz = exact_tz.astimezone(tz)
            
        r["Date"] = exact_tz.strftime('%Y-%m-%d')
        r["Time"] = exact_tz.strftime('%H:%M:%S')
            
    return pd.DataFrame(results)
