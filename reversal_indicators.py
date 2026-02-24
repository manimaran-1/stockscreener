import pandas as pd
import numpy as np
import pandas_ta as ta

def calculate_ema(df, length):
    return ta.ema(df['close'], length=length)

def calculate_atr(df, length):
    return ta.atr(df['high'], df['low'], df['close'], length=length, mamode="rma")

def calculate_bollinger_bands(df, length=20, std_dev=2.0):
    """
    Calculates standard Bollinger Bands using pandas_ta with TradingView exact math (ddof=0).
    """
    bb = ta.bbands(df['close'], length=length, std=std_dev, mamode="sma", ddof=0)
    if bb is not None and not bb.empty:
        bbl_col = [c for c in bb.columns if c.startswith('BBL')][0]
        bbu_col = [c for c in bb.columns if c.startswith('BBU')][0]
        return bb[bbl_col], bb[bbu_col]
    return pd.Series(0, index=df.index), pd.Series(0, index=df.index)

def get_sensitivity_settings(preset, is_custom=False, custom_settings=None):
    """
    Returns ATR Multiplier and Percent Threshold based on preset.
    """
    if is_custom and custom_settings:
        return custom_settings['atr_mult'], custom_settings['pct_threshold']
        
    presets = {
        "Very High": (0.8, 0.005), # 0.5%
        "High": (1.2, 0.008),      # 0.8%
        "Medium": (2.0, 0.01),     # 1.0%
        "Low": (2.8, 0.015),       # 1.5%
        "Very Low": (3.5, 0.02)    # 2.0%
    }
    return presets.get(preset, (2.0, 0.01))

def calculate_reversal_v3(df, sensitivity="Medium", calculation_method="average", 
                          confirmation_bars=0, is_custom=False, custom_settings=None):
    """
    Reversal Detection Pro v3.0 Logic
    """
    if df.empty or len(df) < 50:
        return pd.DataFrame()

    # 1. Sensitivity Settings
    atr_mult, pct_threshold = get_sensitivity_settings(sensitivity, is_custom, custom_settings)
    
    # 2. ATR Calculation
    atr_length = custom_settings['atr_length'] if is_custom and custom_settings else 5
    atr_val = calculate_atr(df, atr_length)
    
    # 3. Reversal Amount Calculation
    close = df['close']
    
    # Max of (Price * %, Fixed Amount, ATR * Mult)
    # Fixed amount default from script is 0.05 (usually for futures, but keeping logic)
    fixed_amount = custom_settings['fixed_reversal'] if is_custom and custom_settings else 0.05
    
    reversal_amount = pd.concat([
        close * (pct_threshold / 100), 
        pd.Series([fixed_amount] * len(df), index=df.index),
        atr_val * atr_mult
    ], axis=1).max(axis=1)

    # 4. Triple EMA Trend
    ema9 = calculate_ema(df, 9)
    ema14 = calculate_ema(df, 14)
    ema21 = calculate_ema(df, 21)
    
    # 5. ZigZag Preparation (Average vs High/Low)
    avg_length = custom_settings['avg_length'] if is_custom and custom_settings else 5
    
    if calculation_method == "average":
        # EMA of High and Low
        priceh = ta.ema(df['high'], length=avg_length)
        pricel = ta.ema(df['low'], length=avg_length)
    else:
        priceh = df['high']
        pricel = df['low']
        
    # Pre-calculate shifted arrays for faster access
    priceh_vals = priceh.fillna(0).values
    pricel_vals = pricel.fillna(0).values
    rev_amt_vals = reversal_amount.fillna(0).values
    
    n = len(df)
    
    # State Variables
    zhigh = priceh_vals[0]
    zlow = pricel_vals[0]
    
    # Tracking Indices for Pivot Time
    zhigh_idx = 0
    zlow_idx = 0
    
    direction = 1 # 1 = Up (Looking for High), -1 = Down (Looking for Low)
    
    # Output Arrays
    signals = np.zeros(n) # 1 = Bull, -1 = Bear
    signal_prices = np.zeros(n) # Price at which reversal happened (Pivot)
    pivot_times = np.full(n, None) # Timestamps of the pivot
    
    # Iterate
    # Start slightly later to allow indicators to stabilize
    start_idx = max(21, confirmation_bars) 
    
    for i in range(start_idx, n):
        # Current Confirmed Prices (Effective "Current" for logic due to lag)
        curr_ph = priceh_vals[i - confirmation_bars]
        curr_pl = pricel_vals[i - confirmation_bars]
        curr_rev = rev_amt_vals[i] 
        
        # ZigZag Logic
        if direction == 1: # Uptrend, updating High
            if curr_ph > zhigh:
                zhigh = curr_ph
                zhigh_idx = i - confirmation_bars
            
            # Check Reversal to Down
            if (zhigh - curr_pl) >= curr_rev:
                # Pivot High Confirmed
                # We found a High, now switch to looking for Low
                direction = -1
                zlow = curr_pl
                zlow_idx = i - confirmation_bars
                
                # Signal Generation
                signals[i] = -1
                signal_prices[i] = zhigh
                pivot_times[i] = df.index[zhigh_idx]

        elif direction == -1: # Downtrend, updating Low
            if curr_pl < zlow:
                zlow = curr_pl
                zlow_idx = i - confirmation_bars
                
            # Check Reversal to Up
            if (curr_ph - zlow) >= curr_rev:
                # Pivot Low Confirmed
                # We found a Low, now switch to looking for High
                direction = 1
                zhigh = curr_ph
                zhigh_idx = i - confirmation_bars
                
                # Signal Generation
                signals[i] = 1
                signal_prices[i] = zlow
                pivot_times[i] = df.index[zlow_idx]
    
    # 6. Trend State
    # Bull: 9>14>21
    # Bear: 9<14<21
    trend_vals = []
    e9 = ema9.values
    e14 = ema14.values
    e21 = ema21.values
    
    for i in range(n):
        if e9[i] > e14[i] and e14[i] > e21[i]:
            trend_vals.append("Bullish")
        elif e9[i] < e14[i] and e14[i] < e21[i]:
            trend_vals.append("Bearish")
        else:
            trend_vals.append("Neutral")

    # 7. Add Standard ATR and Bollinger Bands for the App
    atr_14 = calculate_atr(df, 14)
    bbl, bbu = calculate_bollinger_bands(df, 20, 2.0)

    # Construct Result DataFrame
    result_df = df.copy()
    result_df['Signal'] = signals
    result_df['Signal_Price'] = signal_prices
    result_df['Pivot_Time'] = pivot_times
    result_df['Trend'] = trend_vals
    result_df['EMA9'] = ema9
    result_df['EMA14'] = ema14
    result_df['EMA21'] = ema21
    result_df['ATR'] = atr_14
    result_df['BBL'] = bbl
    result_df['BBU'] = bbu
    
    return result_df
