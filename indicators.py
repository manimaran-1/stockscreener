import pandas as pd
import numpy as np
import pandas_ta as ta

def calculate_dmi(df, length=14):
    """
    Calculates the Directional Movement Index (ADX, +DI, -DI) manually to exactly 
    match TradingView's Wilder Smoothing.
    """
    if df.empty or len(df) <= length:
        # Fill with NaNs just in case
        df['ADX'] = np.nan
        df['+DI'] = np.nan
        df['-DI'] = np.nan
        return df
        
    # Calculate True Range (TR)
    high = df['high']
    low = df['low']
    close = df['close']
    
    prev_close = close.shift(1)
    
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    
    # Calculate Directional Movement (+DM and -DM)
    up_move = high - high.shift(1)
    down_move = low.shift(1) - low
    
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    
    plus_dm = pd.Series(plus_dm, index=df.index)
    minus_dm = pd.Series(minus_dm, index=df.index)
    
    # Wilder's Smoothing (RMA) function
    def wma(series, length):
        # RMA is equivalent to EMA with alpha = 1/length
        wma_series = pd.Series(index=series.index, dtype=float)
        
        # First valid value is simple sum
        first_valid_idx = series.first_valid_index()
        if first_valid_idx is None:
            return wma_series
            
        first_idx = series.index.get_loc(first_valid_idx)
        if len(series) <= first_idx + length:
            return wma_series
            
        start_sum = series.iloc[first_idx:first_idx+length].sum()
        wma_series.iloc[first_idx+length-1] = start_sum
        
        # Iterative Wilder's Smoothing
        for i in range(first_idx+length, len(series)):
            val = wma_series.iloc[i-1] - (wma_series.iloc[i-1]/length) + series.iloc[i]
            wma_series.iloc[i] = val
            
        return wma_series
        
    # Smoothed TR, +DM, and -DM
    smooth_tr = wma(tr, length)
    smooth_plus_dm = wma(plus_dm, length)
    smooth_minus_dm = wma(minus_dm, length)
    
    # Calculate +DI and -DI
    plus_di = (smooth_plus_dm / smooth_tr) * 100
    minus_di = (smooth_minus_dm / smooth_tr) * 100
    
    # Calculate Directional Index (DX)
    dx = (plus_di - minus_di).abs() / (plus_di + minus_di) * 100
    
    # Calculate ADX (Smoothed DX)
    # The first ADX value is a simple moving average of DX, then Wilder smoothed.
    adx = pd.Series(index=df.index, dtype=float)
    
    first_valid_dx_idx = dx.first_valid_index()
    if first_valid_dx_idx is not None:
        first_dx = dx.index.get_loc(first_valid_dx_idx)
        if len(dx) > first_dx + length:
            start_adx = dx.iloc[first_dx:first_dx+length].mean()
            adx.iloc[first_dx+length-1] = start_adx
            
            for i in range(first_dx+length, len(dx)):
                val = (adx.iloc[i-1] * (length - 1) + dx.iloc[i]) / length
                adx.iloc[i] = val

    df['+DI'] = plus_di
    df['-DI'] = minus_di
    df['ADX'] = adx
    
    return df

def calculate_support_resistance(df):
    """
    Calculates Support (S1) and Resistance (R1) based on standard Pivot Points
    using the previous period's High, Low, and Close.
    
    Pivot (PP) = (High + Low + Close) / 3
    Resistance 1 (R1) = (2 * Pivot) - Low
    Support 1 (S1) = (2 * Pivot) - High
    """
    if df.empty or len(df) < 2:
        df['Pivot'] = np.nan
        df['R1'] = np.nan
        df['S1'] = np.nan
        return df
        
    # We calculate PP based on the PREVIOUS bar so the current bar has valid levels
    prev_high = df['high'].shift(1)
    prev_low = df['low'].shift(1)
    prev_close = df['close'].shift(1)
    
    pp = (prev_high + prev_low + prev_close) / 3
    r1 = (2 * pp) - prev_low
    s1 = (2 * pp) - prev_high
    
    df['Pivot'] = pp
    df['R1'] = r1
    df['S1'] = s1
    
    return df

def detect_dmi_crossovers(df):
    """
    Detects when +DI crosses above -DI (Buy Signal) and vice versa (Sell Signal).
    """
    if df.empty or '+DI' not in df.columns or '-DI' not in df.columns:
        df['Signal'] = 0
        df['Signal_Type'] = "None"
        return df
        
    # +DI > -DI
    bullish = df['+DI'] > df['-DI']
    
    # Previous state
    prev_bullish = bullish.shift(1).fillna(False).astype(bool)
    
    # Sell state
    bearish = df['-DI'] > df['+DI']
    prev_bearish = bearish.shift(1).fillna(False).astype(bool)
    
    # Buy Signal: Currently bullish, previously not bullish
    buy_signals = bullish & ~prev_bullish
    
    # Sell Signal: Currently bearish, previously not bearish
    sell_signals = bearish & ~prev_bearish
    
    df['Signal'] = 0
    df.loc[buy_signals, 'Signal'] = 1
    df.loc[sell_signals, 'Signal'] = -1
    
    df['Signal_Type'] = "None"
    df.loc[buy_signals, 'Signal_Type'] = "Buy"
    df.loc[sell_signals, 'Signal_Type'] = "Sell"
    
    return df

def apply_all_indicators(df, dmi_length=14):
    """
    Wrapper function to apply all technical indicators.
    """
    df = calculate_dmi(df, length=dmi_length)
    df = calculate_support_resistance(df)
    
    # Add Risk Indicators
    df['EMA21'] = calculate_ema(df, 21)
    df['ATR'] = calculate_atr(df, 14)
    bbl, bbu = calculate_bollinger_bands(df, 20, 2.0)
    df['BBL'] = bbl
    df['BBU'] = bbu
    
    df = detect_dmi_crossovers(df)
    
    return df

def calculate_dsmi(df, length=20, weak_thr=10, neutral_thr=35, strong_thr=45, overheat_thr=55, entry_level=20):
    """
    Calculates the Modified Directional Strength and Momentum Index (DSMI)
    from PineScript. Uses EMA smoothing instead of Wilder's RMA.
    """
    if df.empty or len(df) <= length:
        df['+DS'] = np.nan
        df['-DS'] = np.nan
        df['DSMI'] = np.nan
        df['Trend_Strength_Text'] = "N/A"
        df['DSMI_Signal'] = "None"
        return df

    high = df['high']
    low = df['low']
    close = df['close']
    open_p = df['open']
    
    # Base params
    candle_size = high - low
    
    # direction = close > open ? 1 : close < open ? -1 : 0
    direction = np.where(close > open_p, 1, np.where(close < open_p, -1, 0))
    
    # plusDM = direction > 0 ? candleSize : 0
    plus_dm = pd.Series(np.where(direction > 0, candle_size, 0.0), index=df.index)
    # minusDM = direction < 0 ? candleSize : 0
    minus_dm = pd.Series(np.where(direction < 0, candle_size, 0.0), index=df.index)
    candle_s = pd.Series(candle_size, index=df.index)
    
    # pine script ta.ema uses alpha = 2 / (length + 1)
    plus_dm_ema = plus_dm.ewm(span=length, adjust=False).mean()
    minus_dm_ema = minus_dm.ewm(span=length, adjust=False).mean()
    candle_ema = candle_s.ewm(span=length, adjust=False).mean()
    
    candle_ema_safe = np.where(candle_ema == 0, 1e-10, candle_ema)
    
    plus_ds = 100 * plus_dm_ema / candle_ema_safe
    minus_ds = 100 * minus_dm_ema / candle_ema_safe
    
    sum_ds = plus_ds + minus_ds
    dx = np.where(sum_ds == 0, 0.0, 100 * np.abs(plus_ds - minus_ds) / sum_ds)
    dx_series = pd.Series(dx, index=df.index)
    
    dsmi = dx_series.ewm(span=length, adjust=False).mean()
    is_bull = plus_ds > minus_ds
    
    df['+DS'] = plus_ds
    df['-DS'] = minus_ds
    df['DSMI'] = dsmi
    df['is_bull'] = is_bull
    
    # Trend Strength logic
    # strength = DSMI <= weak_thr ? 'WEAK' : DSMI <= neutral_thr ? 'MODERATE' : DSMI <= strong_thr ? 'STRONG' : DSMI <= overheat_thr ? 'OVERHEATED' : 'EXTREME'
    conditions = [
        dsmi <= weak_thr,
        dsmi <= neutral_thr,
        dsmi <= strong_thr,
        dsmi <= overheat_thr
    ]
    choices = ['WEAK', 'MODERATE', 'STRONG', 'OVERHEATED']
    df['Trend_Strength_Text'] = np.select(conditions, choices, default='EXTREME')
    
    # Simplified Crossover Logic: +DS crossing -DS
    # +DS > -DS
    bullish = plus_ds > minus_ds
    
    # Previous state
    prev_bullish = bullish.shift(1).fillna(False).astype(bool)
    
    # Sell state
    bearish = minus_ds > plus_ds
    prev_bearish = bearish.shift(1).fillna(False).astype(bool)
    
    # Buy Signal: Currently bullish, previously not bullish
    bull_entry = bullish & ~prev_bullish
    
    # Sell Signal: Currently bearish, previously not bearish
    bear_entry = bearish & ~prev_bearish
    
    df['DSMI_Signal'] = "None"
    df.loc[bull_entry, 'DSMI_Signal'] = "Buy"
    df.loc[bear_entry, 'DSMI_Signal'] = "Sell"
    
    return df

def apply_dsmi_indicators(df, length=20, weak_thr=10, neutral_thr=35, strong_thr=45, overheat_thr=55, entry_level=20):
    df = calculate_dsmi(df, length, weak_thr, neutral_thr, strong_thr, overheat_thr, entry_level)
    df = calculate_support_resistance(df)
    
    # Add Risk Indicators
    df['EMA21'] = calculate_ema(df, 21)
    df['ATR'] = calculate_atr(df, 14)
    bbl, bbu = calculate_bollinger_bands(df, 20, 2.0)
    df['BBL'] = bbl
    df['BBU'] = bbu
    
    return df

def calculate_ema(df, length):
    return ta.ema(df['close'], length=length)

def calculate_atr(df, length=14):
    return ta.atr(df['high'], df['low'], df['close'], length=length, mamode="rma")

def calculate_bollinger_bands(df, length=20, std_dev=2.0):
    bb = ta.bbands(df['close'], length=length, std=std_dev, mamode="sma", ddof=0)
    if bb is not None and not bb.empty:
        bbl_col = [c for c in bb.columns if c.startswith('BBL')][0]
        bbu_col = [c for c in bb.columns if c.startswith('BBU')][0]
        return bb[bbl_col], bb[bbu_col]
    return pd.Series(0, index=df.index), pd.Series(0, index=df.index)
