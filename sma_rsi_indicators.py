import pandas as pd
import numpy as np

def calculate_sma(df, column='close', length=9):
    """Calculates Simple Moving Average."""
    if df is None or df.empty or len(df) < length:
        return pd.Series(index=df.index if df is not None else [], dtype='float64')
    return df[column].rolling(window=length).mean()

def calculate_rsi(df, column='close', length=14):
    """
    Calculates the Relative Strength Index (RSI).
    """
    if df is None or df.empty or len(df) < length + 1:
        return pd.Series(index=df.index if df is not None else [], dtype='float64')

    delta = df[column].diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)

    # Use standard Wilder's moving average (EMA with alpha=1/length)
    ema_up = up.ewm(com=length - 1, adjust=False).mean()
    ema_down = down.ewm(com=length - 1, adjust=False).mean()

    rs = ema_up / ema_down
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_atr(df, length=14):
    """Calculates True Range and Average True Range (ATR)."""
    if df is None or df.empty or len(df) < 1:
        return pd.Series(index=df.index if df is not None else [], dtype='float64')
        
    df = df.copy()
    df['prev_close'] = df['close'].shift(1)
    
    # True Range components
    df['tr1'] = df['high'] - df['low']
    df['tr2'] = (df['high'] - df['prev_close']).abs()
    df['tr3'] = (df['low'] - df['prev_close']).abs()
    
    df['TR'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
    
    # ATR using Wilder's Smoothing (usually approximated with RMA or EMA)
    atr = df['TR'].ewm(alpha=1/length, adjust=False).mean()
    return atr
    
def calculate_ema(df, column='close', length=21):
    """Calculates Exponential Moving Average."""
    if df is None or df.empty or len(df) < length:
        return pd.Series(index=df.index if df is not None else [], dtype='float64')
    return df[column].ewm(span=length, adjust=False).mean()

def apply_all_indicators(df, sma_fast=9, sma_slow=21, rsi_length=14):
    """
    Applies 9 SMA, 21 SMA, and RSI to the dataframe.
    Also adds common utilities like ATR and EMA 21 for Stop Loss calculations.
    """
    if df is None or df.empty:
        return df
        
    df = df.copy()
    
    # Strategy Indicators
    df['SMA_Fast'] = calculate_sma(df, length=sma_fast)
    df['SMA_Slow'] = calculate_sma(df, length=sma_slow)
    df['RSI'] = calculate_rsi(df, length=rsi_length)
    
    # Utility/Risk Indicators (Standard match with VWMA page)
    df['ATR'] = calculate_atr(df, length=14)
    df['EMA21'] = calculate_ema(df, length=21)
    
    # Strategy Analysis values
    # We want to know if RSI is sloping UP or DOWN
    df['RSI_Prev'] = df['RSI'].shift(1)
    
    return df
