import pandas as pd
import numpy as np
import pandas_ta as ta

def calculate_ema(df, length):
    """
    Calculate Exponential Moving Average.
    """
    # Using pandas_ta for robust calculation
    return ta.ema(df['close'], length=length)

def calculate_stoch_rsi(df, length=14, rsi_length=14, k=3, d=3):
    """
    Calculate Stochastic RSI.
    """
    # Calculate RSI
    rsi = ta.rsi(df['close'], length=rsi_length)
    
    # Calculate Stoch RSI
    stoch_rsi_k = ta.stochrsi(df['close'], length=length, rsi_length=rsi_length, k=k, d=d)
    
    # pandas_ta returns a DataFrame with 'STOCHRSIk_...' and 'STOCHRSId_...' columns
    # We need the %K line
    if stoch_rsi_k is not None and not stoch_rsi_k.empty:
        return stoch_rsi_k.iloc[:, 0] # Return the K line
    return pd.Series([np.nan] * len(df), index=df.index)

def calculate_smi(df, length=10, smooth=3):
    """
    Calculate Stochastic Momentum Index matching Pine Script
    """
    # Get highest high and lowest low over length period
    hh = df['high'].rolling(window=length).max()
    ll = df['low'].rolling(window=length).min()
    
    # Calculate differences
    diff = hh - ll
    rdiff = df['close'] - (hh + ll) / 2
    
    # Apply double EMA smoothing
    avg_rdiff = rdiff.ewm(span=smooth, adjust=False).mean().ewm(span=smooth, adjust=False).mean()
    avg_diff = diff.ewm(span=smooth, adjust=False).mean().ewm(span=smooth, adjust=False).mean()
    
    # Calculate SMI
    # Handle division by zero
    smi = np.where(avg_diff != 0, 100 * avg_rdiff / (avg_diff / 2), 0)
    
    return pd.Series(smi, index=df.index)

def calculate_macd(df, fast=12, slow=26, signal=9):
    """
    Calculate MACD.
    """
    macd = ta.macd(df['close'], fast=fast, slow=slow, signal=signal)
    
    # pandas_ta returns MACD line, Histogram, and Signal line
    # We need the MACD line itself
    if macd is not None and not macd.empty:
        return macd.iloc[:, 0] # Return the MACD line
    return pd.Series([np.nan] * len(df), index=df.index)

def calculate_atr(df, length=14):
    return ta.atr(df['high'], df['low'], df['close'], length=length, mamode="rma")

def calculate_bollinger_bands(df, length=20, std_dev=2.0):
    bb = ta.bbands(df['close'], length=length, std=std_dev, mamode="sma", ddof=0)
    if bb is not None and not bb.empty:
        bbl_col = [c for c in bb.columns if c.startswith('BBL')][0]
        bbu_col = [c for c in bb.columns if c.startswith('BBU')][0]
        return bb[bbl_col], bb[bbu_col]
    return pd.Series(0, index=df.index), pd.Series(0, index=df.index)
