import pandas as pd
import pandas_ta as ta

def calculate_vwma(df, length=20):
    """Calculates Volume Weighted Moving Average (VWMA)."""
    vwma = ta.vwma(df['close'], df['volume'], length=length)
    
    # Fallback for stocks/indices without volume
    if vwma is None or vwma.isna().all():
        vwma = ta.sma(df['close'], length=length)
        
    return vwma

def calculate_macd(df, fast=12, slow=26, signal=9):
    """
    Calculates MACD. 
    Returns MACD line, Signal line, and Histogram.
    """
    macd = ta.macd(df['close'], fast=fast, slow=slow, signal=signal)
    
    if macd is None or macd.empty:
        return pd.Series(), pd.Series(), pd.Series()
        
    macd_line = macd.iloc[:, 0]
    macd_hist = macd.iloc[:, 1]
    macd_signal = macd.iloc[:, 2]
    
    return macd_line, macd_signal, macd_hist

def apply_all_indicators(df, vwma_length=20, macd_fast=12, macd_slow=26, macd_signal=9):
    """
    Applies VWMA and MACD to the DataFrame.
    """
    try:
        # Calculate VWMA
        vwma = calculate_vwma(df, length=vwma_length)
        df['VWMA'] = vwma
        
        # Calculate MACD
        macd_line, signal_line, macd_hist = calculate_macd(
            df, 
            fast=macd_fast, 
            slow=macd_slow, 
            signal=macd_signal
        )
        
        df['MACD_Line'] = macd_line
        df['MACD_Signal'] = signal_line
        df['MACD_Hist'] = macd_hist
        
        # Standard EMA21 and ATR14 for Trend/Volatility context and SL/TP
        if len(df) > 21:
            df['EMA21'] = ta.ema(df['close'], length=21)
        else:
            df['EMA21'] = pd.Series(dtype='float64')
            
        if len(df) > 14:
            df['ATR'] = ta.atr(df['high'], df['low'], df['close'], length=14)
        else:
            df['ATR'] = pd.Series(dtype='float64')
        
        # Determine long-term trend based on EMA21
        df['Trend'] = "Neutral"
        df.loc[df['close'] > df['EMA21'], 'Trend'] = "Bullish"
        df.loc[df['close'] < df['EMA21'], 'Trend'] = "Bearish"

    except Exception as e:
        print(f"Error applying indicators: {e}")
        
    return df
