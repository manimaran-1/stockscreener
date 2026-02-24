import pandas as pd
import pandas_ta as ta

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

def calculate_bbands(df, length=20, std_dev=2.0):
    """
    Calculates Bollinger Bands.
    Returns Lower Band, Middle Band, Upper Band.
    """
    bbands = ta.bbands(df['close'], length=length, std=std_dev)
    
    if bbands is None or bbands.empty:
        return pd.Series(), pd.Series(), pd.Series()
        
    bb_lower = bbands.iloc[:, 0]
    bb_mid = bbands.iloc[:, 1]
    bb_upper = bbands.iloc[:, 2]
    
    return bb_lower, bb_mid, bb_upper

def apply_all_indicators(df, macd_fast=12, macd_slow=26, macd_signal=9, bb_length=20, bb_std=2.0):
    """
    Applies MACD and Bollinger Bands to the DataFrame.
    """
    try:
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
        
        # Calculate Bollinger Bands
        bb_lower, bb_mid, bb_upper = calculate_bbands(
            df, 
            length=bb_length, 
            std_dev=bb_std
        )
        
        df['BB_Lower'] = bb_lower
        df['BB_Mid'] = bb_mid
        df['BB_Upper'] = bb_upper
        
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
