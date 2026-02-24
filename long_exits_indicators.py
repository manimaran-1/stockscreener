import pandas as pd
import pandas_ta as ta

def calculate_mas(df, short_len=9, long_len=21):
    """
    Calculates Short and Long Term Moving Averages.
    """
    ma_short = ta.sma(df['close'], length=short_len)
    ma_long = ta.sma(df['close'], length=long_len)
    
    return ma_short, ma_long

def apply_all_indicators(df, ma_short_len=9, ma_long_len=21):
    """
    Applies Moving Averages to the DataFrame.
    """
    try:
        # Calculate MAs
        ma_short, ma_long = calculate_mas(df, short_len=ma_short_len, long_len=ma_long_len)
        df['MA_Short'] = ma_short
        df['MA_Long'] = ma_long
        
        # Calculate recent Swing Low for Break of Structure (e.g. 10 period lowest low)
        df['Swing_Low_10'] = df['low'].rolling(window=10).min().shift(1)
        
        # Standard EMA21 and ATR14 for context
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
        if 'EMA21' in df.columns:
            df.loc[df['close'] > df['EMA21'], 'Trend'] = "Bullish"
            df.loc[df['close'] < df['EMA21'], 'Trend'] = "Bearish"

    except Exception as e:
        print(f"Error applying indicators: {e}")
        
    return df
