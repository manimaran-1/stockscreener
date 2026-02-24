import pandas as pd
import pandas_ta as ta

def calculate_fib_levels(df, lookback=50):
    """
    Calculates dynamic 50% and 61.8% Fibonacci retracement levels based on a rolling lookback window.
    """
    roll_high = df['high'].rolling(window=lookback).max()
    roll_low = df['low'].rolling(window=lookback).min()
    
    diff = roll_high - roll_low
    
    fib_50 = roll_low + (diff * 0.50)
    fib_618 = roll_low + (diff * 0.618)
    fib_382 = roll_low + (diff * 0.382)
    
    return roll_high, roll_low, fib_50, fib_618, fib_382

def apply_all_indicators(df, fib_lookback=50, chop_length=14):
    """
    Applies Fibonacci levels and Chop Zone to the DataFrame.
    """
    try:
        # Calculate Fib Levels
        roll_high, roll_low, fib_50, fib_618, fib_382 = calculate_fib_levels(df, lookback=fib_lookback)
        df['Swing_High'] = roll_high
        df['Swing_Low'] = roll_low
        df['Fib_50'] = fib_50
        df['Fib_618'] = fib_618
        df['Fib_382'] = fib_382
        
        # Calculate Chop Zone
        chop = ta.chop(df['high'], df['low'], df['close'], length=chop_length)
        if chop is None or chop.isna().all():
            chop = pd.Series(50.0, index=df.index) # Default to neutral chop
        df['Chop'] = chop
        
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
        if 'EMA21' in df.columns:
            df.loc[df['close'] > df['EMA21'], 'Trend'] = "Bullish"
            df.loc[df['close'] < df['EMA21'], 'Trend'] = "Bearish"

    except Exception as e:
        print(f"Error applying indicators: {e}")
        
    return df
