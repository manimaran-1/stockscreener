import pandas as pd
import pandas_ta as ta

def get_chop_color(chop_val):
    if pd.isna(chop_val):
        return "Unknown"
    if chop_val > 61.8:
        return "Cyan (Choppy)"
    elif chop_val >= 50:
        return "Green (Mild Choppy)"
    elif chop_val >= 38.2:
        return "Yellow (Trending)"
    else:
        return "Red (Strong Trend)"

def apply_all_indicators(df, chop_length=14):
    """
    Applies Chop Zone to the DataFrame.
    """
    try:
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
