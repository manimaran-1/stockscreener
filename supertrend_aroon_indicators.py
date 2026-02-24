import pandas as pd
import pandas_ta as ta

def calculate_supertrend(df, length=10, multiplier=3.0):
    """
    Calculates Supertrend.
    """
    st = ta.supertrend(df['high'], df['low'], df['close'], length=length, multiplier=multiplier)
    
    if st is None or st.empty:
        return pd.Series(), pd.Series(), pd.Series(), pd.Series()
        
    st_line = st.iloc[:, 0]
    st_trend = st.iloc[:, 1] # 1 for bullish, -1 for bearish in pandas_ta
    st_lower = st.iloc[:, 2]
    st_upper = st.iloc[:, 3]
    
    return st_line, st_trend, st_lower, st_upper

def calculate_aroon(df, length=14):
    """
    Calculates Aroon Indicator.
    Returns Aroon Down, Aroon Up, Aroon Oscillator
    """
    aroon_df = ta.aroon(df['high'], df['low'], length=length)
    if aroon_df is None or aroon_df.empty:
        return pd.Series(), pd.Series(), pd.Series()
        
    aroon_down = aroon_df.iloc[:, 0]
    aroon_up = aroon_df.iloc[:, 1]
    aroon_osc = aroon_df.iloc[:, 2]
    
    return aroon_down, aroon_up, aroon_osc

def apply_all_indicators(df, supertrend_length=10, supertrend_multiplier=3.0, aroon_length=14):
    """
    Applies Supertrend and Aroon to the DataFrame.
    """
    try:
        # Calculate Supertrend
        st_line, st_trend, st_lower, st_upper = calculate_supertrend(
            df, 
            length=supertrend_length, 
            multiplier=supertrend_multiplier
        )
        
        df['Supertrend'] = st_line
        df['Supertrend_Direction'] = st_trend # 1 or -1
        
        # Calculate Aroon
        aroon_down, aroon_up, aroon_osc = calculate_aroon(df, length=aroon_length)
        df['Aroon_Down'] = aroon_down
        df['Aroon_Up'] = aroon_up
        df['Aroon_Osc'] = aroon_osc
        
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
