import pandas as pd
import pandas_ta as ta

def calculate_rsi(df, length=14):
    """Calculates Relative Strength Index (RSI)."""
    return ta.rsi(df['close'], length=length)

def calculate_keltner_channels(df, length=20, atr_length=10, multiplier=2.0, bands_style="True Range"):
    """
    Calculates Keltner Channels identical to TradingView default:
    Middle Line = EMA(close, length)
    If Bands Style == "True Range" (TV Default): Band Distance = RMA(True Range, length) * mult
    If Bands Style == "Average True Range": Band Distance = ATR(atr_length) * mult
    """
    ema = ta.ema(df['close'], length=length)
    
    # In pandas_ta, ATR uses Wilder's Moving Average (RMA) by default.
    # Therefore, ta.atr(length) is exactly equivalent to TV's ta.rma(ta.tr, length)
    if bands_style == "True Range":
        range_ma = ta.atr(df['high'], df['low'], df['close'], length=length)
    else:
        # Average True Range Option
        range_ma = ta.atr(df['high'], df['low'], df['close'], length=atr_length)
    
    if ema is None or range_ma is None:
        return pd.Series(), pd.Series(), pd.Series()
        
    upper = ema + (multiplier * range_ma)
    lower = ema - (multiplier * range_ma)
    
    return ema, upper, lower

def apply_all_indicators(df, rsi_length=14, kc_length=20, kc_atr_length=10, kc_mult=2.0, kc_bands_style="True Range"):
    """
    Applies RSI and Keltner Channels to the DataFrame.
    """
    try:
        # Calculate RSI
        rsi = calculate_rsi(df, length=rsi_length)
        df['RSI'] = rsi
        
        # Calculate Keltner Channels
        ema, upper, lower = calculate_keltner_channels(
            df, 
            length=kc_length, 
            atr_length=kc_atr_length, 
            multiplier=kc_mult,
            bands_style=kc_bands_style
        )
        
        df['KC_Middle'] = ema
        df['KC_Upper'] = upper
        df['KC_Lower'] = lower
        
        # Additional context standard features
        df['EMA21'] = ta.ema(df['close'], length=21)
        if df['EMA21'] is not None:
            df['Trend'] = "Bullish"
            df.loc[df['close'] < df['EMA21'], 'Trend'] = "Bearish"
        else:
            df['Trend'] = "Neutral"
            
        df['ATR'] = ta.atr(df['high'], df['low'], df['close'], length=14)
        
    except Exception as e:
        print(f"Error applying indicators: {e}")
        
    return df
