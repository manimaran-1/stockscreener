import pandas as pd
import pandas_ta as ta

def calculate_adx(df, length=14):
    """Calculates Average Directional Index (ADX)."""
    adx_df = ta.adx(df['high'], df['low'], df['close'], length=length)
    if adx_df is None or adx_df.empty:
        return pd.Series(), pd.Series(), pd.Series()
    
    # adx_df has columns: ADX_14, DMP_14, DMN_14
    adx = adx_df.iloc[:, 0]
    di_plus = adx_df.iloc[:, 1]
    di_minus = adx_df.iloc[:, 2]
    
    return adx, di_plus, di_minus

def calculate_psar(df, af0=0.02, af=0.02, max_af=0.2):
    """Calculates Parabolic SAR."""
    psar_df = ta.psar(df['high'], df['low'], df['close'], af0=af0, af=af, max_af=max_af)
    
    if psar_df is None or psar_df.empty:
        return pd.Series(), pd.Series()
        
    # Columns typically: PSARl_0.02_0.2 (long), PSARs_0.02_0.2 (short), PSARaf, PSARr
    # To determine direction, check which one is non-null. 
    # Or use the combined PSAR column if available. Parabolic SAR returns multiple cols in pandas_ta.
    # Usually: PSARdir is returned as well. Let's extract the actual PSAR value and direction.
    
    # Find the main PSAR value column and direction column
    psar_val = pd.Series(index=df.index, dtype='float64')
    psar_dir = pd.Series(index=df.index, dtype='int') # 1 for long (below), -1 for short (above)
    
    long_col = [c for c in psar_df.columns if c.startswith('PSARl_')]
    short_col = [c for c in psar_df.columns if c.startswith('PSARs_')]
    dir_col = [c for c in psar_df.columns if c.startswith('PSARd_')]
    
    if long_col and short_col:
        # Combine them: if short_col is not nan, it's above. if long_col is not nan, it's below.
        psar_val = psar_df[long_col[0]].fillna(psar_df[short_col[0]])
        
        # Determine direction: 1 if below (long), -1 if above (short)
        psar_dir = pd.Series(-1, index=df.index)
        psar_dir.loc[psar_df[long_col[0]].notna()] = 1
    
    if dir_col:
         psar_dir = psar_df[dir_col[0]] # some versions return PSARd
         
    return psar_val, psar_dir

def apply_all_indicators(df, adx_length=14, psar_af=0.02, psar_max_af=0.2):
    """
    Applies ADX and PSAR to the DataFrame.
    """
    try:
        # Calculate ADX
        adx, di_plus, di_minus = calculate_adx(df, length=adx_length)
        df['ADX'] = adx
        df['DI_Plus'] = di_plus
        df['DI_Minus'] = di_minus
        
        # Calculate PSAR
        psar_val, psar_dir = calculate_psar(df, af0=psar_af, af=psar_af, max_af=psar_max_af)
        df['PSAR'] = psar_val
        df['PSAR_Dir'] = psar_dir # 1 when below candle (long), -1 when above candle (short)
        
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
