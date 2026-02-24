import streamlit as st

st.set_page_config(
    page_title="DMI Scanner Hub",
    page_icon="📈",
    layout="wide"
)

st.title("📈 DMI Scanner Hub")

st.markdown("""
Welcome to the unified Stock Scanner Dashboard. Please select a scanner from the sidebar.

### 1. Standard DMI Scanner
The original scanner focusing on Directional Movement Index (+DI / -DI) crossovers. Includes volume filtering and various market statistics (Top Gainers, Most Active, etc.).

### 2. Modified DSMI Scanner 
An enhanced version utilizing a proprietary Directional Strength and Momentum Index (DSMI) calculation heavily smoothed via exponential moving averages (EMA).

### 3. Squeeze Momentum Pro
A specialized scanner combining Bollinger Bands and Keltner Channels to identify periods of low volatility (the "squeeze") right before explosive momentum breakouts, utilizing linear regression for direction.

### 4. Advanced Screener
A highly aggressive pattern-matching scanner finding combinations of ADX trend strength, EMA positioning, and RSI momentum.

### 5. Reversal Pro v3 Scanner
A specialized non-repainting reversal detection scanner. It maps deep oversold conditions mixed with V-shape Pivot strength logic to find the literal bottom of downtrends.

### 6. Keltner + RSI Scanner
An isolated mean-reversion scanner finding extreme price stretches. It identifies entry opportunities when assets pierce the Keltner Channels concurrently with RSI reaching extreme oversold (<= 30) or overbought (>= 70) territory.

### 7. VWMA + MACD Scanner
A standalone Trend-Following momentum breakout strategy. It targets heavy moving volume via the Volume Weighted Moving Average (VWMA) mapped against MACD crossovers.

### 8. BB + MACD Scanner
A reversal and momentum strategy targeting turning points by identifying momentum crossovers (MACD) occurring precisely at extreme volatility boundaries (Bollinger Bands).

### 9. SMA + RSI Scanner
A trend-following strategy that looks for entries when the price is above the Simple Moving Average (SMA), accompanied by a bullish momentum shift on the RSI.

### 10. OBV & Supertrend Scanner
A volume-backed momentum strategy that triggers when the Supertrend turns Green (bullish) simultaneously with rising On-Balance Volume (OBV).

### 11. ADX Indicator & Parabolic SAR Scanner
A strong trending strategy that enters exclusively when the Average Directional Index (ADX) crosses above the 25 threshold, supported by Parabolic SAR dots flipping below the price.

### 12. Long Exits Scanner
A defensive scanner strictly focused on identifying optimal exit points for long positions, utilizing Price vs Moving Average closures and Short/Long MA crossovers.

### 13. Fibonacci & Chop Zone Scanner
A pullback strategy designed to enter strongly trending markets (indicated by a Red Chop Zone) when the price perfectly retraces to the 50% or 61.8% Fibonacci levels.

### 14. Supertrend & Aroon Scanner
A dual-confirmation trend strategy that generates signals when the price flips the Supertrend green while simultaneously having the Aroon Up indicator cross above the Aroon Down.

### 15. Standalone Chop Zone Filter
An isolated visual mapping scanner that filters the entire market based exclusively on current market momentum state (Red: Trending, Yellow: Mild Trend, Green: Mild Chop, Cyan: Heavy Chop).
""")

st.info("👈 Select a page from the sidebar to begin scanning.")
