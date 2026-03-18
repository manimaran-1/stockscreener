import streamlit as st
import subprocess
import sys
st.set_page_config(
    page_title="DMI Scanner Hub",
    page_icon="📈",
    layout="wide"
)

# --- SECURITY & UI CONFIG ---
hide_st_style = '''
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}
</style>
'''
st.markdown(hide_st_style, unsafe_allow_html=True)
import hmac

# Secure password check using st.secrets
if "password_correct" not in st.session_state:
    st.session_state.password_correct = False

def check_password():
    if st.secrets.get("password"):
        if st.session_state.password_correct:
            return True
        st.markdown("""
            <h1 style='text-align: center; margin-top: 50px;'>🔐 Secure Access</h1>
        """, unsafe_allow_html=True)
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            with st.form("login_form"):
                password = st.text_input("Password", type="password", key="login_password")
                submit = st.form_submit_button("Login", use_container_width=True)
            if submit:
                # Convert the secret to string to match input reliably
                if password == str(st.secrets.get("password", "")):
                    st.session_state.password_correct = True
                    st.rerun()
                else:
                    st.error("❌ Incorrect password")
        return False
    else:
        st.error("Secrets not configured. Please set password in Streamlit Cloud Advanced Settings.")
        return False

if not check_password():
    st.stop()

# -----------------------------

# ─── Manual package updater (sidebar button) ─────────────────────────────────
PACKAGES_TO_UPGRADE = [
    "yfinance",
    "pandas",
    "requests",
    "pytz",
    # numpy and numba must stay in sync — pandas-ta 0.4.71b0 requires:
    #   numpy>=2.2.6 (NOT 2.3+, breaks numba) and numba==0.61.2 exactly
    "numpy>=2.2.6,<2.3",
    "numba==0.61.2",
    "pandas_ta",    # install last so it picks up correct numpy/numba
]
def _run_upgrade():
    results = {}
    for pkg in PACKAGES_TO_UPGRADE:
        try:
            proc = subprocess.run(
                [sys.executable, "-m", "pip", "install", "--upgrade", "--quiet", pkg],
                capture_output=True, text=True, timeout=60
            )
            results[pkg] = "✅ Updated" if proc.returncode == 0 else f"⚠️ {proc.stderr.strip()[:80]}"
        except subprocess.TimeoutExpired:
            results[pkg] = "⏱️ Timeout"
        except Exception as e:
            results[pkg] = f"❌ {str(e)[:60]}"
    return results
with st.sidebar.expander("📦 Package Updater", expanded=False):
    st.write("Click to upgrade all scanner dependencies to their latest versions.")
    if st.button("🔄 Update All Packages", key="pkg_update_btn"):
        with st.spinner("Upgrading packages..."):
            results = _run_upgrade()
        for pkg, status in results.items():
            st.write(f"**{pkg}**: {status}")
        st.success("Done! Restart the app to use updated packages.")
# ─────────────────────────────────────────────────────────────────────────────
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
### 16. Pivot Points & Stochastic RSI Scanner
A highly effective pullback momentum scanner. It targets assets breaking above the first Resistance (R1) pivot level confirmed by a bullish `%K / %D` crossover on the Stochastic RSI.
### 17. OBV & Hull Moving Average Scanner
A dual-directional trend scanner. It generates Long signals when the price closes above the Hull Moving Average (HMA) while On-Balance Volume (OBV) is rising, and Short signals when it closes below the HMA while OBV is falling.
### 18. Vortex Indicator & Alligator Scanner
A robust momentum breakout scanner. It enters longs when the Alligator's Green line (Lips) crosses above the Red (Teeth) and Blue (Jaw) lines, simultaneously confirmed by a bullish Vortex Indicator (`+VI > -VI`) crossover.
""")
st.info("👈 Select a page from the sidebar to begin scanning.")
