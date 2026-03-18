# Source Generated with Decompyle++
# File: app.cpython-312.pyc (Python 3.12)

import streamlit as st
import subprocess
import sys
st.set_page_config(page_title = 'DMI Scanner Hub', page_icon = '📈', layout = 'wide')
PACKAGES_TO_UPGRADE = [
    'yfinance',
    'pandas',
    'pandas_ta',
    'requests',
    'pytz',
    'numpy']

def _run_upgrade():
    results = { }
    for pkg in PACKAGES_TO_UPGRADE:
        proc = subprocess.run([
            sys.executable,
            '-m',
            'pip',
            'install',
            '--upgrade',
            '--quiet',
            pkg], capture_output = True, text = True, timeout = 60)
        results[pkg] = '✅ Updated' if proc.returncode == 0 else f'''⚠️ {proc.stderr.strip()[:80]}'''
    return results
# WARNING: Decompyle incomplete

# WARNING: Decompyle incomplete
