
import time
import pandas as pd
from datetime import datetime, timedelta
from vnstock.api.quote import Quote
# VN30 index constituents
VN30 = [
    "ACB", "BCM", "BID", "BVH", "CTG", "FPT", "GAS", "GVR", "HDB", "HPG",
    "MBB", "MSN", "MWG", "PLX", "POW", "SAB", "SHB", "SSB", "SSI", "STB",
    "TCB", "TPB", "VCB", "VHM", "VIB", "VIC", "VJC", "VNM", "VPB", "VRE"
]

# Date range: last 10 years
END_DATE   = datetime.today().strftime("%Y-%m-%d")
START_DATE = (datetime.today() - timedelta(days=10 * 365)).strftime("%Y-%m-%d")


def get_stock_data():
    """Fetch historical price data for all 30 VN30 tickers with Audit columns."""
    stock_data = {}

    for i, ticker in enumerate(VN30):
        print(f"[{i+1}/{len(VN30)}] Fetching: {ticker}...")
        try:
            df = Quote(symbol=ticker, source='VCI').history(
                start=START_DATE,
                end=END_DATE,
                interval='1D'
            )
            if not df.empty:
                #  Add Audit Columns 
                df['ticker']      = ticker
                df['ingested_at'] = datetime.now()
                df['source_name'] = 'vnstock_api'
                
                stock_data[ticker] = df
                print(f"  -> {len(df)} rows")
            else:
                print(f"  -> No data available")

        except Exception as e:
            print(f"  -> Error: {e}")

        # Sleep 3s to stay under the 20 req/min rate limit (skip last ticker)
        if i < len(VN30) - 1:
            time.sleep(3)

    return stock_data