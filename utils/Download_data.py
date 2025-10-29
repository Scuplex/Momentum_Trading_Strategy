import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
import time
from .config import PATH

def download_single_ticker(ticker: str, days_back: int = 400, max_retries: int = 3) -> float:
    """
    Download price data for a single ticker and calculate gross return.
    
    Args:
        ticker (str): The ticker symbol to download
        days_back (int): Number of days back to calculate return
        max_retries (int): Maximum number of retry attempts for network errors
        
    Returns:
        float: Gross return (current_price / past_price - 1), or 0.0 if failed
    """
    today = datetime.today()
    past_date = today - timedelta(days=days_back)
    
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                print(f"Downloading data for {ticker}... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(2)  # Wait 2 seconds between retries
            else:
                print(f"Downloading data for {ticker}...")
            
            data = yf.download(ticker, start=past_date, end=today, auto_adjust=True, progress=False)
            
            if not data.empty and 'Close' in data.columns:
                close_prices = data['Close'].dropna()
                
                if len(close_prices) >= 2:
                    # Get the first (oldest) and last (newest) prices
                    past_price = close_prices.iloc[0].values[0]
                    current_price = close_prices.iloc[-1].values[0]
                    
                    # Calculate gross return
                    gross_return = (current_price / past_price) - 1
                    
                    print(f"✅ {ticker}: Past price: ${past_price:.2f}, Current price: ${current_price:.2f}, Return: {gross_return:.4f}")
                    return gross_return
                else:
                    print(f"❌ Insufficient data for {ticker} (need at least 2 data points)")
                    return 0.0
            else:
                print(f"❌ No Close data available for {ticker}")
                return 0.0
                
        except Exception as e:
            error_msg = str(e).lower()
            is_network_error = any(keyword in error_msg for keyword in [
                'curl', 'connection', 'timeout', 'network', 'ssl', 'http', 'request', 'unauthorized', 'crumb'
            ])
            
            if is_network_error and attempt < max_retries - 1:
                wait_time = (attempt + 1) * 3  # Increasing wait time: 3s, 6s, 9s
                print(f"⚠️  Network error for {ticker} (attempt {attempt + 1}/{max_retries}): {e}")
                print(f"⏳ Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
                continue
            else:
                print(f"❌ Error downloading {ticker}: {e}")
                return 0.0
    
    return 0.0