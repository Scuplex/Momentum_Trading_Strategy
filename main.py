import pandas as pd
import numpy as np
import yfinance as yf
import requests
import time
from datetime import datetime, timedelta
from utils.config import PATH
from utils.Download_data import download_single_ticker

# First we download the latest Tickers
url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt"
response = requests.get(url)
tickers = response.text.splitlines()
df = pd.DataFrame(tickers, columns=["ticker"])
df.to_csv(f"{PATH}/us_symbols.csv")

raw_data = pd.read_csv(f"{PATH}/us_symbols.csv", sep=',')
tickers = raw_data['ticker'].dropna()

# Process tickers sequentially
print(f"Processing {len(tickers)} tickers sequentially...")
filtered_tickers = []
gross_returns_data = []
threshold = 1_000_000_000

for i, ticker in enumerate(tickers, 1):
    print(f"\nProcessing {i}/{len(tickers)}: {ticker}")
    
    try:
        # 1 second delay to avoid rate limiting
        time.sleep(1.0)
        
        info = yf.Ticker(ticker).info  # Get all the info of the Stock
        market_cap = info.get('marketCap', None) # if there is no marketCap we bring none value for no errors
        
        if market_cap and market_cap > threshold:
            filtered_tickers.append(ticker)
            gross_return = download_single_ticker(ticker)
            
            # Collect gross return data for CSV
            if gross_return != 0.0:  # Only add if we got valid data
                gross_returns_data.append({
                    'ticker': ticker,
                    'gross_return': gross_return
                })
            
            print(f"✅ {ticker}: {market_cap}")
        else:
            print(f"❌ {ticker}: {market_cap}")
            
    except Exception as e:
        print(f"❌ Error processing {ticker}: {e}")

# Save to CSV
df_filtered = pd.DataFrame(filtered_tickers, columns=["ticker"])
df_filtered.to_csv(f"{PATH}/filtered_tickers.csv", index=False)

print(f"\nTotal tickers above $1,000,000,000: {len(filtered_tickers)}")

# Save gross returns data to CSV
if gross_returns_data:
    returns_df = pd.DataFrame(gross_returns_data)
    returns_df = returns_df.sort_values('gross_return', ascending=False)
    returns_df.to_csv(f"{PATH}/sorted_returns.csv", index=False)
    
    print(f"\n✅ Successfully calculated gross returns for {len(returns_df)} tickers")
    print(f"📊 Top 10 performers:")
    print(returns_df.head(10)[['ticker', 'gross_return']].to_string(index=False))
    print(f"\n📉 Bottom 10 performers:")
    print(returns_df.tail(10)[['ticker', 'gross_return']].to_string(index=False))
else:
    print("❌ No valid gross return data calculated")
 # test