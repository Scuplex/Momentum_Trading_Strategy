import pandas as pd

# Load dataset
print("Loading dataset...")
df = pd.read_csv('all_stock_data.csv', parse_dates=['Date'])
df = df.sort_values(['Ticker', 'Date']).reset_index(drop=True)

# Define adjustment function
def compute_adjusted_prices(group):
    
    g = group.copy()
    g = g.sort_values('Date').reset_index(drop=True)

    # Handle missing/zero splits
    g['Stock Splits'] = g['Stock Splits'].replace(0, 1.0)

    # Dividend adjustment
    denom = g['Close'] - g['Dividends']
    g['div_mult'] = (g['Close'] / denom).where(denom > 0, 1.0).fillna(1.0)

    # Split adjustment 
    g['split_mult'] = 1 / g['Stock Splits']

    # Combined adjustment factor
    g['multiplier'] = g['div_mult'] * g['split_mult']

    # Apply backward cumulative adjustment
    multiplier_shifted = g['multiplier'].shift(-1, fill_value=1.0)
    adj_factor = multiplier_shifted.iloc[::-1].cumprod().iloc[::-1]
    g['adj_factor'] = adj_factor

    # Adjusted close
    g['Adj Close'] = g['Close'] * g['adj_factor']

    # Adjusted daily returns
    g['Adj Return'] = g['Adj Close'].pct_change()
    g['Total Return Index'] = (1 + g['Adj Return'].fillna(0)).cumprod()

    return g

# Apply adjustment per ticker
print("Computing adjusted prices and returns...")
df_adj = df.groupby('Ticker', group_keys=False, sort=False).apply(compute_adjusted_prices)

# Save daily adjusted results
cols_daily = [
    'Date', 'Ticker',
    'Close', 'Adj Close',
    'Adj Return', 'Total Return Index'
]
df_adj[cols_daily].to_csv('adjusted_stock_returns_daily.csv', index=False)
print("✅ Saved daily adjusted results to 'adjusted_stock_returns_daily.csv'")

# Convert to monthly
print("Aggregating to monthly data...")
df_monthly = (
    df_adj.set_index('Date')
    .groupby('Ticker')['Adj Close']
    .resample('M')
    .last()  # last trading day of each month
    .reset_index()
)

# Monthly returns
df_monthly['Monthly Return'] = df_monthly.groupby('Ticker')['Adj Close'].pct_change()

# Step 6: Save monthly data
df_monthly.to_csv('adjusted_stock_returns_monthly.csv', index=False)
print("✅ Saved monthly adjusted results to 'adjusted_stock_returns_monthly.csv'")

print("🎉 All done! You now have daily and monthly adjusted returns.")