import pandas as pd
import time

df_monthly = pd.read_csv('adjusted_stock_returns_daily.csv', parse_dates=['Date'])

date_to_check = '2010-01-12'

apple_on_date = df_monthly[
    (df_monthly['Ticker'] == 'SFES') & (df_monthly['Date'] == date_to_check)
]

print(apple_on_date)