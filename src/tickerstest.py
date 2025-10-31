import pandas as pd

all_stock_data = pd.read_parquet("../data/all_stock_data.parquet")

print(all_stock_data.describe())
