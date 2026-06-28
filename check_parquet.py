import pandas as pd

df = pd.read_parquet("data/silver/sales_silver.parquet")

print(df.head())

print()

print(df.columns)
print(df["metadata"].iloc[0])