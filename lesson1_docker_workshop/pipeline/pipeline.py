import sys
import pandas as pd

print("Pipeline started")

print("Arguments passed to the script:", sys.argv)

month =  sys.argv[1]


df = pd.DataFrame({"day": [1, 2], "num_passangers": [3, 4]})
df['month'] = int(month)
print(df.head())



df.to_parquet(f"output_day_{sys.argv[1]}.parquet")

# print(f'month={month}')