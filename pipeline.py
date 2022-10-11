import pandas as pd
import os
import pyarrow.parquet as pq
import wget
url = "http://172.25.112.1:8000/yellow_tripdata_2022-01.parquet"
parquet_name = 'output.parquet'
os.system(f"wget {url} -O {parquet_name}")
trips = pq.read_table(parquet_name)
df = trips.to_pandas()
print(df)
