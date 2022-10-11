
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
import argparse
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    if url.endswith('.parquet.gz'):
        parquet_name = 'output.parquet.gz'
    else:
        parquet_name = 'output.parquet'

    os.system(f"wget {url} -O {parquet_name}")
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    trips = pq.read_table(parquet_name)
    df = trips.to_pandas()

    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    # engine.connect()

    df.head(0).to_sql(name= table_name, con = engine, if_exists = 'replace')
    df.to_sql(name= table_name, con = engine, if_exists = 'append', chunksize=100000)
    # print(pd.io.sql.get_schema(name = table_name, name = 'yellow_taxi_data', con = engine))
    print("Data Inserted")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')
    args = parser.parse_args()
    main(args)