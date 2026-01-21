#!/usr/bin/env python
# coding: utf-8
import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm  import tqdm
@click.command()
@click.option('--user', default='root', help='PostgreSQL user')
@click.option('--password', default='root', help='PostgreSQL password')
@click.option('--host', default='localhost', help='PostgreSQL host')
@click.option('--port', default=5432, type=int, help='PostgreSQL port')
@click.option('--db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--table', default='yellow_taxi_data', help='Target table name')
def ingest_data(user, password, host, port, db, table):
    year=2021
    month=1
    # Read a sample of the data
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url=f"{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz"
    dtype = {
        "VendorID": "Int64",
        "passenger_count": "Int64",
        "trip_distance": "float64",
        "RatecodeID": "Int64",
        "store_and_fwd_flag": "string",
        "PULocationID": "Int64",
        "DOLocationID": "Int64",
        "payment_type": "Int64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "improvement_surcharge": "float64",
        "total_amount": "float64",
        "congestion_surcharge": "float64"
    }

    parse_dates = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime"
    ]
    chunk_size=100000
    engine=create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    df_iter=pd.read_csv(url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunk_size)
    first=True
    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(n=0).to_sql(name=table,con=engine,if_exists='replace')
            fist=False
        df_chunk.to_sql(name=table,con=engine,if_exists='append')


if __name__ =="__main__":
    ingest_data()
     




