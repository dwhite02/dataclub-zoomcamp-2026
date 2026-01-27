#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm
import click
import subprocess
import os

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table-trips', default='green_taxi_trips', help='Target table name for trip data')
@click.option('--target-table-zones', default='taxi_zones', help='Target table name for zone lookup')
@click.option('--chunksize', default=100000, help='Chunk size for data ingestion')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table_trips, target_table_zones, chunksize):
    """Ingest green taxi trip data and taxi zone lookup into PostgreSQL"""
    
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    
    # Download green taxi trip data
    trip_parquet_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet'
    trip_parquet_file = 'green_tripdata_2025-11.parquet'
    
    if not os.path.exists(trip_parquet_file):
        print(f"Downloading {trip_parquet_file}...")
        subprocess.run(['wget', trip_parquet_url, '-O', trip_parquet_file], check=True)
    
    # Download taxi zone lookup data
    zone_csv_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'
    zone_csv_file = 'taxi_zone_lookup.csv'
    
    if not os.path.exists(zone_csv_file):
        print(f"Downloading {zone_csv_file}...")
        subprocess.run(['wget', zone_csv_url, '-O', zone_csv_file], check=True)
    
    # Ingest taxi zone lookup (smaller, can load all at once)
    print("Loading taxi zone lookup...")
    df_zones = pd.read_csv(zone_csv_file)
    df_zones.to_sql(
        name=target_table_zones,
        con=engine,
        if_exists='replace',
        index=False
    )
    print(f"Inserted {len(df_zones)} zone records into {target_table_zones}")
    
    # Ingest green taxi trip data (parquet file)
    print("Loading green taxi trip data...")
    df_trips = pd.read_parquet(trip_parquet_file)
    
    # Process in chunks for memory efficiency
    for i in tqdm(range(0, len(df_trips), chunksize)):
        df_chunk = df_trips.iloc[i:i+chunksize]
        
        if i == 0:
            # Create table schema on first chunk
            df_chunk.head(0).to_sql(
                name=target_table_trips,
                con=engine,
                if_exists='replace',
                index=False
            )
            print(f"Table {target_table_trips} created")
        
        # Insert chunk
        df_chunk.to_sql(
            name=target_table_trips,
            con=engine,
            if_exists='append',
            index=False
        )
        print(f"Inserted {len(df_chunk)} records into {target_table_trips}")

if __name__ == '__main__':
    run()
