import requests
import os
import csv
import time
from datetime import datetime
from dotenv import load_dotenv

# For Snowflake Loading
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

# FOR S3 Loading
# import boto3
# from io import StringIO 

load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
DS = '2025-10-01'
def run_stock_job():
    DS = datetime.now().strftime('%Y-%m-%d')
    url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit=1000&sort=ticker&apiKey={POLYGON_API_KEY}"

    response = requests.get(url)
    data = response.json()

    tickers = []

    for ticker in data['results']:
        ticker['ds'] = DS
        tickers.append(ticker)

    while 'next_url' in data:
        time.sleep(12)
        print('requesting next page')
        response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
        data = response.json()
        for ticker in data['results']:
            ticker['ds'] = DS
            tickers.append(ticker)


    example_ticker =  {'ticker': 'ZWS', 
        'name': 'Zurn Elkay Water Solutions Corporation', 
        'market': 'stocks', 
        'locale': 'us', 
        'primary_exchange': 'XNYS', 
        'type': 'CS', 
        'active': True, 
        'currency_name': 'usd', 
        'cik': '0001439288', 
        'composite_figi': 'BBG000H8R0N8', 	
        'share_class_figi': 'BBG001T36GB5', 	
        'last_updated_utc': '2025-09-11T06:11:10.586204443Z',
        'ds': '2025-10-01'
        }

    fieldnames = list(example_ticker.keys())

    # Load to Snowflake instead of CSV
    load_to_snowflake(tickers, fieldnames)

    print(f'Loaded {len(tickers)} rows to Snowflake')


#LOADING DATA TO Snowflake
def load_to_snowflake(rows, fieldnames):
    # Build connection kwargs from environment variables
    connect_kwargs = {
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
    }
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    if account:
        connect_kwargs['account'] = account

    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
    database = os.getenv('SNOWFLAKE_DATABASE')
    schema = os.getenv('SNOWFLAKE_SCHEMA')
    role = os.getenv('SNOWFLAKE_ROLE')
    if warehouse:
        connect_kwargs['warehouse'] = warehouse
    if database:
        connect_kwargs['database'] = database
    if schema:
        connect_kwargs['schema'] = schema
    if role:
        connect_kwargs['role'] = role

    print(connect_kwargs)
    conn = snowflake.connector.connect( 
        user=connect_kwargs['user'],
        password=connect_kwargs['password'],
        account=connect_kwargs['account'],
        database=connect_kwargs['database'],
        schema=connect_kwargs['schema'],
        role=connect_kwargs['role'],
        session_parameters={
        "CLIENT_TELEMETRY_ENABLED": False,
        }
    )
    try:
        cs = conn.cursor()
        try:
            table_name = os.getenv('SNOWFLAKE_TABLE', 'stock_tickers')

            # Define typed schema based on example_ticker
            type_overrides = {
                'ticker': 'VARCHAR',
                'name': 'VARCHAR',
                'market': 'VARCHAR',
                'locale': 'VARCHAR',
                'primary_exchange': 'VARCHAR',
                'type': 'VARCHAR',
                'active': 'BOOLEAN',
                'currency_name': 'VARCHAR',
                'cik': 'VARCHAR',
                'composite_figi': 'VARCHAR',
                'share_class_figi': 'VARCHAR',
                'last_updated_utc': 'TIMESTAMP_NTZ',
                'ds': 'VARCHAR'
            }
            columns_sql_parts = []
            for col in fieldnames:
                col_type = type_overrides.get(col, 'VARCHAR')
                columns_sql_parts.append(f'"{col.upper()}" {col_type}')

            create_table_sql = f'CREATE TABLE IF NOT EXISTS {table_name} ( ' + ', '.join(columns_sql_parts) + ' )'
            cs.execute(create_table_sql)

            column_list = ', '.join([f'"{c.upper()}"' for c in fieldnames])
            placeholders = ', '.join([f'%({c})s' for c in fieldnames])
            insert_sql = f'INSERT INTO {table_name} ( {column_list} ) VALUES ( {placeholders} )'

            # Conform rows to fieldnames
            transformed = []
            for t in rows:
                row = {}
                for k in fieldnames:
                    row[k] = t.get(k, None)
                transformed.append(row)

            if transformed:
                cs.executemany(insert_sql, transformed)
        finally:
            cs.close()
    finally:
        conn.close()
    
    # LOADING DATA INTO LOCAL CSV
    # fieldnames = list(example_ticker.keys()) 
    # output_csv = 'tickers.csv'
    # with open(output_csv, mode='w', newline='', encoding='utf-8') as f:
    #     writer = csv.DictWriter(f, fieldnames=fieldnames)
    #     writer.writeheader()
    #     for t in tickers:
    #         row = {key: t.get(key, '') for key in fieldnames}
    #         writer.writerow(row)
    # print(f'Wrote {len(tickers)} rows to {output_csv}')

    # LOADING DATA INTO AWS S3
    # def load_to_s3():
    #     s3 = boto3.client('s3')
    #     bucket_name = 'stock-price-tracker-data'
    #     csv_buffer = StringIO()
    #     writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    #     writer.writeheader()
    #     for t in tickers:
    #         row = {key: t.get(key, '') for key in fieldnames}
    #         writer.writerow(row)
    #     s3.put_object(Bucket=bucket_name, Key='tickers.csv', Body=csv_buffer.getvalue())
    #     print(f'Wrote {len(tickers)} rows to S3 bucket {bucket_name} as tickers.csv')
if __name__ == "__main__":
    run_stock_job()
