import pandas as pd
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas
import boto3

ssm = boto3.client('ssm', region_name='us-east-1')
s3  = boto3.client('s3',  region_name='us-east-1')

def get_param(name):
    return ssm.get_parameter(Name=name, WithDecryption=True)['Parameter']['Value']

def run_script():

    # Get Snowflake credentials from SSM
    sf_user     = get_param('/snowflake/username')
    sf_password = get_param('/snowflake/password')
    sf_account  = get_param('/snowflake/accountname')

    # Connect to Snowflake
    conn = snow.connect(
        user=sf_user,
        password=sf_password,
        account=sf_account,
        warehouse='COMPUTE_WH',
        database='PROD',
        schema='DBT_RAW'
    )
    cur = conn.cursor()
    print('Snowflake connection created')
    cur.execute("USE DATABASE PROD")
    cur.execute("USE SCHEMA DBT_RAW")
    # Truncate tables to avoid duplicate loads
    cur.execute("TRUNCATE TABLE IF EXISTS TITLES_RAW")
    cur.execute("TRUNCATE TABLE IF EXISTS CREDITS_RAW")
    print('Tables truncated')

    # Read CSV files from S3
    titles_obj  = s3.get_object(
        Bucket='netflix-data-analytics-artvion',
        Key='raw_files/titles.csv'
    )
    credits_obj = s3.get_object(
        Bucket='netflix-data-analytics-artvion',
        Key='raw_files/credits.csv'
    )

    titles_df  = pd.read_csv(titles_obj['Body'])
    credits_df = pd.read_csv(credits_obj['Body'])
    print(f'Files read - Titles: {len(titles_df)} rows, Credits: {len(credits_df)} rows')

    # Convert column names to uppercase for Snowflake
    titles_df.columns  = [c.upper() for c in titles_df.columns]
    credits_df.columns = [c.upper() for c in credits_df.columns]

    # Load into Snowflake
    write_pandas(conn, titles_df,  'TITLES_RAW',  auto_create_table=True, overwrite=True)
    print('Titles loaded into Snowflake')

    write_pandas(conn, credits_df, 'CREDITS_RAW', auto_create_table=True, overwrite=True)
    print('Credits loaded into Snowflake')

    cur.close()
    conn.close()
    print('Done!')
