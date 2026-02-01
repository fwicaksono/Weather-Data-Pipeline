import duckdb
import os
import sys
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import psycopg2.extras

# Optional: Load .env for local testing
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

def get_env_variable(var_name):
    """Helper to enforce strict env var loading."""
    value = os.getenv(var_name)
    if not value:
        raise ValueError(f"CRITICAL ERROR: Environment variable '{var_name}' is missing.")
    return value

def load_to_postgres():
    print("--- Starting Load Process (Silver -> Gold) ---")

    # 1. Load Strict Configs
    minio_endpoint = get_env_variable('MINIO_ENDPOINT')
    access_key = get_env_variable('MINIO_ROOT_USER')
    secret_key = get_env_variable('MINIO_ROOT_PASSWORD')
    silver_bucket = get_env_variable('MINIO_BUCKET_SILVER')
    
    pg_user = get_env_variable('POSTGRES_USER')
    pg_pass = get_env_variable('POSTGRES_PASSWORD')
    pg_host = get_env_variable('POSTGRES_HOST')
    pg_port = get_env_variable('POSTGRES_PORT')
    pg_db = get_env_variable('POSTGRES_DB')

    # 2. Setup DuckDB to Read Parquet from MinIO
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_endpoint='{minio_endpoint}';
        SET s3_access_key_id='{access_key}';
        SET s3_secret_access_key='{secret_key}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)

    # 3. Read Data into a Pandas DataFrame
    # In a huge production system, we might stream this. 
    # For <1GB data, fetching to Pandas is perfectly fine and easier to debug.
    print(f"Reading Parquet from s3://{silver_bucket}...")
    try:
        df = con.execute(f"SELECT * FROM 's3://{silver_bucket}/weather_data.parquet'").fetchdf()
        print(f"Read {len(df)} rows from Silver Layer.")
    except Exception as e:
        print(f"ERROR: Could not read Parquet file. Is the Silver bucket empty? Details: {e}")
        raise e

    # Writing Data (Gold Layer)
    try:
        print(f"Connecting to Postgres at {pg_host}:{pg_port}...")
        with psycopg2.connect(
            host=pg_host,
            database=pg_db,
            user=pg_user,
            password=pg_pass,
            port=pg_port
        ) as conn:
            with conn.cursor() as cur:
                # 1. Create table if not exists
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS weather_gold (
                        city TEXT,
                        temp_c DOUBLE PRECISION,
                        wind_speed DOUBLE PRECISION,
                        observation_time TEXT,
                        processed_at TIMESTAMP
                    )
                """)
                
                # 2. Insert Data
                columns = df.columns.tolist()
                values = [tuple(x) for x in df.values]
                insert_query = f"INSERT INTO weather_gold ({','.join(columns)}) VALUES %s"
                
                psycopg2.extras.execute_values(cur, insert_query, values)
                print(f"SUCCESS: Loaded {len(df)} rows into table 'weather_gold'.")
                
    except Exception as e:
        print(f"LOAD FAILED: {e}")
        raise e

if __name__ == "__main__":
    load_to_postgres()