import duckdb
import os
import sys
from minio import Minio

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

def transform_weather():
    print("--- Starting Transformation (Bronze -> Silver) ---")

    # 1. Load Strict Configs (No Defaults)
    minio_endpoint = get_env_variable('MINIO_ENDPOINT')
    access_key = get_env_variable('MINIO_ROOT_USER')
    secret_key = get_env_variable('MINIO_ROOT_PASSWORD')
    bronze_bucket = get_env_variable('MINIO_BUCKET_BRONZE')
    silver_bucket = get_env_variable('MINIO_BUCKET_SILVER')

    # 2. Connect to MinIO to ensure buckets exist
    print(f"Connecting to MinIO at {minio_endpoint} to ensure buckets exist...")
    try:
        minio_client = Minio(
            minio_endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
        
        # Check/Create Silver Bucket
        if not minio_client.bucket_exists(silver_bucket):
            minio_client.make_bucket(silver_bucket)
            print(f"Created bucket: {silver_bucket}")
        else:
            print(f"Bucket '{silver_bucket}' found.")

    except Exception as e:
        print(f"MINIO CONNECTION/BUCKET ERROR: {e}")
        raise e

    # 3. Initialize DuckDB & Install S3 Extensions
    # We use an in-memory DuckDB instance to process the data
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    
    # 3. Configure DuckDB to talk to MinIO
    # Note: s3_use_ssl is False because we are using local HTTP MinIO
    con.execute(f"""
        SET s3_endpoint='{minio_endpoint}';
        SET s3_access_key_id='{access_key}';
        SET s3_secret_access_key='{secret_key}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    print("DuckDB configured for MinIO access.")

    # 4. The Transformation Logic (SQL)
    # - We extract 'temperature' and 'windspeed' from the nested 'current_weather' JSON object
    # - We convert Kelvin to Celsius (if source is Kelvin, though Open-Meteo usually gives C, this logic is good practice)
    # - We standardise the column names
    
    query = f"""
    COPY (
        SELECT 
            'jogja' as city, 
            current_weather.temperature as temp_c,
            current_weather.windspeed as wind_speed,
            current_weather.time as observation_time,
            current_timestamp + INTERVAL 7 HOUR as processed_at
        FROM read_json_auto('s3://{bronze_bucket}/jogja_weather.json')
        
        UNION ALL
        
        SELECT 
            'aceh' as city, 
            current_weather.temperature as temp_c,
            current_weather.windspeed as wind_speed,
            current_weather.time as observation_time,
            current_timestamp + INTERVAL 7 HOUR as processed_at
        FROM read_json_auto('s3://{bronze_bucket}/aceh_weather.json')
        
        UNION ALL
        
        SELECT 
            'muntilan' as city, 
            current_weather.temperature as temp_c,
            current_weather.windspeed as wind_speed,
            current_weather.time as observation_time,
            current_timestamp + INTERVAL 7 HOUR as processed_at
        FROM read_json_auto('s3://{bronze_bucket}/muntilan_weather.json')
        
    ) TO 's3://{silver_bucket}/weather_data.parquet' (FORMAT 'PARQUET');
    """
    
    try:
        print("Executing transformation query...")
        con.execute(query)
        print(f"SUCCESS: Transformed data saved to s3://{silver_bucket}/weather_data.parquet")
    except Exception as e:
        print(f"TRANSFORMATION FAILED: {e}")
        raise e

if __name__ == "__main__":
    transform_weather()