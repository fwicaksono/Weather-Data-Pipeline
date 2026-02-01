import requests
import json
import os
import sys
from minio import Minio
from io import BytesIO

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

def extract_weather():
    print("--- Starting Extraction (Internet -> Bronze) ---")

    # 1. Load Strict Configs (No Defaults)
    minio_endpoint = get_env_variable('MINIO_ENDPOINT')
    access_key = get_env_variable('MINIO_ROOT_USER')
    secret_key = get_env_variable('MINIO_ROOT_PASSWORD')
    bucket_name = get_env_variable('MINIO_BUCKET_BRONZE')
    api_base_url = get_env_variable('WEATHER_API_BASE_URL')

    # 2. Define Locations
    locations = {
        "jogja": {"lat": -7.7956, "lon": 110.3695},
        "aceh": {"lat": 5.5483, "lon": 95.3238},
        "muntilan": {"lat": -7.5811, "lon": 110.2928}
    }
    
    # 3. Connect to MinIO
    print(f"Connecting to MinIO at {minio_endpoint}...")
    try:
        client = Minio(
            minio_endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
        
        # Check/Create Bucket
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Created bucket: {bucket_name}")
        else:
            print(f"Bucket '{bucket_name}' found.")

    except Exception as e:
        print(f"CONNECTION FAILED: {e}")
        raise e

    # 4. Fetch & Save
    for city, coords in locations.items():
        try:
            # Construct URL
            # Construct URL
            base_url_clean = api_base_url.split('?')[0]
            url = f"{base_url_clean}?latitude={coords['lat']}&longitude={coords['lon']}&current_weather=true&timezone=Asia%2FJakarta"
            print(f"Fetching {city} from {url}...")
            
            response = requests.get(url)
            response.raise_for_status()
            
            data = response.json()
            
            # File name: jogja_weather.json
            file_name = f"{city}_weather.json"
            
            # Prepare Payload
            json_data = json.dumps(data).encode('utf-8')
            
            # Upload
            client.put_object(
                bucket_name,
                file_name,
                data=BytesIO(json_data),
                length=len(json_data),
                content_type='application/json'
            )
            print(f"SUCCESS: Saved {file_name} to Bronze Layer.")
            
        except Exception as e:
            print(f"ERROR processing {city}: {e}")
            # We don't raise here so one failure doesn't stop the other city
            # But in a strict pipeline, you might want to raise e.
            raise e

if __name__ == "__main__":
    extract_weather()