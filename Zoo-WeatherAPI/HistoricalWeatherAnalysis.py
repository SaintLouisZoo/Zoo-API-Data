import dlt
from dlt.sources.helpers import requests
from datetime import datetime, timedelta, timezone
import os
from dotenv import load_dotenv
import duckdb
import json

load_dotenv()

API_KEY = os.getenv('API_KEY')
LOCATION = "38.6355,-90.2905"


if not API_KEY:
    raise ValueError("API Key not found")

@dlt.resource(
    write_disposition="append",
    primary_key="time"
)
def weather_realtime():
    """Fetch current weather data from Tomorrow.io"""
    url = "https://api.tomorrow.io/v4/weather/realtime"

    headers = {
        "content-type": "application/json",
        "apikey": f'{API_KEY}'
        
    }

    params = {
        #"apikey": API_KEY,
        "location": LOCATION,
        "units": "imperial"
    }
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()

    data = response.json()

    print("Response debug:")
    print(json.dumps(data, indent=2))

    data['data']['time'] = datetime.now().isoformat()
    data['data']['location'] = LOCATION

    yield data['data']



pipeline = dlt.pipeline(
    pipeline_name="tomorrow_zoo_weather",
    destination="duckdb",
    dataset_name="weather",
    dev_mode=False, 
)

if __name__ == "__main__":
    try:
        info = pipeline.run(weather_realtime(), table_name='weather_realtime')
        print(f"Loaded realtime: {info}")

       
        print(f"\nData loaded to: {pipeline.dataset_name}")
        print(f"Database location: {pipeline.pipeline_name}.duckdb")

    except Exception as e:
        print(f"Error occurred: {e}")
        import traceback
        traceback.print_exc()