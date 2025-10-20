# This API call does not work with basic API key, should work with pro plan(?).
#===========================================================================#
import dlt
from dlt.sources.helpers import requests
from datetime import datetime, timedelta, timezone
import os
from dotenv import load_dotenv
import duckdb

load_dotenv()

API_KEY = os.getenv('API_KEY')
LOCATION = "38.6355,-90.2905"


if not API_KEY:
    raise ValueError("API Key not found")


@dlt.resource(
    write_disposition="replace",
    primary_key="time"
)

def weather_history():
    """Fetch past 7 days of weather data from Tomorrow.io"""
    url = "https://api.tomorrow.io/v4/timelines"

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=7)
    
    params = {
        #"apikey": API_KEY,
        "location": LOCATION,
        "fields": [
            "temperature",
            "temperatureApparent",
            "temperatureMin",
            "temperatureMax"
        ],
        "timesteps": ["1h"],
        "startTime": start_time.isoformat(),
        "endTime": end_time.isoformat(),
        "units": "imperial",
        "timezone": "America/Chicago"
    }
    headers = {
        "content-type": "application/json",
        "apikey": f'{API_KEY}'
    }

    response = requests.post(url, json=params, headers=headers)
    response.raise_for_status()
    data = response.json()

    print(f"Response keys: {data.keys()}")
    print(f"Status code: {response.status_code}")
    print(f"Start time: {start_time.isoformat()}")
    print(f"End time: {end_time.isoformat()}")

    if 'data' in data and 'timelines' in data:
        timelines = data['data']['timelines']
        print(f"Number of timelines: {len(timelines)}")

        if len(timelines) > 0:
            intervals = timelines[0].get('intervals', [])
            print(f"Number of intervals: {len(intervals)}")

            if len(intervals) > 0:
                print(f"First interval sample: {intervals[0]}")

            for timelines in timelines:
                for interval in timelines.get('intervals', []):
                    record = {
                        'time': interval['startTime'],
                        'location': LOCATION,
                        **interval['values']
                    }
                    yield record
        else:
            print("No timeline data returned.")
    else:
        print("Unexpected response structure:, {data}")


pipeline = dlt.pipeline(
    pipeline_name="tomorrow_zoo_weather",
    destination="duckdb",
    dataset_name="weather",
    dev_mode=False, 
)

if __name__ == "__main__":
    try:
        info = pipeline.run(weather_history(), table_name='weather_history')
        print(f"Loaded history: {info}")
       
        print(f"\nData loaded to: {pipeline.dataset_name}")
        print(f"Database location: {pipeline.pipeline_name}.duckdb")

    except Exception as e:
        print(f"Error occurred: {e}")
        import traceback
        traceback.print_exc()

