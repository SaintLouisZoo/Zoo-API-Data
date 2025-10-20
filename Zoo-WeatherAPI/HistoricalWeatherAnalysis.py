import dlt
from dlt.sources.helpers import requests
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

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

    params = {
        "apikey": API_KEY,
        "location": LOCATION,
        "units": "imperial"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()

    data['data']['time'] = datetime.now().isoformat()
    data['data']['location'] = LOCATION

    yield data['data']


@dlt.resource(
    write_disposition="replace",
    primary_key="time"
)
def weather_forecast():
    """Fetch weather forcast from Tomorrow.io"""
    url = "https://api.tomorrow.io/v4/weather/forecast"

    params = {
        "apikey": API_KEY,
        "location": LOCATION,
        "timesteps": "1h",
        "units": "imperial"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()


    if 'timelines' in data and len(data['timelines']) > 0:
        timeline = data['timelines'][0]
        for interval in timeline['intervals']:

            record = {
                'time': interval['startTime'],
                'location': LOCATION,
                **interval['values']
            }
            yield record

pipeline = dlt.pipeline(
    pipeline_name="tomorrow_zoo_weather",
    destination="duckdb",
    dataset_name="weather",
    dev_mode=True #set to false when completed matthew i swear to god.
)

if __name__ == "__main__":
    info = pipeline.run(weather_realtime())
    print(f"Loaded {info}")

    info = pipeline.run(weather_forecast())
    print(f"Loaded {info}")

    print(f"\nData loaded to: {pipeline.dataset_name}")
    print(f"Database location: {pipeline.pipeline_name}.duckdb")