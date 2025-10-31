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
    write_disposition="replace",
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

# Pipeline for weather_realtime table
pipeline_realtime = dlt.pipeline(
    pipeline_name="tomorrow_zoo_weather_realtime",
    destination="duckdb",
    dataset_name="weather",
    dev_mode=False, 
)

# Pipeline for zoo_weather_scores table
pipeline_scores = dlt.pipeline(
    pipeline_name="tomorrow_zoo_weather_scores",
    destination="duckdb",
    dataset_name="weather",
    dev_mode=False, 
)

ZOO_WEATHER_SCORE_QUERY = """
CREATE OR REPLACE TABLE weather.zoo_weather_scores AS
WITH weather_scores AS (
  SELECT 
    time,
    location,
    values__temperature AS temp,
    values__humidity AS humidity,
    COALESCE(values__rain_intensity, 0) AS rain_intensity,
    COALESCE(values__wind_speed, 0) AS wind_speed,
    COALESCE(values__cloud_cover, 0) AS cloud_cover,
    COALESCE(values__uv_index, 0) AS uv_index,
    
    100.0 AS base_score,
    
    -- MORE PUNISHING TEMPERATURE PENALTIES
    CASE
      WHEN values__temperature < 20 THEN -80
      WHEN values__temperature < 32 THEN -70
      WHEN values__temperature < 40 THEN -55
      WHEN values__temperature < 50 THEN (50 - values__temperature) * -4.0
      WHEN values__temperature < 60 THEN (60 - values__temperature) * -2.5
      WHEN values__temperature BETWEEN 60 AND 72 THEN 0
      WHEN values__temperature <= 78 THEN (values__temperature - 72) * -1.5
      WHEN values__temperature <= 85 THEN -9 + (values__temperature - 78) * -3.0
      WHEN values__temperature <= 92 THEN -30 + (values__temperature - 85) * -4.5
      WHEN values__temperature <= 100 THEN -61.5 + (values__temperature - 92) * -5.5
      ELSE -105.5 + (values__temperature - 100) * -7.0
    END AS temp_penalty,
    
    -- MORE PUNISHING HUMIDITY PENALTIES
    CASE
      WHEN values__temperature > 90 AND values__humidity > 80 THEN -30
      WHEN values__temperature > 90 AND values__humidity > 70 THEN -22
      WHEN values__temperature > 90 AND values__humidity > 60 THEN -15
      WHEN values__temperature > 80 AND values__humidity > 80 THEN -25
      WHEN values__temperature > 80 AND values__humidity > 70 THEN -18
      WHEN values__temperature > 80 AND values__humidity > 60 THEN -12
      WHEN values__temperature < 40 AND values__humidity > 80 THEN -15
      WHEN values__temperature < 40 AND values__humidity > 70 THEN -10
      WHEN values__temperature < 50 AND values__humidity > 70 THEN -8
      WHEN values__humidity < 15 THEN -12
      WHEN values__humidity < 20 THEN -10
      WHEN values__humidity < 30 THEN -6
      ELSE 0
    END AS humidity_penalty,
    
    -- MORE PUNISHING PRECIPITATION PENALTIES
    CASE
      WHEN COALESCE(values__rain_intensity, 0) = 0 THEN 0
      WHEN values__rain_intensity < 0.25 THEN -12
      WHEN values__rain_intensity < 0.5 THEN -18
      WHEN values__rain_intensity < 1.0 THEN -28
      WHEN values__rain_intensity < 1.5 THEN -38
      WHEN values__rain_intensity < 3 THEN -50
      WHEN values__rain_intensity < 6 THEN -65
      WHEN values__rain_intensity < 10 THEN -80
      ELSE -100
    END AS precip_penalty,
    
    -- MORE PUNISHING WIND PENALTIES
    CASE
      WHEN values__temperature < 40 AND COALESCE(values__wind_speed, 0) > 25 THEN -30
      WHEN values__temperature < 40 AND values__wind_speed > 20 THEN -24
      WHEN values__temperature < 40 AND values__wind_speed > 15 THEN -18
      WHEN values__temperature < 40 AND values__wind_speed > 10 THEN -12
      WHEN values__temperature < 40 AND values__wind_speed > 5 THEN -8
      WHEN values__temperature < 50 AND values__wind_speed > 20 THEN -22
      WHEN values__temperature < 50 AND values__wind_speed > 15 THEN -16
      WHEN values__temperature < 50 AND values__wind_speed > 10 THEN -10
      WHEN values__temperature < 50 AND values__wind_speed > 5 THEN -6
      WHEN values__temperature < 65 AND values__wind_speed > 20 THEN -14
      WHEN values__temperature < 65 AND values__wind_speed > 15 THEN -10
      WHEN values__temperature < 65 AND values__wind_speed > 10 THEN -6
      WHEN values__temperature > 85 AND values__wind_speed > 15 THEN 10
      WHEN values__temperature > 85 AND values__wind_speed > 10 THEN 8
      WHEN values__temperature > 85 AND values__wind_speed > 5 THEN 5
      WHEN values__temperature > 85 AND values__wind_speed <= 5 THEN -8
      WHEN values__temperature > 78 AND values__wind_speed > 15 THEN 6
      WHEN values__temperature > 78 AND values__wind_speed > 10 THEN 5
      WHEN values__temperature > 78 AND values__wind_speed > 5 THEN 3
      ELSE 0
    END AS wind_effect,
    
    -- MUCH MORE PUNISHING EXTREME WIND PENALTIES
    CASE
      WHEN COALESCE(values__wind_speed, 0) > 40 THEN -50
      WHEN values__wind_speed > 35 THEN -35
      WHEN values__wind_speed > 30 THEN -25
      WHEN values__wind_speed > 25 THEN -15
      ELSE 0
    END AS extreme_wind_penalty,
    
    -- MORE PUNISHING CLOUD COVER PENALTIES
    CASE
      WHEN COALESCE(values__cloud_cover, 0) < 10 AND values__temperature > 85 THEN -18
      WHEN values__cloud_cover < 10 AND values__temperature > 75 THEN -15
      WHEN values__cloud_cover < 10 AND values__temperature > 65 THEN -8
      WHEN values__cloud_cover < 10 THEN 0
      WHEN values__cloud_cover BETWEEN 10 AND 29 THEN 2
      WHEN values__cloud_cover BETWEEN 30 AND 69 THEN 3
      WHEN values__cloud_cover BETWEEN 70 AND 84 THEN -6
      WHEN values__cloud_cover BETWEEN 85 AND 94 THEN -15
      ELSE -25
    END AS cloud_penalty,
    
    -- MORE PUNISHING UV INDEX PENALTIES
    CASE
      WHEN COALESCE(values__uv_index, 0) >= 11 THEN -25
      WHEN values__uv_index >= 9 THEN -18
      WHEN values__uv_index >= 7 THEN -12
      WHEN values__uv_index >= 5 THEN -6
      WHEN values__uv_index >= 3 THEN -2
      ELSE 0
    END AS uv_penalty,
    
    -- BONUSES (kept the same)
    CASE
      WHEN values__temperature BETWEEN 62 AND 70 
        AND values__humidity BETWEEN 40 AND 60 
        AND COALESCE(values__rain_intensity, 0) = 0 
      THEN 5
      ELSE 0
    END AS perfect_day_bonus,
    
    CASE
      WHEN COALESCE(values__cloud_cover, 0) BETWEEN 30 AND 60 
        AND COALESCE(values__rain_intensity, 0) = 0 
      THEN 3
      ELSE 0
    END AS nice_clouds_bonus,
    
    CASE
      WHEN values__temperature BETWEEN 55 AND 65 
        AND COALESCE(values__wind_speed, 0) < 8 
        AND COALESCE(values__rain_intensity, 0) = 0 
      THEN 2
      ELSE 0
    END AS comfortable_cool_bonus
    
  FROM realtime_db.weather.weather_realtime
)

SELECT
  time,
  location,
  GREATEST(1, LEAST(100, ROUND(
    base_score + temp_penalty + humidity_penalty + precip_penalty + 
    wind_effect + extreme_wind_penalty + cloud_penalty + uv_penalty + 
    perfect_day_bonus + nice_clouds_bonus + comfortable_cool_bonus, 1
  ))) AS zoo_weather_score,
  
  CASE
    WHEN GREATEST(1, LEAST(100, base_score + temp_penalty + humidity_penalty + 
         precip_penalty + wind_effect + extreme_wind_penalty + cloud_penalty + 
         uv_penalty + perfect_day_bonus + nice_clouds_bonus + comfortable_cool_bonus)) >= 95 
    THEN 'Perfect'
    WHEN GREATEST(1, LEAST(100, base_score + temp_penalty + humidity_penalty + 
         precip_penalty + wind_effect + extreme_wind_penalty + cloud_penalty + 
         uv_penalty + perfect_day_bonus + nice_clouds_bonus + comfortable_cool_bonus)) >= 90 
    THEN 'Outstanding'
    WHEN GREATEST(1, LEAST(100, base_score + temp_penalty + humidity_penalty + 
         precip_penalty + wind_effect + extreme_wind_penalty + cloud_penalty + 
         uv_penalty + perfect_day_bonus + nice_clouds_bonus + comfortable_cool_bonus)) >= 85 
    THEN 'Excellent'
    WHEN GREATEST(1, LEAST(100, base_score + temp_penalty + humidity_penalty + 
         precip_penalty + wind_effect + extreme_wind_penalty + cloud_penalty + 
         uv_penalty + perfect_day_bonus + nice_clouds_bonus + comfortable_cool_bonus)) >= 80 
    THEN 'Very Good'
    WHEN GREATEST(1, LEAST(100, base_score + temp_penalty + humidity_penalty + 
         precip_penalty + wind_effect + extreme_wind_penalty + cloud_penalty + 
         uv_penalty + perfect_day_bonus + nice_clouds_bonus + comfortable_cool_bonus)) >= 75 
    THEN 'Good'
    WHEN GREATEST(1, LEAST(100, base_score + temp_penalty + humidity_penalty + 
         precip_penalty + wind_effect + extreme_wind_penalty + cloud_penalty + 
         uv_penalty + perfect_day_bonus + nice_clouds_bonus + comfortable_cool_bonus)) >= 70 
    THEN 'Pleasant'
    WHEN GREATEST(1, LEAST(100, base_score + temp_penalty + humidity_penalty + 
         precip_penalty + wind_effect + extreme_wind_penalty + cloud_penalty + 
         uv_penalty + perfect_day_bonus + nice_clouds_bonus + comfortable_cool_bonus)) >= 65 
    THEN 'Acceptable'
    WHEN GREATEST(1, LEAST(100, base_score + temp_penalty + humidity_penalty + 
         precip_penalty + wind_effect + extreme_wind_penalty + cloud_penalty + 
         uv_penalty + perfect_day_bonus + nice_clouds_bonus + comfortable_cool_bonus)) >= 60 
    THEN 'Fair'
    WHEN GREATEST(1, LEAST(100, base_score + temp_penalty + humidity_penalty + 
         precip_penalty + wind_effect + extreme_wind_penalty + cloud_penalty + 
         uv_penalty + perfect_day_bonus + nice_clouds_bonus + comfortable_cool_bonus)) >= 55 
    THEN 'Tolerable'
    WHEN GREATEST(1, LEAST(100, base_score + temp_penalty + humidity_penalty + 
         precip_penalty + wind_effect + extreme_wind_penalty + cloud_penalty + 
         uv_penalty + perfect_day_bonus + nice_clouds_bonus + comfortable_cool_bonus)) >= 50 
    THEN 'Mediocre'
    WHEN GREATEST(1, LEAST(100, base_score + temp_penalty + humidity_penalty + 
         precip_penalty + wind_effect + extreme_wind_penalty + cloud_penalty + 
         uv_penalty + perfect_day_bonus + nice_clouds_bonus + comfortable_cool_bonus)) >= 45 
    THEN 'Subpar'
    WHEN GREATEST(1, LEAST(100, base_score + temp_penalty + humidity_penalty + 
         precip_penalty + wind_effect + extreme_wind_penalty + cloud_penalty + 
         uv_penalty + perfect_day_bonus + nice_clouds_bonus + comfortable_cool_bonus)) >= 40 
    THEN 'Poor'
    WHEN GREATEST(1, LEAST(100, base_score + temp_penalty + humidity_penalty + 
         precip_penalty + wind_effect + extreme_wind_penalty + cloud_penalty + 
         uv_penalty + perfect_day_bonus + nice_clouds_bonus + comfortable_cool_bonus)) >= 35 
    THEN 'Bad'
    WHEN GREATEST(1, LEAST(100, base_score + temp_penalty + humidity_penalty + 
         precip_penalty + wind_effect + extreme_wind_penalty + cloud_penalty + 
         uv_penalty + perfect_day_bonus + nice_clouds_bonus + comfortable_cool_bonus)) >= 30 
    THEN 'Very Bad'
    WHEN GREATEST(1, LEAST(100, base_score + temp_penalty + humidity_penalty + 
         precip_penalty + wind_effect + extreme_wind_penalty + cloud_penalty + 
         uv_penalty + perfect_day_bonus + nice_clouds_bonus + comfortable_cool_bonus)) >= 25 
    THEN 'Unpleasant'
    WHEN GREATEST(1, LEAST(100, base_score + temp_penalty + humidity_penalty + 
         precip_penalty + wind_effect + extreme_wind_penalty + cloud_penalty + 
         uv_penalty + perfect_day_bonus + nice_clouds_bonus + comfortable_cool_bonus)) >= 20 
    THEN 'Miserable'
    WHEN GREATEST(1, LEAST(100, base_score + temp_penalty + humidity_penalty + 
         precip_penalty + wind_effect + extreme_wind_penalty + cloud_penalty + 
         uv_penalty + perfect_day_bonus + nice_clouds_bonus + comfortable_cool_bonus)) >= 15 
    THEN 'Severe'
    WHEN GREATEST(1, LEAST(100, base_score + temp_penalty + humidity_penalty + 
         precip_penalty + wind_effect + extreme_wind_penalty + cloud_penalty + 
         uv_penalty + perfect_day_bonus + nice_clouds_bonus + comfortable_cool_bonus)) >= 10 
    THEN 'Extreme'
    ELSE 'Dangerous'
  END AS condition_rating

FROM weather_scores
ORDER BY time DESC;
"""

if __name__ == "__main__":
    try:
        # Load realtime data to first database
        info = pipeline_realtime.run(weather_realtime(), table_name='weather_realtime')
        print(f"Loaded realtime: {info}")
        print(f"Realtime database: {pipeline_realtime.pipeline_name}.duckdb")
       
        # Open connections
        conn_realtime = duckdb.connect(f"{pipeline_realtime.pipeline_name}.duckdb", read_only=True)
        conn_scores = duckdb.connect(f"{pipeline_scores.pipeline_name}.duckdb")
        
        # Create the weather schema in the scores database
        conn_scores.execute("CREATE SCHEMA IF NOT EXISTS weather")
        
        # Attach the realtime database to read from it
        conn_scores.execute(f"ATTACH '{pipeline_realtime.pipeline_name}.duckdb' AS realtime_db")
        
        # Execute the query (already modified to use realtime_db in ZOO_WEATHER_SCORE_QUERY)
        conn_scores.execute(ZOO_WEATHER_SCORE_QUERY)
        
        # Verify the data was loaded
        result = conn_scores.execute("SELECT COUNT(*) FROM weather.zoo_weather_scores").fetchone()
        print(f"Rows inserted into zoo_weather_scores: {result[0]}")
        
        conn_scores.close()
        conn_realtime.close()
        
        print("Created zoo_weather_scores table")
        print(f"Scores database: {pipeline_scores.pipeline_name}.duckdb")
        
        print(f"Two separate databases created:")  
        print(f"1. {pipeline_realtime.pipeline_name}.duckdb - contains weather_realtime table")
        print(f"2. {pipeline_scores.pipeline_name}.duckdb - contains zoo_weather_scores table")
        
    except Exception as e:
        print(f"Error occurred: {e}")
        import traceback
        traceback.print_exc()