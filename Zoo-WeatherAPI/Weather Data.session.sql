
SELECT
            time,
            location,
            values__temperature as temp_f,
            values__temperature_apparent as feels_like,
            values__humidity as humidity,
            values__wind_speed as wind_speed,
            values__weather_code as weather_code
        FROM weather.weather_realtime
        ORDER BY time ASC
        LIMIT 10;


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
    
    -- TEMPERATURE PENALTIES
    CASE
      WHEN values__temperature < 32 THEN -50
      WHEN values__temperature < 40 THEN -40
      WHEN values__temperature < 50 THEN (50 - values__temperature) * -2.5
      WHEN values__temperature < 60 THEN (60 - values__temperature) * -1.5
      WHEN values__temperature BETWEEN 60 AND 72 THEN 0
      WHEN values__temperature <= 78 THEN (values__temperature - 72) * -1.0
      WHEN values__temperature <= 85 THEN -6 + (values__temperature - 78) * -2.0
      WHEN values__temperature <= 92 THEN -20 + (values__temperature - 85) * -3.0
      ELSE -41 + (values__temperature - 92) * -4.0
    END AS temp_penalty,
    
    -- HUMIDITY PENALTIES
    CASE
      WHEN values__temperature > 80 AND values__humidity > 80 THEN -15
      WHEN values__temperature > 80 AND values__humidity > 70 THEN -10
      WHEN values__temperature > 80 AND values__humidity > 60 THEN -5
      WHEN values__temperature < 50 AND values__humidity > 70 THEN -5
      WHEN values__humidity < 20 THEN -8
      WHEN values__humidity < 30 THEN -4
      ELSE 0
    END AS humidity_penalty,
    
    -- PRECIPITATION PENALTIES
    CASE
      WHEN COALESCE(values__rain_intensity, 0) = 0 THEN 0
      WHEN values__rain_intensity < 0.5 THEN -8
      WHEN values__rain_intensity < 1.5 THEN -18
      WHEN values__rain_intensity < 3 THEN -28
      WHEN values__rain_intensity < 6 THEN -38
      WHEN values__rain_intensity < 10 THEN -50
      ELSE -65
    END AS precip_penalty,
    
    -- WIND PENALTIES/BONUSES
    CASE
      WHEN values__temperature < 50 AND COALESCE(values__wind_speed, 0) > 20 THEN -18
      WHEN values__temperature < 50 AND values__wind_speed > 15 THEN -12
      WHEN values__temperature < 50 AND values__wind_speed > 10 THEN -8
      WHEN values__temperature < 50 AND values__wind_speed > 5 THEN -4
      WHEN values__temperature < 65 AND values__wind_speed > 20 THEN -10
      WHEN values__temperature < 65 AND values__wind_speed > 15 THEN -6
      WHEN values__temperature < 65 AND values__wind_speed > 10 THEN -3
      WHEN values__temperature > 85 AND values__wind_speed > 15 THEN 8
      WHEN values__temperature > 85 AND values__wind_speed > 10 THEN 6
      WHEN values__temperature > 85 AND values__wind_speed > 5 THEN 4
      WHEN values__temperature > 85 AND values__wind_speed <= 5 THEN -5
      WHEN values__temperature > 78 AND values__wind_speed > 15 THEN 5
      WHEN values__temperature > 78 AND values__wind_speed > 10 THEN 4
      WHEN values__temperature > 78 AND values__wind_speed > 5 THEN 2
      ELSE 0
    END AS wind_effect,
    
    -- EXTREME WIND PENALTIES
    CASE
      WHEN COALESCE(values__wind_speed, 0) > 35 THEN -25
      WHEN values__wind_speed > 30 THEN -15
      WHEN values__wind_speed > 25 THEN -8
      ELSE 0
    END AS extreme_wind_penalty,
    
    -- CLOUD COVER EFFECTS
    CASE
      WHEN COALESCE(values__cloud_cover, 0) < 10 AND values__temperature > 75 THEN -12
      WHEN values__cloud_cover < 10 AND values__temperature > 65 THEN -5
      WHEN values__cloud_cover < 10 THEN 0
      WHEN values__cloud_cover BETWEEN 10 AND 29 THEN 2
      WHEN values__cloud_cover BETWEEN 30 AND 69 THEN 3
      WHEN values__cloud_cover BETWEEN 70 AND 84 THEN -3
      WHEN values__cloud_cover BETWEEN 85 AND 94 THEN -8
      ELSE -12
    END AS cloud_penalty,
    
    -- UV INDEX PENALTIES
    CASE
      WHEN COALESCE(values__uv_index, 0) >= 11 THEN -15
      WHEN values__uv_index >= 9 THEN -10
      WHEN values__uv_index >= 7 THEN -6
      WHEN values__uv_index >= 5 THEN -3
      WHEN values__uv_index >= 3 THEN -1
      ELSE 0
    END AS uv_penalty,
    
    -- BONUS POINTS!
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
    
  FROM weather.weather_realtime
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
         uv_penalty + perfect_day_bonus + nice_clouds_bonus + comfortable_cool_bonus)) >= 90 
    THEN 'Perfect'
    WHEN GREATEST(1, LEAST(100, base_score + temp_penalty + humidity_penalty + 
         precip_penalty + wind_effect + extreme_wind_penalty + cloud_penalty + 
         uv_penalty + perfect_day_bonus + nice_clouds_bonus + comfortable_cool_bonus)) >= 80 
    THEN 'Excellent'
    WHEN GREATEST(1, LEAST(100, base_score + temp_penalty + humidity_penalty + 
         precip_penalty + wind_effect + extreme_wind_penalty + cloud_penalty + 
         uv_penalty + perfect_day_bonus + nice_clouds_bonus + comfortable_cool_bonus)) >= 70 
    THEN 'Good'
    WHEN GREATEST(1, LEAST(100, base_score + temp_penalty + humidity_penalty + 
         precip_penalty + wind_effect + extreme_wind_penalty + cloud_penalty + 
         uv_penalty + perfect_day_bonus + nice_clouds_bonus + comfortable_cool_bonus)) >= 60 
    THEN 'Fair'
    WHEN GREATEST(1, LEAST(100, base_score + temp_penalty + humidity_penalty + 
         precip_penalty + wind_effect + extreme_wind_penalty + cloud_penalty + 
         uv_penalty + perfect_day_bonus + nice_clouds_bonus + comfortable_cool_bonus)) >= 50 
    THEN 'Mediocre'
    ELSE 'Poor'
  END AS condition_rating

FROM weather_scores
ORDER BY time DESC;

SELECT * from weather.weather_realtime;