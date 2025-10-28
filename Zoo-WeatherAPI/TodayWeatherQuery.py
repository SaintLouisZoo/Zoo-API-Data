import duckdb
import pandas as pd

def query_weather():
    """Query both weather databases and display results"""
    
    # Query realtime weather data
    con_realtime = duckdb.connect('tomorrow_zoo_weather_realtime.duckdb', read_only=True)
    
    print("=" * 70)
    print("Latest Weather Data (Realtime)")
    print("=" * 70)
    
    df_realtime = con_realtime.execute("""
        SELECT
            time,
            location,
            values__temperature as temp_f,
            values__temperature_apparent as feels_like,
            values__humidity as humidity,
            values__wind_speed as wind_speed,
            values__weather_code as weather_code,
            values__precipitation_probability as precip_chance
        FROM weather.weather_realtime
        ORDER BY time DESC
        LIMIT 10
    """).fetchdf()
    
    print(df_realtime.to_string())
    print()
    
    con_realtime.close()
    
    # Query zoo weather scores
    con_scores = duckdb.connect('tomorrow_zoo_weather_scores.duckdb', read_only=True)
    
    print("=" * 70)
    print("Zoo Weather Scores")
    print("=" * 70)
    
    df_scores = con_scores.execute("""
        SELECT
            time,
            location,
            zoo_weather_score,
            condition_rating
        FROM weather.zoo_weather_scores
        ORDER BY time DESC
        LIMIT 10
    """).fetchdf()
    
    print(df_scores.to_string())
    print()
    
    con_scores.close()
    
    # Display summary
    print("=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"Realtime records: {len(df_realtime)}")
    print(f"Score records: {len(df_scores)}")
    
    if not df_scores.empty:
        print(f"Current zoo weather score: {df_scores.iloc[0]['zoo_weather_score']}")
        print(f"Current condition rating: {df_scores.iloc[0]['condition_rating']}")

if __name__ == "__main__":
    query_weather()