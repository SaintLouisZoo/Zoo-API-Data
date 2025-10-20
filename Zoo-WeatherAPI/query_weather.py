import duckdb
import pandas as pd

def query_weather():
    con = duckdb.connect('tomorrow_zoo_weather.duckdb', read_only=True)

    print("=" * 70)
    print("Latest Weather Data")
    print("=" * 70)

    df = con.execute("""
        SELECT
            time,
            location,
            values__temperature as temp_f,
            values__temperature_apparent as feels_like,
            values__humidity as humidity,
            values__wind_speed as wind_speed,
            values__weather_code as weather_code,
            values__precipitation_probability as precip_chance,
        FROM weather.weather_realtime
        ORDER BY time DESC
        LIMIT 10
    """).fetchdf()

    print(df.to_string())




if __name__ == "__main__":
    query_weather()
    