import duckdb

con = duckdb.connect('tomorrow_zoo_weather.duckdb')

# See all schemas
print("Available schemas:")
print(con.execute("SELECT * FROM information_schema.schemata").fetchall())
print(con.sql("SHOW TABLES").df())
# Query with correct schema name
print("\nRealtime Data:")
df = con.execute("SELECT * FROM weather.weather_realtime LIMIT 10").fetchdf()
print(df)

con.close()