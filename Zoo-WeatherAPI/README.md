## Zoo-WeatherAPI
Weather API reporting for the STL Zoo


# On clean start-up: ##  

Add API key to .env file.  

Run HistoricalWeatherAnalysis.py  

Navigate to SQLTools and establish connection with Weather Data duckdb database.  
You will be prompted to download duckdbasync0.10.2 --> select install, it already is installed but it doesn't recognize it idk.  

After you connect the database, you will not be able to execute the 'HistoricalWeatherAnalysis.py' script again.  
To refresh the weather data you will need to start a new codespace.  
^^^Looking for fix^^^  
  
  
Test.py outputs the schema of the table.  
HistoricalWeatherAnalysis.py calls tomorrow.io API for real time weather data.  
Weather_query outputs the HistoricalWeatherAnalysis.py data to the terminal output to view data without needing to connect to duckdb.


When updating the data, simply rightclick on "tomrrow_zoo_weather.duckdb" and select download. Click yes if prompted to replace an exising file. 
