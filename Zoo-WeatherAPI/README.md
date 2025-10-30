# Zoo-WeatherAPI
---

To update the tomorrow.io API data, run HistoricalWeatherAnalysis.py script.
This script will create/update tomorrow_zoo_weather_realtime.duckdb and tomorrow_zoo_weather_scores.duckdb.  
*weather_realtime.duckdb contains current weather conditions, and *weather_scores.duckdb contains the numeric weather score based on the current weather data.   
After the db files are updated, right click on each of them and select the download option.  
Note: If this is not the first time running the script on your device, select the 'replace' option in the pop-up that appears.  

---
You can now navigate back to the Power BI report and refresh the data with the 'refresh' button in the Home tab.  
Your weather data should now be up-to-date.  

---
The TodayWeatherQuery.py can be used to display the data from the API call to the terminal if needed.  

---
The WeatherCodes.json file contains the codes that corresponde to weather conditions retrieved from tomorrow.io core API call.  
These codes are used in the Power BI report to display weather conditions.  

---
The PastWeekWeather.py script does not work with the free tier of tomorrow.io's API. If the API key is upgreaded, the script will collect and store the past 7 days of weather data.

---
The SQL file titled Weather Data.session.sql is used when a local connection is made to the duckdb file in VS Code.  
It is not used in typical uses and should, for the most part, be ignored.
