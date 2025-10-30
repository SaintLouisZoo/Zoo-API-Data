# Sensource-API
---

To update the sensource API data, run DailyTotal.py and YearToDate.py scripts.  
These scripts will update both duckdb files in Zoo-API-Data/Sensource-API/data.  
After the db files are updated, right click them and select the download option.  
If this not the first time running this process on your device, select the replace option on the pop-up that appears.  

---
You can now navigate back to the Power BI report and refresh the data with the 'refresh' button in the Home tab.  
Your Attendance data should now be up-to-date.

---

The DailyTotalQuery.py and YTDQuery.py scripts can be used to display the data from the API calls to the terminal if needed.
They also save the data to a .csv file for potential use elsewhere.
Follow the same steps above to download the .csv file.

---

### Note:  

The YearToDate.py file does not collect data from the day that it is ran --> you must run DailyTotal.py to get today's gate data.
