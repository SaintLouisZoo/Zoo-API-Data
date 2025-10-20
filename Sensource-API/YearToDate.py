import os
import requests
import pandas as pd
import duckdb
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging
import sys
from rich.logging import RichHandler
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

# Configure logging
logger = logging.getLogger("GateCountFetcher")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# Rich console handler
rich_handler = RichHandler(rich_tracebacks=True, show_time=False, show_level=True, show_path=False)
rich_handler.setFormatter(formatter)
logger.addHandler(rich_handler)

# File handler
file_handler = logging.FileHandler(os.path.join(os.path.dirname(__file__), '.', 'logs', 'GateCount.log'))
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

class GateCountFetcher:
    def __init__(self, env_path='.env'):
        load_dotenv(env_path)
        self.api_token = os.getenv("SENSOURCE_TOKEN")
        self.south_gate = 'decd9257-660d-412b-a019-1bd44f0aecd9'
        self.tlw_gate = 'b49b0f74-7af5-480c-a8ef-bb1a090731cf'

    def get_gate_count(self, date=None, to_db=True):
        if date is None:
            date = datetime.today().strftime('%m-%d-%Y')
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.api_token}"
        }
        relativeDate = "custom"
        startDate = (datetime(datetime.today().year, 1, 1) - timedelta(days=1)).strftime('%m-%d-%Y') # Query from start of year
        endDate = datetime.today().strftime('%m-%d-%Y')
        dateGroupings = "minute(15)"  # get gate data every 15 minutes
        entityType = "sensor"
        excludeClosedHours = "true"
        metrics = "ins%2Couts"  
        
        # Refrence link: https://vea.sensourceinc.com/api-docs/
        url = (
            f"https://vea.sensourceinc.com/api/data/traffic?relativeDate={relativeDate}&startDate={startDate}"
            f"&endDate={endDate}&dateGroupings={dateGroupings}&entityType={entityType}"
            f"&excludeClosedHours={excludeClosedHours}&metrics={metrics}"
        )
        response = requests.get(url, headers=headers).json()
        
        df = pd.DataFrame.from_dict(response['results'])
        
        # Find the correct column names dynamically
        date_col = [col for col in df.columns if 'recordDate' in col][0]  # Dynamic column detection
        
        df = df[[date_col, 'name', 'sumins', 'sumouts']]  # Include sumouts
        df = df.rename(columns={date_col: 'DateTime', 'name': 'location', 'sumins': 'Ingress', 'sumouts': 'Egress'})
        df['DateTime'] = pd.to_datetime(df['DateTime'])
        df['DateTime'] = df['DateTime'].dt.tz_convert('America/Chicago')
        df['DateTime'] = df['DateTime'].dt.tz_localize(None)
        df = df[(df.Ingress != 0) | (df.Egress != 0)]  # Filter for non-zero ingress OR egress
        df['Gate'] = df['location'].apply(lambda x: 'THE LIVING WORLD' if x in ['Treetop', 'TLW1'] else 'SOUTH GATE')
        df = df.groupby(['DateTime', 'Gate'], as_index=False)[['Ingress', 'Egress']].sum() 
        if to_db:
            self.write_to_db(df)
        return df

    def write_to_db(self, df):
        con = duckdb.connect(database=os.path.join(os.path.dirname(__file__), '.', 'data', 'YTD.data.duckdb'))
        con.execute("DROP TABLE IF EXISTS GateCount;")
        con.execute("CREATE TABLE GateCount (DateTime TIMESTAMP, Gate VARCHAR, Ingress INTEGER, Egress INTEGER);")  
        con.execute("INSERT INTO GateCount SELECT * FROM df;")
        logger.info("Gate count data written to DuckDB.")

if __name__ == "__main__":
    logger.info("Sending gate count data to DuckDB")
    fetcher = GateCountFetcher()
    try:
        fetcher.get_gate_count()
        logger.info("Success")
    except Exception as e:
        logger.warning(f"Failure! {e}")