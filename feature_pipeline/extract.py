##extract data for a given time frame for one region 
import openmeteo_requests
import pandas as pd 
from utils import setup_logger
import requests_cache
from retry_requests import retry
from utils import setup_logger
import logging
from datetime import datetime, timezone
import os
#set env vars for dates we want 
from dotenv import load_dotenv
load_dotenv()

#Load env vars 
logger_name=os.getenv("LOGGER_NAME")
data_url=os.getenv("API_URL")
start_date=os.getenv("START_DATE_DATA_PULL")

#get loger and start logging
#my_logger=setup_logger(logger_name) 
#ATTENTION!!this will change to just getting the logger, when run from main
my_logger = logging.getLogger(logger_name)
   
def extract_data(data_url,start_date,all_data_or_current_hour="all"):
    """
    Pulls in data of the full time range, or for the current hour
    Give arg "all" if you want data to be pulled from 2010 to current hour, else hour
    """
    try:     
        cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
        retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
        openmeteo = openmeteo_requests.Client(session = retry_session)
        #if we pull all the data, set start and end date, else set start and end date to current hour 
        if all_data_or_current_hour=="all":
            start_date=start_date #"2010-01-01"
            end_date=datetime.now(timezone.utc).strftime('%Y-%m-%d')#current hour as str
        else:
            start_date=datetime.now(timezone.utc).strftime('%Y-%m-%d') #current hour as str
            end_date=datetime.now(timezone.utc).strftime('%Y-%m-%d') #current hour as str
        params = {
        "latitude": [18.5204, 43.5890],     # Pune, Mississauga
        "longitude": [73.8567, -79.6441],   # Pune, Mississauga
        "start_date": start_date, #can make this dynamic 
        "end_date": end_date,
        "hourly": ["temperature_2m", "rain", "wind_speed_10m", "pressure_msl"],
            }
        
        responses = openmeteo.weather_api(data_url, params=params)
        all_dfs = []
        locations = ["Pune", "Mississauga"]
        for idx, response in enumerate(responses):
            lat = response.Latitude()
            lon = response.Longitude()
            hourly = response.Hourly()
            temps = hourly.Variables(0).ValuesAsNumpy()
            rain = hourly.Variables(1).ValuesAsNumpy()
            wind_speed_10m = hourly.Variables(2).ValuesAsNumpy()
            pressure_msl = hourly.Variables(3).ValuesAsNumpy()
            

            time_index = pd.date_range(
                start = pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end   = pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq  = pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            )    
            df = pd.DataFrame({
            "date":           time_index,
            "temperature_2m": temps,
            "rain": rain,
            "wind_speed_10m": wind_speed_10m,
            "pressure_msl": pressure_msl,
            "latitude":       lat,
            "longitude":      lon,
            "location":       locations[idx]
            })
            all_dfs.append(df)
        combined_df = pd.concat(all_dfs, ignore_index=True)
        print(combined_df.head())
        my_logger.info(f"{datetime.now()}:Data Pulled in for dates {combined_df.date.min(),combined_df.date.max()}")
        my_logger.info(f"{datetime.now()}:Data Pulled in for params {params}")
        my_logger.info(f"{datetime.now()}:Data Pulled complete")
        #do i really need path here? I can just write it to gcp-change later
        #path_to_write=f"{all_data_or_current_hour}_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
        #combined_df.to_csv(f'/tmp/temp_data_{path_to_write}.csv')
        return combined_df #f'/tmp/temp_data_{path_to_write}.csv'        
    except Exception as e:
        print(e)
        my_logger.info(f"{datetime.now()}:Error!:{e}")


"""if __name__=="__main__":
    extract_data(data_url,start_date,"all")"""


