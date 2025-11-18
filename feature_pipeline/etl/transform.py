import pandas as pd 
import numpy as np 
import os 
from dotenv import load_dotenv
import logging 
from feature_pipeline.settings import SETTINGS

#Load env vars 
logger_name=SETTINGS["LOGGER_NAME"]
my_logger = logging.getLogger(logger_name)

def transform_data(data):
    data.date=pd.to_datetime(data.date)
    location_map={'Pune':1,'Mississauga':2}
    data.location=data.location.map(location_map)
    return data
