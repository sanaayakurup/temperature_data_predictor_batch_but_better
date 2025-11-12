##A script that calls various components of the pipeline
##Extract the data from the api 
##clean the data 
##validate the data 
##Load into feature store
from feature_pipeline.extract import extract_data
from feature_pipeline.transform import transform_data
from feature_pipeline.load import to_feature_store
from utils import setup_logger

import pandas as pd 
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
api_key=os.getenv("FEATURE_STORE_API_KEY")
project_name=os.getenv("FEATURE_STORE_PROJECT_NAME")
my_logger=setup_logger(logger_name) 

def run_etl_pipeline(data_url,start_date,api_key,project_name,version):
    my_logger.info(f"Extracting data from API.")
    pulled_data=extract_data(data_url,start_date)
    my_logger.info(f"{datetime.now()}:Data Pull complete from the pipeline")
    my_logger.info(f"{datetime.now()}:Data Transformation started from the pipeline")
    transformed_data=transform_data(pulled_data)
    print(transformed_data.head())
    my_logger.info(f"{datetime.now()}:Data Transformation completed from the pipeline")
    my_logger.info(f"{datetime.now()}:Data Validation started from the pipeline")
    my_logger.info(f"{datetime.now()}:Data Validation completed from the pipeline")
    my_logger.info(f"{datetime.now()}:Validated Data Loading started from the pipeline")
    to_feature_store(transformed_data,api_key,project_name,version)
    my_logger.info(f"{datetime.now()}:Validated Data Loading completed from the pipeline")

if __name__=="__main__":
    run_etl_pipeline(data_url,start_date,api_key,project_name,1)