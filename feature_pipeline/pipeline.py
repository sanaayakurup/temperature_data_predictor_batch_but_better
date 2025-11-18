##A script that calls various components of the pipeline
##Extract the data from the api 
##clean the data 
##validate the data 
##Load into feature store
from feature_pipeline.etl.extract import extract_data
from feature_pipeline.etl.transform import transform_data
from feature_pipeline.etl.load import to_feature_store
from feature_pipeline.utils import setup_logger
from feature_pipeline.utils import save_json
from feature_pipeline.settings import SETTINGS
import pandas as pd 
import logging
from datetime import datetime, timezone
import os
#set env vars for dates we want 
from dotenv import load_dotenv



#Load env vars 
logger_name=SETTINGS["LOGGER_NAME"]
data_url=SETTINGS["API_URL"]
start_date=SETTINGS["START_DATE_DATA_PULL"]
fs_api_key=SETTINGS["FEATURE_STORE_API_KEY"]
fs_project_name=SETTINGS["FEATURE_STORE_PROJECT_NAME"]
my_logger=setup_logger(logger_name) 

def run_etl_pipeline(data_url,start_date,fs_api_key,fs_project_name,feature_store_version):
    my_logger.info(f"Extracting data from API.")
    pulled_data,metadata=extract_data(data_url,start_date)
    my_logger.info(f"{datetime.now()}:Data Pull complete from the pipeline")
    my_logger.info(f"{datetime.now()}:Data Transformation started from the pipeline")
    transformed_data=transform_data(pulled_data)
    print(transformed_data.head())
    my_logger.info(f"{datetime.now()}:Data Transformation completed from the pipeline")
    my_logger.info(f"{datetime.now()}:Data Validation started from the pipeline")
    my_logger.info(f"{datetime.now()}:Data Validation completed from the pipeline")
    my_logger.info(f"{datetime.now()}:Validated Data Loading started from the pipeline")
    to_feature_store(transformed_data,fs_api_key,fs_project_name,feature_store_version)
    metadata["feature_group_version"] = feature_store_version
    save_json(metadata, file_name="feature_pipeline_metadata.json")

    my_logger.info(f"{datetime.now()}:Validated Data Loading completed from the pipeline")

if __name__=="__main__":
    run_etl_pipeline(data_url,start_date,fs_api_key,fs_project_name,1)