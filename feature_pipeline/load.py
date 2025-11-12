#Load the data into a feature store 
import pandas as pd 
import numpy as np 
from utils import setup_logger
import os
from dotenv import load_dotenv
import logging
import hopsworks
from datetime import datetime, timezone
from hsfs.feature_group import FeatureGroup
load_dotenv()

#Load env vars 
logger_name=os.getenv("LOGGER_NAME")
my_logger = logging.getLogger(logger_name)

def to_feature_store(transformed_data:pd.DataFrame,api_key,project_name, feature_group_version: int):
    """This function takes in a pandas DataFrame (and a validation expectation suite later),
    (performs validation on the data using the suite) and then saves the data to a
    feature store in the feature store.
    """
    # Connect to feature store.
    project = hopsworks.login(api_key_value=api_key, project=project_name)
    feature_store = project.get_feature_store()
    my_logger.info(f"{datetime.now()}:Connected to HP Feature store")
    #create a feature group 
    energy_feature_group = feature_store.get_or_create_feature_group(
        name="temp_data_pune_mississauga",
        version=feature_group_version,
        description="pune_mississauga hourly energy consumption data. Data is uploaded from api.",
        primary_key=["location"],
        event_time="date",
        online_enabled=False,
        #expectation_suite=validation_expectation_suite,
)
       # Upload data.
    energy_feature_group.insert(
            features=transformed_data,
            overwrite=False,
            write_options={
                "wait_for_job": True,
            },
        )
        # Add feature descriptions.
    feature_descriptions = [
        {
            "name": "date",
            "description": """
                            Datetime interval in UTC when the data was observed.
                            """
        },
        {
            "name": "location",
            "description": """
                            Pune or Mississauga
                            """
        },
        {
            "name": "temperature_2m",
            "description": """
                            Temp we want to pred
                            """
        },
        {
            "name": "rain",
            "description": """
                            Rain mm
                            """
        },
                {
            "name": "wind_speed_10m",
            "description": """
                            wind_speed_10m
                            """
        },
                {
            "name": "pressure_msl",
            "description": """
                            pressure_msl
                            """
        },
        {
            "name": "latitude",
            "description": """
                            latitude
                            """
        },
        {
            "name": "longitude",
            "description": """
                            longitude
                            """
        },
        

    ]
    for description in feature_descriptions:
        energy_feature_group.update_feature_description(
            description["name"], description["description"]
        )

    # Update statistics.
    energy_feature_group.statistics_config = {
        "enabled": True,
        "histograms": True,
        "correlations": True,
    }
    energy_feature_group.update_statistics_config()
    energy_feature_group.compute_statistics()

    return energy_feature_group

    
