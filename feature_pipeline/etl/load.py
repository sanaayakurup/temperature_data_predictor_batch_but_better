import pandas as pd
import os
import logging
import hopsworks
from datetime import datetime
from dotenv import load_dotenv
from feature_pipeline.settings import SETTINGS

logger_name = SETTINGS["LOGGER_NAME"]
my_logger = logging.getLogger(logger_name)


def to_feature_store(transformed_data: pd.DataFrame, api_key, project_name, feature_group_version: int):
    """Save a pandas DataFrame to the Hopsworks Feature Store safely."""
    # Connect to Hopsworks project
    project = hopsworks.login(api_key_value=api_key, project=project_name)
    feature_store = project.get_feature_store()
    my_logger.info(f"{datetime.now()}: Connected to Hopsworks Feature Store")

    # Create or get the feature group
    energy_feature_group = feature_store.get_or_create_feature_group(
        name="temp_data_pune_mississauga",
        version=feature_group_version,
        description="Hourly temperature and weather data for Pune and Mississauga.",
        primary_key=["location", "date"],
        event_time="date",
        online_enabled=False,
    )

    # skip if there is no data
    if transformed_data is None or transformed_data.empty:
        my_logger.warning(f"{datetime.now()}: No data to insert — skipping.")
        return energy_feature_group

    # Insert data (non-destructive)
    try:
        insert_result = energy_feature_group.insert(
            features=transformed_data,
            overwrite=False,              # append/upsert only
            write_options={"wait_for_job": True},
        )
        my_logger.info(f"{datetime.now()}: Inserted {len(transformed_data)} rows.")
    except Exception as e:
        my_logger.error(f"{datetime.now()}: Failed to insert data — {e}")
        return energy_feature_group

    # Add feature descriptions
    feature_descriptions = [
        ("date", "Datetime interval in UTC when the data was observed."),
        ("location", "Pune or Mississauga."),
        ("temperature_2m", "Temperature at 2 m height (°C)."),
        ("rain", "Rainfall (mm)."),
        ("wind_speed_10m", "Wind speed at 10 m (m/s)."),
        ("pressure_msl", "Mean sea-level pressure (hPa)."),
        ("latitude", "Latitude of observation."),
        ("longitude", "Longitude of observation."),
    ]
    for name, desc in feature_descriptions:
        energy_feature_group.update_feature_description(name, desc)

    # Enable statistics
    energy_feature_group.statistics_config = {
        "enabled": True,
        "histograms": True,
        "correlations": True,
    }
    energy_feature_group.update_statistics_config()

    #only compute statistics if commits exist
    try:
        commits = energy_feature_group.get_commit_details()
        if commits:
            energy_feature_group.compute_statistics()
            my_logger.info(f"{datetime.now()}: Computed statistics successfully.")
        else:
            my_logger.warning(f"{datetime.now()}: No commits found — skipping compute_statistics().")
    except Exception as e:
        my_logger.warning(f"{datetime.now()}: compute_statistics() failed or skipped — {e}")

    return energy_feature_group
