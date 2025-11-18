#create a feature view for the data 
# We load the metadata from the feature pipeline. Remember that the FE metadata contains the start and end of the extraction window, the version of the feature group, etc.
# We login into the Hopswork project & create a reference to the feature store.
# We delete all the old feature views (usually, you don't have to do this step. Quite the opposite, you want to keep your old datasets. But Hopwork's free version limits you to 100 feature views. Thus, we wanted to keep our free version).
# We get the feature group based on the given version.
# We create a feature view with all the data from the loaded feature group.
# We create a training dataset using only the given time window.
# We create a snapshot of the metadata and save it to disk.
import logging 
from datetime import datetime, timezone 
import pandas 
import hopsworks
from feature_pipeline.utils import setup_logger
import hsfs
from typing import Optional 

def create_feature_store(
    feature_group_version: Optional[int] = None,
    start_datetime: Optional[datetime] = None,
    end_datetime: Optional[datetime] = None,
):
    """Create a new feature view version and training dataset
    based on the given feature group version and start and end datetimes.
    """
    if feature_group_version is None:
        feature_pipeline_metadata = utils.load_json("feature_pipeline_metadata.json")
        feature_group_version = feature_pipeline_metadata["feature_group_version"]

    if start_datetime is None or end_datetime is None:
        feature_pipeline_metadata = utils.load_json("feature_pipeline_metadata.json")
        start_datetime = datetime.strptime(
            feature_pipeline_metadata["export_datetime_utc_start"],
            feature_pipeline_metadata["datetime_format"],
        )
        end_datetime = datetime.strptime(
            feature_pipeline_metadata["export_datetime_utc_end"],
            feature_pipeline_metadata["datetime_format"],
        )
