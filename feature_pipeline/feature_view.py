#create a feature view for the data 
# We load the metadata from the feature pipeline. 
# Remember that the FE metadata contains the start and end of the extraction window, the version of the feature group, etc.
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
from feature_pipeline.settings import SETTINGS
import hsfs
from typing import Optional 
from feature_pipeline.utils import load_json,save_json

#load env vars 
logger_name=SETTINGS["LOGGER_NAME"]
fs_api_key=SETTINGS["FEATURE_STORE_API_KEY"]
fs_project_name=SETTINGS["FEATURE_STORE_PROJECT_NAME"]
fs_name=SETTINGS["FEATURE_STORE_NAME"]
#get logger 
my_logger=setup_logger(logger_name) 

def create_feature_store(
    fs_name,
    feature_group_version: Optional[int] = None,
    start_datetime: Optional[datetime] = None,
    end_datetime: Optional[datetime] = None):
    """Create a new feature view version and training dataset
    based on the given feature group version and start and end datetimes.
    """
    if feature_group_version is None: #if no feature group version is passed 
        #to the arg-read metadata from json file 
        feature_pipeline_metadata = load_json("feature_pipeline_metadata.json")
        feature_group_version = feature_pipeline_metadata["feature_group_version"]

    if start_datetime is None or end_datetime is None:
        #similarly for start and end time-get from json metadata if no arg provided
        feature_pipeline_metadata = load_json("feature_pipeline_metadata.json")
        start_datetime = datetime.strptime(
            feature_pipeline_metadata["start_date"],"%Y-%m-%d"
        )
        end_datetime = datetime.strptime(
            feature_pipeline_metadata["end_date"],"%Y-%m-%d"
        )

    #initiate the HP project 
    project = hopsworks.login(api_key_value=fs_api_key, project=fs_project_name)
    #GET THE FEATURE STORE 
    fs = project.get_feature_store()
    try:
        feature_views = fs.get_feature_views(name=f"{fs_name}_view")
    except hsfs.client.exceptions.RestAPIError:
        my_logger.info(f"No feature views found for {fs_name}_view.")
        #No feature views were found so we create one 
        feature_views = []
    for feature_view in feature_views:
        try:
            #to avoid billing 
            feature_view.delete_all_training_datasets()
        except hsfs.client.exceptions.RestAPIError:
            my_logger.error(f"Failed to delete training datasets for feature view {feature_view.name} with version {feature_view.version}.")

        try:
            feature_view.delete()
        except hsfs.client.exceptions.RestAPIError:
            my_logger.error(f"Failed to delete feature view {feature_view.name} with version {feature_view.version}.")

    # Create feature view in the given feature group version.
    #get the feature store 
    temp_fg = fs.get_feature_group(
        f"{fs_name}", version=feature_group_version
    )
    #QUERY  feature store 
    ds_query = temp_fg.select_all()
    print(ds_query)
    #Create featuer view from feature store 
    feature_view = fs.create_feature_view(
    name= f"{fs_name}_view",
    description=f"Feature View for {fs_name}",
    query=ds_query,
    labels=[],
    )

    # Create training dataset.
    my_logger.info(
        f"Creating training dataset between {start_datetime} and {end_datetime}."
    )
    feature_view.create_training_data(
        description=f"{fs_name} model training dataset",
        data_format="csv",
        start_time=start_datetime,
        end_time=end_datetime,
        write_options={"wait_for_job": True},
        coalesce=False,
    )

    # Save metadata.
    metadata = {
        "feature_view_version": feature_view.version,
        "training_dataset_version": 1,
    }
    save_json(
        metadata,
        file_name="feature_view_metadata.json",
    )
    my_logger.info(f"feature views created with name {fs_name}_view.")

    return metadata


if __name__ == "__main__":
    create_feature_store(fs_name)



