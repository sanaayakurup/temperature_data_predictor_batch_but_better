from training_pipeline.settings import SETTINGS
from training_pipeline.utils import setup_logger,save_json,load_json,init_wandb_run
import wandb 
import hopsworks
from sktime.forecasting.model_selection import temporal_train_test_split
import pandas as pd
logger_name=SETTINGS["LOGGER_NAME"]
fs_api_key=SETTINGS["FEATURE_STORE_API_KEY"]
fs_project_name=SETTINGS["FEATURE_STORE_PROJECT_NAME"]
fs_name=SETTINGS["FEATURE_STORE_NAME"]

#load env vars 
my_logger=setup_logger(logger_name) 


#read training data from feature view 
def load_dataset_from_feature_view(fs_api_key,
                                   fs_project_name,
                                   fs_name,
                                   feature_view_version: int,
                                   training_dataset_version:int,
                                   target,
                                   test_size):
                                   
    #initialise the project
    project=hopsworks.login(api_key_value=fs_api_key, project=fs_project_name)
    fs = project.get_feature_store()
    my_logger.info("project Conn to load dataset from fV done")
    with init_wandb_run(
    name="load_training_data", job_type="load_feature_view", group="dataset"
    ) as run:
        #get the feature view 
        feature_view=fs.get_feature_view(
                name=f"{fs_name}_view",
                version=feature_view_version)
        
        #get training data and labels 
        data,_=feature_view.get_training_data(training_dataset_version=training_dataset_version)
        #log metadata of feature view to wb
        fv_metadata = feature_view.to_dict()
        fv_metadata["query"] = fv_metadata["query"].to_string()
        fv_metadata["features"] = [f.name for f in fv_metadata["features"]]
        fv_metadata["link"] = feature_view._feature_view_engine._get_feature_view_url(
            feature_view
        )
        fv_metadata["feature_view_version"] = feature_view_version
        fv_metadata["training_dataset_version"] = training_dataset_version
        raw_data_at = wandb.Artifact(
            name=f"{fs_name}_view",
            type="feature_view",
            metadata=fv_metadata,
        ) #log to wandb
        run.log_artifact(raw_data_at)

        run.finish()
    with init_wandb_run(
        name="train_test_split", job_type="prepare_dataset", group="dataset"
    ) as run:
        run.use_artifact(f"{fs_name}_view:latest")
        y_train, y_test, X_train, X_test = prepare_data(data, target,test_size)

        for split in ["train", "test"]:
            split_X = locals()[f"X_{split}"]
            split_y = locals()[f"y_{split}"]

            split_metadata = {
                "timespan": [
                    split_X.index.get_level_values(-1).min(),
                    split_X.index.get_level_values(-1).max(),
                ],
                "dataset_size": len(split_X),
                "y_features": split_y.columns.tolist(),
                "X_features": split_X.columns.tolist(),
            }
            artifact = wandb.Artifact(
                name=f"split_{split}",
                type="split",
                metadata=split_metadata,
            )
            run.log_artifact(artifact)

        run.finish()

    return y_train, y_test, X_train, X_test

def prepare_data(data,target,test_size):
    """
    Structure the data for training:
    - Set the index as is required by sktime.
    - Prepare exogenous variables.
    - Prepare the time series to be forecasted.
    - Split the data into train and test sets.
    """

    # Set the index as is required by sktime.
    data["date"] = pd.PeriodIndex(data["date"], freq="H")
    data = data.set_index(["date","location"]).sort_index()

    # Prepare exogenous variables.
    X = data.drop(columns=[target])
    # Prepare the time series to be forecasted.
    y = data[[target]]

    y_train, y_test, X_train, X_test = temporal_train_test_split(y, X, test_size=test_size)

    return y_train, y_test, X_train, X_test    


if __name__=="__main__":
    load_dataset_from_feature_view(fs_api_key,
                                   fs_project_name,
                                   fs_name,
                                   1,
                                   1,
                                   "temperature_2m",
                                   0.2)






