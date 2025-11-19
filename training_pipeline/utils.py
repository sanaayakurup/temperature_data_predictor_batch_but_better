import logging
import sys
import json
import logging
from pathlib import Path
from feature_pipeline.settings import SETTINGS
from typing import Union, Optional
import pandas as pd 
import wandb
#load env vars 
logger_name = SETTINGS["LOGGER_NAME"]


def setup_logger(name="app_logger", log_file=logger_name, level=logging.DEBUG):
    logger = logging.getLogger(name)

    if not logger.handlers:
        if isinstance(level, str):
            level = getattr(logging, level.upper(), logging.DEBUG)

        logger.setLevel(level)

        # Define a formatter that includes module & logger name
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(module)s - %(levelname)s - %(message)s"
        )

        # File handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Capture uncaught exceptions
        def handle_exception(exc_type, exc_value, exc_traceback):
            if issubclass(exc_type, KeyboardInterrupt):
                sys.__excepthook__(exc_type, exc_value, exc_traceback)
                return
            logger.error("Uncaught exception",
                         exc_info=(exc_type, exc_value, exc_traceback))

        sys.excepthook = handle_exception

    return logger

def save_json(data: dict, file_name: str, save_dir: str = SETTINGS["OUTPUT_DIR"]):
    """
    Save a dictionary as a JSON file.

    Args:
        data: data to save.
        file_name: Name of the JSON file.
        save_dir: Directory to save the JSON file.

    Returns: None
    """

    data_path = Path(save_dir) / file_name
    with open(data_path, "w") as f:
        json.dump(data, f)


def load_json(file_name: str, save_dir: str = SETTINGS["OUTPUT_DIR"]) -> dict:
    """
    Load a JSON file.

    Args:
        file_name: Name of the JSON file.
        save_dir: Directory of the JSON file.

    Returns: Dictionary with the data.
    """

    data_path = Path(save_dir) / file_name
    if not data_path.exists():
        raise FileNotFoundError(f"Cached JSON from {data_path} does not exist.")

    with open(data_path, "r") as f:
        return json.load(f)
    

def init_wandb_run(
    name: str,
    group: Optional[str] = None,
    job_type: Optional[str] = None,
    add_timestamp_to_name: bool = False,
    run_id: Optional[str] = None,
    resume: Optional[str] = None,
    reinit: bool = False,
    project: str = SETTINGS["WANDB_PROJECT"],
    entity: str = SETTINGS["WANDB_ENTITY"],
):       
    "wrapper for the wandb init "
    if add_timestamp_to_name:
        name = f"{name}_{pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')}"
    run=wandb.init(
        project=project,
        entity=entity,
        name=name,
        group=group,
        job_type=job_type,
        id=run_id,
        reinit=reinit,
        resume=resume)
    return run 