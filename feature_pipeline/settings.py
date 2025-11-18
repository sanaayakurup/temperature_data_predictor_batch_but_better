##reads in env varaibles to avoid duplication across scripts 
#write a script to load env vars 

import os 
from dotenv import load_dotenv 
from pathlib import Path
from typing import Union

def load_env_vars()->dict:
    """
    Loads env vars from .env.default and .env files 
    Args:
    Returns:
        dict with env vars

    """
    load_dotenv()
    return dict(os.environ)

SETTINGS = load_env_vars()


