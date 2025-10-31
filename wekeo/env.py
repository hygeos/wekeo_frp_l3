from dotenv import load_dotenv, find_dotenv
from os import environ
# from pathlib import Path
# from typing import Optional
# from core import log

load_dotenv(dotenv_path=find_dotenv(usecwd=True)) 


def getvar(
    envvar: str,
    default = None,
    ):
    """
    Returns the value of environment variable `envvar`. If this variable is not defined, returns default.

    The environment variable can be defined in the users `.bashrc`, or in a file `.env`
    in the current working directory.

    Args:
        envvar: the input environment variable
        default: the default return, if the environment variable is not defined
    
    Returns:
        the requested environment variable or the default if the var is not defined and a default has been provided.
    """
    variable = None
    
    if envvar in environ:
        variable = environ[envvar]
        
    elif default is None:
        raise KeyError(f"{envvar} is not defined, and no default has been provided.")
        
    else:
        variable = default

    return variable
