import copy
import logging
import loguru
import os
import sys
from loguru import logger
from pathlib import Path
from utils.fe import *

__author__ = 'kqureshi'


LOG_PATH = get_root() + '/logs/'
CONSOLE_FORMAT = (
    "<green>[{time:YYYY-MM-DD HH:mm:ss.SSS}]</green> <level>[{level}]</level> "
    "<cyan>[{module}:{function}:{line}]</cyan> <level>{message}</level>"
)
FILE_FORMAT = "[{time:YYYY-MM-DD HH:mm:ss.SSS}] [{level}] [{function}] {message}"
SIMPLE_FORMAT = '<green>[{time:YYYY-MM-DD HH:mm:ss.SSS}]</green> <level>[{level}]</level> <level>{message}</level>'

config = {
    "handlers": [
        {"sink": sys.stdout, "format": CONSOLE_FORMAT, 'diagnose': False},
    ],
    "levels": [
        {"name": "DEBUG", "color": "<blue><dim>"},
        {"name": "INFO", "color": "<white>"},
        {"name": "WARNING", "color": "<yellow>"},
        {"name": "ERROR", "color": "<red>"},
        {"name": "CRITICAL", 'color': '<red><bold>'},
    ]
}
# Default logger
logger.configure(**config)

def get_logger(name: str = None, level: str = 'DEBUG') -> loguru.logger:
    """ Get a new logger with given log level
    :param name: an optional name parameter that is used to write the log to file.
    :param level: minimum logging level.
    :return: a loguru logger
    """
    Path(LOG_PATH).mkdir(parents=True, exist_ok=True)
    if name is not None:
        filename = '{}/{}.log'.format(LOG_PATH, name)
        logger.add(filename, format=FILE_FORMAT, level=level, rotation='1 week', diagnose=False)
    return logger