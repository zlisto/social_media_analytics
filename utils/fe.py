import os
import pandas as pd
from pathlib import Path
from typing import *

DATE_FORMAT = '%Y-%m-%d %H:%M'


# Functions
def get_root() -> str:
    return str(Path(__file__).parent.parent)
