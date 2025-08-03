import os
os.getcwd()

from etl_functions import *

# Do this to import needed libraries
import numpy as np # type: ignore
import pandas as pd # type: ignore

import pysftp # type: ignore
import tempfile
import zipfile

import subprocess
from io import BytesIO

import paramiko
import getpass
import time

from datetime import date, timedelta, datetime
from typing import Tuple, Dict, List, Optional

import sys

from tqdm.notebook import tqdm
from tqdm.version import __version__ as tqdm__version__

import warnings
warnings.simplefilter('ignore')

# Logger
logger = logging.getLogger(__name__)
my_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
logger.info(f"Starting data pipeline at {my_date}")
logger.info("----------------------------------------------")

# Step 1: extract data
t0 = time.time()
download_data()
t1 = time.time()
logger.info("Step 1: Done")
logger.info("---> Data downloaded in", str(t1-t0), "seconds", "\n")

# Step 2: unzip data
t0 = time.time()
extract_data()
t1 = time.time()
logger.info("Step 2: Done")
logger.info("---> Data unzipped in", str(t1-t0), "seconds", "\n")

# Step 3: Transform data
t0 = time.time()
transform_data()
t1 = time.time()
logger.info("Step 3: Done")
logger.info("---> Data transformed in", str(t1-t0), "seconds", "\n")

# Step 4: Upload data
t0 = time.time()
upload_data()
t1 = time.time()
logger.info("Step 4: Done")
logger.info("---> Data uploaded in", str(t1-t0), "seconds", "\n")

my_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
logger.info(f"Data pipeline finished at {my_date}")

# Step 5: Run import script
t0 = time.time()
run_import_script()
t1 = time.time()
logger.info("Step 5: Done")
logger.info("---> Data imported in", str(t1-t0), "seconds", "\n")

my_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
logger.info(f"Data import finished at {my_date}")

# Step 6: Send email
t0 = time.time()
# send_email()
t1 = time.time()
logger.info("Step 6: Done")
logger.info("---> Email sent in", str(t1-t0), "seconds", "\n")

my_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
logger.info(f"Email sent at {my_date}")

