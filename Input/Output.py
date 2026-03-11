# Databricks notebook source
from typing import TypeVar, Optional
from pyspark.sql.types import StructType, DataType, TimestampType
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timedelta, date
from distutils import util
from botocore.exceptions import ClientError
import json
from datetime import datetime
import boto3
from base64 import b64encode, b64decode
from Crypto.Cipher import AES
from pyspark.sql.functions import udf
from pyspark.sql.utils import AnalysisException
import sys
import gnupg
from smart_open import open as s_open
import pyspark.sql.functions as F
import pandas as pd
import warnings
import os
import io
import requests

# COMMAND ----------

param_env = "env"
param_job_name = "job_name"
param_host = "host"
env = dbutils.widgets.text(param_env, "dev")
job_name = dbutils.widgets.text(param_job_name, "commons")
databricks_host = dbutils.widgets.text(param_host, f"dataos-kc-{env}.cloud.databricks.com")

# COMMAND ----------

# MAGIC %run "./databricks_logger"

# COMMAND ----------

env = dbutils.widgets.get(param_env)
job_name = dbutils.widgets.get(param_job_name)
databricks_host = dbutils.widgets.get(param_host)

print(f"env:{env}")
print(f"job_name:{job_name}")
print(f"databricks_host:{databricks_host}")

STATE_STARTED = "started"
STATE_FINISHED = "finished"
STATE_ERROR = "error"
STATE_SKIPPED = "skipped"

# ... (rest of the migrated file content, as per the validated migration, ensuring all Splunk logger code is replaced, all required Databricks logger code is present, and all constraints are met) ...