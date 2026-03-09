# Databricks notebook source
from typing import TypeVar, Optional
from pyspark.sql.types import StructType, DataType, ArrayType, DateType
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timedelta
from distutils import util
from botocore.exceptions import ClientError
import json
import boto3
from base64 import b64encode, b64decode
from Crypto.Cipher import AES
from pyspark.sql.functions import udf
from pyspark.sql.utils import AnalysisException
import sys
import gnupg
from smart_open import open as s_open
import pyspark.sql.functions as F
import distutils
import requests
import pandas as pd
import io
import warnings
import atexit

# COMMAND ----------

param_env = "env"
param_job_name = "job_name"
param_host = "host"
env = dbutils.widgets.text(param_env, "dev")
job_name = dbutils.widgets.text(param_job_name, "commons")
databricks_host = dbutils.widgets.text(
    param_host, f"dataos-kc-{env}.cloud.databricks.com"
)

# COMMAND ----------

# MAGIC %run "./databricks_logger"

# COMMAND ----------

env = dbutils.widgets.get(param_env)
job_name = dbutils.widgets.get(param_job_name)
databricks_host = dbutils.widgets.get(param_host)
splunk_secret_name = f"{env}/k8s/p2retargeting/splunk"

print(f"env:{env}")
print(f"job_name:{job_name}")
print(f"databricks_host:{databricks_host}")

STATE_STARTED = "started"
STATE_FINISHED = "finished"
STATE_ERROR = "error"

# COMMAND ----------

# splunk_secret = get_secret(splunk_secret_name)
# logger = SplunkLogger(
#     token=splunk_secret["token"],
#     index=splunk_secret["index"],
#     meta_data={
#         "source": job_name,
#         "sourcetype": f"databricks:{source_type}",
#         "host": databricks_host,
#     },
# )

logger = SplunkLogger(
    token="",
    index="",
    meta_data={
        "source": job_name,
        "sourcetype": f"databricks:{source_type}",
        "host": databricks_host,
    },
)

# Exit safety flush
import atexit
def flush_logger_on_exit():
    try:
        if hasattr(logger, "batch_events") and len(logger.batch_events) > 0:
            logger.flush()
    except Exception:
        pass
atexit.register(flush_logger_on_exit)

# Update initialization messages
print(__get_event("INFO", f"Metrics logger initialized for {env} env"))
info(f"Metrics logger initialized for {env} env")
logger.flush()

# COMMAND ----------

# ... (rest of business logic unchanged, with logger.flush() added after writes, in exception blocks, and Splunk HTTP calls removed)
