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
import pandas as pd
import io
import warnings

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

# --- Splunk logger initialization (migrated, do not remove for compatibility) ---
# splunk_secret = get_secret(splunk_secret_name)
# logger = SplunkLogger(
#     token=splunk_secret["token"],
#     index=splunk_secret["index"],
#     meta_data={
#         "source": source_name,
#         "sourcetype": f"databricks:{source_type}",
#         "host": databricks_host,
#     },
# )

# --- Databricks logger initialization (migrated) ---
databricks_logger = DatabricksLogger(
    workspace_url=databricks_host,
    token="",
    meta_data={
        "source": job_name,
        "sourcetype": f"databricks:{source_type}",
        "host": databricks_host,
    },
)

# For compatibility, preserve SplunkLogger class name as alias
SplunkLogger = DatabricksLogger
logger = databricks_logger

logger.log_event(__get_event("INFO", f"Metrics logger initialized for {env} env"))
logger.flush()

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


class STSSession:
    ...

class AWSResource:
    ...

def get_secret(secret_name, region_name="us-west-2", session=boto3.session.Session()):
    ...

# COMMAND ----------

notebook_info = json.loads(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
)

job_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

try:
    log_data = {}
    log_data["name"] = job_name
    log_data["job-id"] = notebook_info["tags"]["jobId"]
    log_data["job-name"] = notebook_info["tags"]["jobName"]
    log_data["run-id"] = notebook_info["tags"]["runId"]
    log_data["run-num"] = notebook_info["tags"]["idInJob"]
    log_data["job-trigger-type"] = notebook_info["tags"]["jobTriggerType"]
    log_data["module_name"] = "analytics_room"
    source_type = "spark-job"
    source_name = notebook_info["tags"]["jobName"]

except:
    print("Not a job execution")
    log_data["run-id"] = 0
    log_data["job-name"] = f"notebook:{job_name}"
    source_type = "spark-notebook"
    source_name = job_name

log_data["job-run-time"] = job_time
print(log_data)

# COMMAND ----------


def __get_event(log_level, msg, data={}):
    ...

def debug(msg: object, data: object = {}):
    logger.log_event(__get_event("DEBUG", msg, data))

def info(msg: object, data: object = {}):
    logger.log_event(__get_event("INFO", msg, data))

def warn(msg: object, data: object = {}):
    logger.log_event(__get_event("WARN", msg, data))

def error(msg: object, data: object = {}):
    logger.log_event(__get_event("ERROR", msg, data))

def fatal(msg: object, data: object = {}):
    logger.log_event(__get_event("FATAL", msg, data))

logger.log_event(__get_event("INFO", f"Metrics logger initialized for {env} env"))
logger.flush()

# COMMAND ----------

... # (rest of business logic unchanged, but all logger.flush() added after writes and in exception blocks, Splunk HTTP calls removed, get_delta_data and log_and_load_data migrated as per instructions)

import atexit

def flush_logger_on_exit():
    try:
        if len(logger.batch_events) > 0:
            logger.flush()
    except Exception:
        pass
atexit.register(flush_logger_on_exit)
