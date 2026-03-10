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

# Commented Splunk logger initialization
# splunk_secret = get_secret(splunk_secret_name)
# logger = SplunkLogger(
#     token=splunk_secret["token"],
#     index=splunk_secret["index"],
#     meta_data={
#         "env": env,
#         "job_name": job_name,
#         "host": databricks_host,
#     },
# )

# Initialize Databricks Delta Metrics Logger
logger = SplunkLogger(
    token='',
    index='',
    meta_data={
        "env": env,
        "job_name": job_name,
        "host": databricks_host,
    },
)

print("Metrics logger initialized")
logger.flush()

# COMMAND ----------

def get_delta_data(spark, path: str, schema: Optional[StructType] = None) -> DataFrame:
    try:
        logger.info("Loading delta data", extra={"path": path})
        if schema:
            df = spark.read.format("delta").schema(schema).load(path)
        else:
            df = spark.read.format("delta").load(path)
        logger.info("Delta data loaded", extra={"path": path, "count": df.count()})
        logger.flush()
        return df
    except Exception as e:
        logger.error("Error loading delta data", extra={"path": path, "error": str(e)})
        logger.flush()
        raise

# COMMAND ----------

def log_and_load_data(spark, path: str, format: str, schema: Optional[StructType] = None) -> DataFrame:
    try:
        logger.info(f"Loading data from {format}", extra={"path": path})
        if schema:
            df = spark.read.format(format).schema(schema).load(path)
        else:
            df = spark.read.format(format).load(path)
        logger.info(f"Data loaded from {format}", extra={"path": path, "count": df.count()})
        logger.flush()
        return df
    except Exception as e:
        logger.error(f"Error loading data from {format}", extra={"path": path, "error": str(e)})
        logger.flush()
        raise

# COMMAND ----------

def write_delta_table(df: DataFrame, path: str, mode: str = "overwrite"):
    try:
        logger.info("Writing delta table", extra={"path": path, "mode": mode})
        df.write.format("delta").mode(mode).save(path)
        logger.info("Delta table written", extra={"path": path})
        logger.flush()
    except Exception as e:
        logger.error("Error writing delta table", extra={"path": path, "error": str(e)})
        logger.flush()
        raise

def write_delta_data(df: DataFrame, path: str, mode: str = "overwrite"):
    try:
        logger.info("Writing delta data", extra={"path": path, "mode": mode})
        df.write.format("delta").mode(mode).save(path)
        logger.info("Delta data written", extra={"path": path})
        logger.flush()
    except Exception as e:
        logger.error("Error writing delta data", extra={"path": path, "error": str(e)})
        logger.flush()
        raise

def log_and_write_unity_table(df: DataFrame, path: str, mode: str = "overwrite"):
    try:
        logger.info("Writing unity table", extra={"path": path, "mode": mode})
        df.write.format("unity").mode(mode).save(path)
        logger.info("Unity table written", extra={"path": path})
        logger.flush()
    except Exception as e:
        logger.error("Error writing unity table", extra={"path": path, "error": str(e)})
        logger.flush()
        raise

def log_and_write_parquet_data(df: DataFrame, path: str, mode: str = "overwrite"):
    try:
        logger.info("Writing parquet data", extra={"path": path, "mode": mode})
        df.write.format("parquet").mode(mode).save(path)
        logger.info("Parquet data written", extra={"path": path})
        logger.flush()
    except Exception as e:
        logger.error("Error writing parquet data", extra={"path": path, "error": str(e)})
        logger.flush()
        raise

def log_and_write_csv_data(df: DataFrame, path: str, mode: str = "overwrite"):
    try:
        logger.info("Writing csv data", extra={"path": path, "mode": mode})
        df.write.format("csv").mode(mode).save(path)
        logger.info("CSV data written", extra={"path": path})
        logger.flush()
    except Exception as e:
        logger.error("Error writing csv data", extra={"path": path, "error": str(e)})
        logger.flush()
        raise

# COMMAND ----------

def flush_logger_on_exit():
    logger.flush()

atexit.register(flush_logger_on_exit)
