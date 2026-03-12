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

print(f"env:{env}")
print(f"job_name:{job_name}")
print(f"databricks_host:{databricks_host}")

STATE_STARTED = "started"
STATE_FINISHED = "finished"
STATE_ERROR = "error"

# COMMAND ----------

splunk_secret_name = f"{env}/k8s/p2retargeting/splunk"

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

def __get_event(log_level, msg, data={}):
    event = {"level": log_level, "message": msg}
    if isinstance(data, dict):
        event.update(data)
    elif isinstance(data, str) and data.strip():
        event["data"] = data
    event.update(log_data)
    return json.dumps(event)

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

print(__get_event("INFO", f"Metrics logger initialized for {env} env"))
info(f"Metrics logger initialized for {env} env")
logger.flush()

# COMMAND ----------

import atexit

def flush_logger_on_exit():
    try:
        if len(logger.batch_events) > 0:
            logger.flush()
    except Exception:
        pass

atexit.register(flush_logger_on_exit)

# COMMAND ----------

def get_delta_data(source: str, check_on: str = "s3") -> DataFrame:
    if check_on == "unity":
        return spark.read.table(source)
    return spark.read.format("delta").load(source)

# COMMAND ----------

def log_and_load_data(source_info: dict, log_data: dict) -> DataFrame:
    try:
        info(
            f"Starting {log_data['task']}",
            data={
                "task": log_data["task"],
                "state": STATE_STARTED,
                "source_format": source_info.get("format", "unknown"),
                "source_using_sql": source_info.get("using_sql", False),
                "source_location": source_info.get("path", ""),
            },
        )
        if source_info["format"] == "parquet":
            df = get_parquet_data(source_info["path"])
        elif source_info["format"] == "delta":
            df = get_delta_data(source_info["path"])
        elif source_info["format"] == "csv":
            df = get_csv_data(source_info["path"], source_info["separator"])
        elif source_info["format"] == "unity":
            df = get_unity_data(source_info["path"])
        elif source_info["format"] == "redshift":
            df = get_redshift_data(
                source_info["constants"], source_info["create_session"]
            )
        elif source_info["format"] == "metastore":
            if source_info["using_sql"] == True:
                df = spark.sql(source_info["metastore_query"])
            else:
                df = spark.table(source_info["database"] + "." + source_info["table"])
        else:
            df = spark.read.format(source_info["format"]).load(source_info["path"])

        info(
            f"Finished {log_data['task']}",
            data={
                "task": log_data["task"],
                "state": STATE_FINISHED,
                "rows": df.count(),
            },
        )
        logger.flush()
        return df
    except AnalysisException as e:
        error(
            log_data["error_msg"],
            data={
                "task": log_data["task"],
                "dump": str(e),
                "state": STATE_ERROR,
            },
        )
        if str(e).startswith("Path does not exist:"):
            raise SourceEmptyException()
        else:
            raise e
    except Exception as e:
        error(
            log_data["error_msg"],
            data={
                "task": log_data["task"],
                "dump": str(e),
                "state": STATE_ERROR,
            },
        )
        raise e

# COMMAND ----------

def write_delta_data(df: DataFrame, destination_path: str, log_data: dict) -> None:
    if log_data["df_count"] != 0:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(destination_path)
        )
    else:
        error(
            "Could not write to destination as dataframe having zero records",
            data={
                "task": log_data["task"],
                "dest": destination_path,
                "state": STATE_ERROR,
            },
        )
        raise Exception(
            "Could not write to destination as dataframe having zero records"
        )

# COMMAND ----------

def log_and_write_delta_table(
    df: DataFrame, destination_path: str, log_data: dict
) -> None:
    try:
        info(
            f"Writing delta table at {destination_path}",
            data={
                "task": log_data["task"],
                "destination": destination_path,
                "state": STATE_STARTED,
            },
        )
        upload_count = df.count()
        log_data["df_count"] = upload_count
        write_delta_data(df, destination_path, log_data)
        deltaTable = DeltaTable.forName(spark, destination_path)
        info(
            f"Done writing delta table at {destination_path}",
            data={
                "task": log_data["task"],
                "state": STATE_FINISHED,
                "metrics": get_delta_metrics(deltaTable),
            },
        )
        deltaTable.optimize().executeCompaction()
    except Exception as e:
        error(
            f"Could not write to {destination_path}",
            data={
                "task": log_data["task"],
                "dest": destination_path,
                "dump": str(e),
                "state": STATE_ERROR,
            },
        )
        raise e

# COMMAND ----------

info(f"Clean room commons initialize for {env} env")
logger.flush()
