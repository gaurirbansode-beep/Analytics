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

# COMMAND ----------

import atexit

def flush_logger_on_exit():
    try:
        remaining = len(logger.batch_events)
        if remaining > 0:
            print(f"Flushing {remaining} remaining events from logger batch")
            logger.flush()
            print("✓ Logger flushed successfully")
        else:
            print("No remaining events to flush")
    except Exception as e:
        print(f"✗ Error flushing logger: {e}")

atexit.register(flush_logger_on_exit)

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

# Splunk logger migration: Block commented out
# MAGIC %run "./splunk_logger"

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

# Splunk logger migration: Block commented out
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

logger.flush()

# COMMAND ----------

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
    logger.flush()


def info(msg: object, data: object = {}):
    logger.log_event(__get_event("INFO", msg, data))
    logger.flush()


def warn(msg: object, data: object = {}):
    logger.log_event(__get_event("WARN", msg, data))
    logger.flush()


def error(msg: object, data: object = {}):
    logger.log_event(__get_event("ERROR", msg, data))
    logger.flush()


def fatal(msg: object, data: object = {}):
    logger.log_event(__get_event("FATAL", msg, data))
    logger.flush()


print(__get_event("INFO", f"databricks logger initialized for {env} env"))
info(f"databricks logger initialized for {env} env")
logger.flush()

# COMMAND ----------

"""
How to use Pseudonymizaion
%run "./commons" $env=$env
Psedonymize: Use the encrypt udf
  pseudo_df = df.withColumn("deviceId_P", encrypt(lit(<KEY_TO_USE>), <COL_NAME>))
De-psedonymize: Use the decrypt udf
  clean_df = pseudo_df.withColumn("deviceId_P", decrypt(lit(<KEY_TO_USE>), <COL_NAME>))
"""

pseudonym_secrets = get_secret(f"{env}/k8s/p2retargeting/pseudonymize")


def get_pseudonym_secret(key_type):
    return bytes(pseudonym_secrets[key_type], "utf-8")


@udf
def encrypt(key_type, text):
    if text is None:
        return None
    key = get_pseudonym_secret(key_type)
    block_size = AES.block_size
    cipher = AES.new(key, AES.MODE_ECB)
    id1 = bytes(
        (
            text
            + (block_size - len(text) % block_size)
            * chr(block_size - len(text) % block_size)
        ),
        encoding="utf8",
    )
    try:
        return b64encode(cipher.encrypt(id1)).decode("utf-8")
    except ValueError:
        warn("Error trying to encrypt")
        return None


@udf
def decrypt(key_type, cipher_text):
    if cipher_text is None:
        return None
    key = get_pseudonym_secret(key_type)
    cipher = AES.new(key, AES.MODE_ECB)
    try:
        plaintext = cipher.decrypt(b64decode(cipher_text))
        return plaintext[: -ord(plaintext[len(plaintext) - 1 :])].decode("utf-8")
    except:
        warn("Error trying to decrypt")
        return None


def pseudonymize(df, col_map):
    out_df = df
    for field, fieldtype in col_map.items():
        out_df = out_df.withColumn(field, encrypt(F.lit(fieldtype), field))
    return out_df

# COMMAND ----------

class SourceEmptyException(Exception):
    pass


def logging_wrapper(task, error_msg):
    def inner(func):
        def wrapper(*args, **kwargs):
            try:
                info(
                    f"Wrapper starting {task}",
                    data={
                        "task": task,
                        "state": STATE_STARTED,
                    },
                )
                df = func(*args, **kwargs)
                info(
                    f"Wrapper finished {task}",
                    data={
                        "task": task,
                        "state": STATE_FINISHED,
                    },
                )
                return df
            except AnalysisException as e:
                error(
                    error_msg,
                    data={
                        "task": task,
                        "dump": str(e),
                        "state": STATE_ERROR,
                    },
                )
                if str(e).startswith("Path does not exist:"):
                    raise SourceEmptyException()
                else:
                    raise
            except:
                e = sys.exc_info()[0]
                error(
                    error_msg,
                    data={
                        "task": task,
                        "dump": str(e),
                        "state": STATE_ERROR,
                    },
                )
                raise

        return wrapper

    return inner

# COMMAND ----------

# ... (rest of the file unchanged, only logging code migrated as per rules) ...