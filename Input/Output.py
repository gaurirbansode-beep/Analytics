...
# (Second chunk of migrated code, continuing from previous write)
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
    # padding message to a length that is multiple of AES block size
    id1 = bytes(
        (
            text
            + (block_size - len(text) % block_size)
            * chr(block_size - len(text) % block_size)
        ),
        encoding="utf8",
    )
    # instantiate a new AES cipher object
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


# for every key/value in col_map, replace df[key] with encrypt(value, key)
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
                logger.flush()
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
                logger.flush()
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
                logger.flush()
                raise

        return wrapper

    return inner

# COMMAND ----------

# (Continue with remaining migrated code, including all logger.flush() after writes, exception blocks, and at exit)
import atexit

def flush_logger_on_exit():
    try:
        if len(logger.batch_events) > 0:
            logger.flush()
    except Exception:
        pass
atexit.register(flush_logger_on_exit)
