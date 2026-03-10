# Databricks Logger Migration - 100% Complete
# %run './databricks_logger'

import atexit
from databricks_logger import DatabricksLogger

# Initialize Databricks Logger
logger = DatabricksLogger(table_name="analytics_metrics", schema="default")
atexit.register(logger.flush)

def log_event(event_name, event_data):
    try:
        logger.log_metric(event_name, event_data)
        logger.flush()
    except Exception as e:
        logger.log_error("Error logging event", {"exception": str(e)})
        logger.flush()
        raise

def process_data(data):
    try:
        # Business logic remains unchanged
        result = data * 2
        log_event("data_processed", {"input": data, "result": result})
        return result
    except Exception as e:
        logger.log_error("Error in process_data", {"exception": str(e)})
        logger.flush()
        raise

def main():
    try:
        data = 10
        result = process_data(data)
        log_event("main_completed", {"result": result})
    except Exception as e:
        logger.log_error("Error in main", {"exception": str(e)})
        logger.flush()
        raise

if __name__ == "__main__":
    main()
    logger.flush()
