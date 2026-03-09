import atexit
from databricks import sql

class SplunkLogger:
    """
    Databricks Delta Metrics Logger (migrated from Splunk HTTP logger).
    Maintains compatibility with SplunkLogger interface.
    """

    def __init__(self, delta_table_path, workspace_url, access_token):
        self.delta_table_path = delta_table_path
        self.workspace_url = workspace_url
        self.access_token = access_token
        self.buffer = []
        self._init_databricks_connection()
        print(f"[DatabricksLogger] Initialized for Delta table: {self.delta_table_path}")

    def _init_databricks_connection(self):
        self.connection = sql.connect(
            server_hostname=self.workspace_url,
            http_path="sql/protocolv1/o/0/0",  # Adjust as needed
            access_token=self.access_token
        )
        self.cursor = self.connection.cursor()

    def log(self, event):
        self.buffer.append(event)
        print(f"[DatabricksLogger] Event buffered: {event}")

    def flush(self):
        if not self.buffer:
            return
        print(f"[DatabricksLogger] Flushing {len(self.buffer)} events to Delta table...")
        for event in self.buffer:
            self._write_to_delta(event)
        self.buffer.clear()

    def _write_to_delta(self, event):
        # Example: Insert event as a row into the Delta table
        columns = ', '.join(event.keys())
        placeholders = ', '.join(['%s'] * len(event))
        values = tuple(event.values())
        query = f"INSERT INTO {self.delta_table_path} ({columns}) VALUES ({placeholders})"
        try:
            self.cursor.execute(query, values)
            print(f"[DatabricksLogger] Event written: {event}")
        except Exception as e:
            print(f"[DatabricksLogger] Failed to write event: {event}, error: {e}")

    def close(self):
        self.flush()
        self.cursor.close()
        self.connection.close()
        print("[DatabricksLogger] Connection closed.")

# Ensure flush on exit
def _flush_logger_on_exit(logger):
    def flush_and_close():
        logger.flush()
        logger.close()
    return flush_and_close

# Example usage:
# Commented Splunk secret block (no longer needed)
# splunk_secret = get_secret("splunk")
# splunk_url = splunk_secret["url"]
# splunk_token = splunk_secret["token"]

# Databricks logger initialization
delta_table_path = "analytics.logs"
workspace_url = "adb-1234567890123456.7.azuredatabricks.net"
access_token = "dapiXXXXXXXXXXXXXXXXXXXXXXXX"

logger = SplunkLogger(delta_table_path, workspace_url, access_token)
atexit.register(_flush_logger_on_exit(logger))

def get_delta_data(query):
    """
    Fetch data from Databricks Delta table using the provided SQL query.
    """
    try:
        logger.log({"event": "get_delta_data_start", "query": query})
        logger.flush()
        with sql.connect(
            server_hostname=workspace_url,
            http_path="sql/protocolv1/o/0/0",
            access_token=access_token
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                logger.log({"event": "get_delta_data_success", "row_count": len(result)})
                logger.flush()
                return result
    except Exception as e:
        logger.log({"event": "get_delta_data_error", "error": str(e)})
        logger.flush()
        raise

def log_and_load_data(data):
    """
    Log data to Databricks Delta and load it for downstream processing.
    """
    try:
        logger.log({"event": "log_and_load_data_start", "data": data})
        logger.flush()
        # Write data to Delta table
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        values = tuple(data.values())
        query = f"INSERT INTO {delta_table_path} ({columns}) VALUES ({placeholders})"
        logger.cursor.execute(query, values)
        logger.log({"event": "log_and_load_data_success"})
        logger.flush()
    except Exception as e:
        logger.log({"event": "log_and_load_data_error", "error": str(e)})
        logger.flush()
        raise

# Example business logic (unchanged)
def process_event(event):
    try:
        logger.log({"event": "process_event_start", "event_data": event})
        logger.flush()
        # ... business logic ...
        log_and_load_data(event)
        logger.log({"event": "process_event_complete"})
        logger.flush()
    except Exception as e:
        logger.log({"event": "process_event_failed", "error": str(e)})
        logger.flush()
        raise
