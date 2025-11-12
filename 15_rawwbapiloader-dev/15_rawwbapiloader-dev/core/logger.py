import logging
from typing import Optional, Dict, Any, cast
from logging import LogRecord
from datetime import datetime

from core.db.schema import DBSchema as DatabaseSchema
from config.core import LoggingEventCodeEnum


class DatabaseLogHandler(logging.Handler):
    """Custom logging handler that writes logs to a database."""
    
    def __init__(
            self,
            db_manager,
            schema_config: DatabaseSchema,
            lpid: int = None,
            scenario: Optional[str] = None
    ):
        """
        Initialize the database log handler.
        
        Args:
            db_manager: Database connection manager
            schema_config: Database schema configuration
            lpid: Load process ID
        """
        super().__init__()
        self.db_manager = db_manager
        self.schema_config = schema_config
        self.scenario = scenario
        self.lpid = lpid
        self.setFormatter(logging.Formatter('%(message)s'))
        

        self._log_table = schema_config.log_table
        self._log_schema = schema_config.log_schema

    def emit(self, record: LogRecord) -> None:
        """Emit a log record to the database."""
        try:
            # Ensure log_data exists on the record
            if not hasattr(record, 'log_data'):
                record.log_data = {}

            # Apply the same filter logic as URLLib3LogFilter
            log_data = record.log_data

            # Prepare log entry with all required fields
            log_entry = {
                'EventCode': log_data.get('event_code', "API_WARNING"),
                'EventSource': self.schema_config.project_name,
                'EventMessage': self.format(record)[:4000],  # Truncate to avoid overflow
                'EventStatus': record.levelname,
                'LoadParamsId': log_data.get('load_params_id'),
                'DestinationTable': log_data.get('destination_table'),
                'DataLoadScenario': self.scenario,
                'Lpid': self.lpid
            }

            
            # Prepare the SQL query with proper parameter binding
            columns = ', '.join([f'[{k}]' for k in log_entry.keys()])
            placeholders = ', '.join(['?'] * len(log_entry))
            
            query = f"""
            INSERT INTO [{self._log_schema}].[{self._log_table}]
            ({columns})
            VALUES ({placeholders})
            """
            
            # Execute the query with parameter values
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, list(log_entry.values()))
                    conn.commit()
                    
        except Exception as e:
            # Fallback to console if database logging fails
            import traceback
            print(f"Failed to write log to database: {e}\n{traceback.format_exc()}")


def setup_logging(
    level=None,
    db_manager=None,
    lpid: int = None,
    schema_config: Optional[DatabaseSchema] = None,
    scenario: Optional[str] = None
):
    # Get the root logger
    root_logger = logging.getLogger()

    # Remove all existing handlers and filters
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        handler.close()


    # Create a console handler with improved formatting
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    console_handler.setLevel(level)

    # Add console handler to root logger
    root_logger.addHandler(console_handler)
    # Configure root logger
    root_logger.setLevel(level or logging.INFO)
    root_logger.propagate = False

    # Configure urllib3 logger to ensure it propagates to root
    urllib3_logger = logging.getLogger('urllib3')
    urllib3_logger.setLevel(logging.DEBUG)
    urllib3_logger.propagate = True

    # Add database handler if db_manager is provided
    if db_manager is not None:
        try:
            # Create database handler with schema config
            db_handler = DatabaseLogHandler(
                db_manager=db_manager,
                schema_config=schema_config,
                lpid=lpid,
                scenario=scenario
            )
            
            # Set appropriate log level for database handler
            db_handler.setLevel(logging.INFO)
            root_logger.addHandler(db_handler)

            # Log successful database handler initialization
            root_logger.debug("Database logging initialized.")
            
        except Exception as e:
            root_logger.error(
                f"Failed to initialize database logging: {e}",
                exc_info=True
            )




