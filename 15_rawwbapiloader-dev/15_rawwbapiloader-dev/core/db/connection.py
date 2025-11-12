import logging
from typing import Optional, Dict, Any

import pyodbc

from config.core import WindowsAuthDBConfig, BaseDBConfig
from config.settings import DB_QUERY_TIMEOUT, DB_LOCK_TIMEOUT_MS, DB_LOGIN_TIMEOUT

logger = logging.getLogger(__name__)


class DatabaseConnectionManager:
    """Manages database connections with lazy initialization and context manager support."""

    db_auth_cls: BaseDBConfig = WindowsAuthDBConfig

    def __init__(
            self, db_params: Dict[str, Any],
            query_timeout: int = DB_QUERY_TIMEOUT,
            lock_timeout_ms: int = DB_LOCK_TIMEOUT_MS,
            login_timeout: int = DB_LOGIN_TIMEOUT,
    ):
        self.db_params = db_params
        self.connection_string = self._get_connection_string()
        self._connection: Optional[pyodbc.Connection] = None
        self.query_timeout = query_timeout
        self.lock_timeout_ms = lock_timeout_ms
        self.login_timeout = login_timeout


    def _get_connection_string(self):
        return self.db_auth_cls(**self.db_params).get_connection_string()



    def _init_connection(self) -> pyodbc.Connection:
        cn = pyodbc.connect(
            self.connection_string,
            timeout=self.login_timeout,  # login timeout
        )
        logger.debug("Created new database connection", extra={
            'log_data': {'event_code': 'LOAD_CONFIG_SUCCESS'}
        })

        # Per-connection query timeout (seconds); 0 = no timeout (default)
        cn.timeout = self.query_timeout

        # SQL Server lock timeout: how long to wait on locks before error 1222
        with cn.cursor() as cur:
            cur.execute(f"SET LOCK_TIMEOUT {self.lock_timeout_ms};")
        return cn

    def get_connection(self) -> pyodbc.Connection:
        """Get a database connection, creating it if necessary."""
        if self._connection is None:
            self._connection = self._init_connection()
        return self._connection
    
    def close(self) -> None:
        """Close the database connection if it's open."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            logger.debug("Closed database connection", extra={
                'log_data': {'event_code': 'LOAD_CONFIG_SUCCESS'}
            })
    
    def generate_lpid(self, timeout_seconds: int = 5) -> int:
        """Generate a unique LPID using database sequence with distributed locking.

        Args:
            timeout_seconds: Lock timeout in seconds (default 5)

        Returns:
            int: Generated unique LPID

        Raises:
            RuntimeError: If unable to acquire lock or generate LPID
        """
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # First, ensure the sequence exists
                cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM sys.sequences WHERE name = 'LpidSequence')
                BEGIN
                    CREATE SEQUENCE Lpid.LpidSequence 
                    START WITH 1 
                    INCREMENT BY 1;
                    
                    PRINT 'Created sequence Lpid.LpidSequence';
                END
                
                -- Now acquire lock
                DECLARE @LockResult INT;
                DECLARE @TimeoutMs INT = ? * 1000;
                
                EXEC @LockResult = sp_getapplock 
                    @Resource = 'LpidLock',
                    @LockMode = 'Exclusive',
                    @LockOwner = 'Session',
                    @LockTimeout = @TimeoutMs,
                    @DbPrincipal = 'public';

                -- Check lock acquisition result
                IF @LockResult < 0
                BEGIN
                    DECLARE @ErrorMsg NVARCHAR(100);
                    SET @ErrorMsg = CASE 
                        WHEN @LockResult = -1 THEN 'Lock timeout expired'
                        WHEN @LockResult = -2 THEN 'Canceled (deadlock)'
                        WHEN @LockResult = -3 THEN 'Canceled (error)'
                        ELSE 'Failed to acquire lock (code: ' + CAST(@LockResult AS NVARCHAR(10)) + ')'
                    END;
                    
                    THROW 50000, @ErrorMsg, 1;
                END;

                -- Get next LPID value
                DECLARE @NextLpid INT;
                SELECT @NextLpid = NEXT VALUE FOR Lpid.LpidSequence;
                
                -- Release the lock
                EXEC sp_releaseapplock @Resource = 'LpidLock', @LockOwner = 'Session';
                
                -- Return the generated LPID
                SELECT @NextLpid AS NextLPID;
                """, timeout_seconds)
                
                result = cursor.fetchone()[0]
                if result is None:
                    raise RuntimeError("Failed to generate LPID: No value returned from sequence")
                return result
                
        except pyodbc.DatabaseError as e:
            error_msg = f"Database error while generating LPID: {str(e)}"
            logger.error(error_msg, exc_info=True, extra={
                'log_data': {
                    'event_code': 'DB_ERROR',
                    'error': str(e)
                }
            })
            raise RuntimeError(error_msg) from e
            
        except Exception as e:
            error_msg = f"Unexpected error while generating LPID: {str(e)}"
            logger.error(error_msg, exc_info=True, extra={
                'log_data': {
                    'event_code': 'UNEXPECTED_ERROR',
                    'error': str(e)
                }
            })
            raise RuntimeError(error_msg) from e

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures connection is closed."""
        self.close()
        return False  # Don't suppress exceptions
