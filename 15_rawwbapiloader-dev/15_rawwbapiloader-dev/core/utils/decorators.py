import functools
import pyodbc

def with_query_timeout(timeout_seconds: int):
    """Temporarily override conn.timeout for the duration of a DB call."""
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(self, *args, **kwargs):
            conn = getattr(self, "connection", None)
            if conn is None or not isinstance(conn, pyodbc.Connection):
                raise RuntimeError("Repository has no active pyodbc connection")

            old_timeout = conn.timeout
            try:
                conn.timeout = timeout_seconds
                return fn(self, *args, **kwargs)
            finally:
                conn.timeout = old_timeout
        return wrapper
    return decorator
