from abc import ABC, abstractmethod
from typing import Any, Optional

import pyodbc



class BaseRepository(ABC):
    """Base class for all repositories."""
    pass


class BaseDBRepository(BaseRepository):
    """Base class for database repositories.
    
    This class provides common database operations and manages connection lifecycle.
    """
    
    def __init__(self, connection: pyodbc.Connection):
        """Initialize with a database connection manager.
        
        Args:
            connection: A DatabaseConnectionManager instance
        """
        self._connection = connection
    
    @property
    def connection(self) -> pyodbc.Connection:
        """Get the database connection manager."""
        return self._connection
    

    def insert(self, **kwargs) -> Any | None:
        """Insert data into the database."""
        raise NotImplementedError


    def update(self, **kwargs) -> Any | None:
        """Update data in the database."""
        raise NotImplementedError

    def delete(self, **kwargs) -> Any | None:
        """Delete data from the database."""
        raise NotImplementedError

    def retrieve(self, **kwargs) -> Any | None:
        """Get data from the database."""
        raise NotImplementedError