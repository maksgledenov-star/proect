from dataclasses import dataclass, field
from typing import List, Optional, Dict


@dataclass
class DBSchema:
    """Represents database schema configuration with default values."""
    project_name: str = 'RawWBApiLoader'
    config_schema: str = 'cfg'
    config_table: str = 'WBApiLoadParams'
    log_schema: str = 'RawLogdata'
    log_table: str = 'LoaderLog'
    data_schema: str = 'RawWBData'
    tables: List[str] = field(default_factory=lambda: ['wb17_ProductsReport', 'wb24_ProductsPricesReport'])
    scenario: Optional[str] = None


class DatabaseSchemaService:
    """Service for managing and validating database schema configurations."""
    
    def __init__(self, db_manager, scenario: Optional[str] = None):
        """
        Initialize with a database manager and optional scenario.
        
        Args:
            db_manager: Database connection manager
            scenario: Optional scenario name (e.g., 'wb17', 'wb18')
        """
        self.db_manager = db_manager
        self.scenario = scenario
        self._schema = DBSchema(scenario=scenario)
    
    def get_schema_config(self) -> DBSchema:
        """
        Validate and return the schema configuration.
        
        Returns:
            DBSchema: Validated schema configuration
        """
        if not hasattr(self, 'db_manager') or self.db_manager is None:
            return self._schema
            
        try:
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Get all schemas and tables
                    cursor.execute("""
                        SELECT 
                            SCHEMA_NAME(t.schema_id) as schema_name,
                            t.name as table_name
                        FROM sys.tables t
                        WHERE SCHEMA_NAME(t.schema_id) IN (?, ?, ?)
                    """, (self._schema.config_schema, self._schema.log_schema, self._schema.data_schema))
                    
                    schemas = {}
                    for schema_name, table_name in cursor.fetchall():
                        if schema_name not in schemas:
                            schemas[schema_name] = []
                        schemas[schema_name].append(table_name)
                    
                    # Validate schemas and tables
                    self._validate_schema(schemas, self._schema.config_schema, [self._schema.config_table])
                    self._validate_schema(schemas, self._schema.log_schema, [self._schema.log_table])
                    
                    # If we have a scenario, validate its tables exist
                    if self.scenario:
                        self._validate_schema(schemas, self._schema.data_schema, 
                                           [table for table in self._schema.tables])
                    
                    return self._schema
                    
        except Exception as e:
            # Log error but don't fail - return default schema
            import logging
            logging.error(f"Error validating database schema: {str(e)}")
            return self._schema

    @staticmethod
    def _validate_schema(schemas: Dict[str, List[str]], schema_name: str, expected_tables: List[str]) -> None:
        """
        Validate that the schema and its tables exist.
        
        Args:
            schemas: Dictionary of schema names to their tables
            schema_name: Schema to validate
            expected_tables: List of tables that should exist in the schema
        """
        if schema_name not in schemas:
            raise ValueError(f"Schema '{schema_name}' does not exist")
            
        existing_tables = set(schemas[schema_name])
        missing_tables = [t for t in expected_tables if t not in existing_tables]
        
        if missing_tables:
            raise ValueError(f"Missing tables in schema '{schema_name}': {', '.join(missing_tables)}")
