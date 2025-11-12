
import logging
from typing import List, Tuple

import pyodbc

from core.repositories import BaseDBRepository
from core.utils.decorators import with_query_timeout

logger = logging.getLogger(__name__)


class WB24Repository(BaseDBRepository):
    """Repository for WB17 data operations."""

    def __init__(self, connection: pyodbc.Connection):
        """Initialize with a database connection manager.
        
        Args:
            connection: A DatabaseConnectionManager instance
        """
        super().__init__(connection)

    @with_query_timeout(60)
    def insert(self, data: List[Tuple], lpid: int, is_test_data: bool, collection_start_dttm: str) -> None:
        """Insert WB17 product data into the database.

        Args:
            data: List of product data tuples
            lpid: Load process ID for tracking this batch
            is_test_data: Whether this is test data
            collection_start_dttm: Collection start datetime in ISO format
        """
        try:
            with self.connection as conn:
                # Start a transaction
                conn.autocommit = False
                # Get a single timestamp for all records in this transaction
                with conn.cursor() as cursor:
                    cursor.execute("SELECT CAST(SYSUTCDATETIME() AS datetime2(3))")
                    collection_end = cursor.fetchone()[0]

                # Prepare parameters with the same collection_end for all records
                params = [(lpid, is_test_data, collection_start_dttm, collection_end, *item)
                          for item in data]

                # Insert all records
                with conn.cursor() as cursor:
                    cursor.executemany("""
                             INSERT INTO [RawWBData].[wb24_ProductsPricesReport]
                             (Lpid, IsTestData, CollectionStartDttm, CollectionEndDttm,
                              NmID, VendorCode, Sizes, CurrencyISOCode4217, Discount, ClubDiscount,
                              EditableSizePrice, IsBadTurnover)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                     """, params)

                # Commit the transaction
                conn.commit()

                logger.info(f"Successfully inserted {len(params)} records",
                            extra={'log_data': {'event_code': 'INSERT_DATA_SUCCESS',
                                                'destination_table': 'wb24_ProductsSizesReport'}}
                            )

        except pyodbc.DatabaseError as err:
            conn.rollback()
            logger.error(f"Database error inserting products: {err.args[1]}",
                         extra={'log_data': {'event_code': 'DB_ERROR'}}
                         )

            raise

        except Exception as e:
            # This will catch any exceptions that occur outside the transaction
            logger.error(f"Unexpected error in insert operation: {e}",
                         extra={'log_data': {'event_code': 'UNEXPECTED_ERROR'}})
            raise

        finally:
            # Reset autocommit to True
            conn.autocommit = True



