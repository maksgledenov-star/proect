import logging

import pyodbc

from core.repositories import BaseDBRepository

logger = logging.getLogger(__name__)


class WBAPISettingsRepository(BaseDBRepository):
    """Repository for retrieving API settings from cfg.WBApiLoadParams."""

    def __init__(self, connection: pyodbc.Connection):
        super().__init__(connection)

    def retrieve(
            self,
            scenario: str,
            is_debug: bool
    ) -> pyodbc.Row | None:
        """
        Retrieve API settings for the specified scenario, environment, and debug mode.

        Args:
            scenario: Data load scenario name
            is_debug: Whether debug mode is enabled

        Returns:
            WBApiSettings if found, None otherwise
        """
        query = """
        SELECT 
            *
        FROM cfg.WBApiLoadParams 
        WHERE DataLoadScenario = ? 
          AND IsTestData = ?
        """

        try:
            with self.connection as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (scenario, is_debug))

                    row = cursor.fetchone()

                    if not row:
                        logger.warning(
                            f"No API settings found for scenario={scenario},"
                            f"debug={is_debug}",
                            extra={'log_data': {'event_code': 'LOAD_CONFIG_ERROR'}}
                        )
                        return None

                    return row


        except Exception as e:
            logger.error(
                f"Error fetching API settings for scenario={scenario}, "
                f"debug={is_debug}: {e}",
                extra={'log_data': {'event_code': 'LOAD_CONFIG_ERROR'}}
            )
            return None