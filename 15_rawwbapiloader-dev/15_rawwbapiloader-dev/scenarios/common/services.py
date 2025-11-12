import logging
from abc import abstractmethod
from typing import Dict, Any, List

from core.db import DatabaseConnectionManager
from core.services import BaseService


logger = logging.getLogger(__name__)


class WBService(BaseService):
    """Service class for handling WB scenario operations."""

    def __init__(self,
                 config: Dict[str, Any],
                 db_manager: DatabaseConnectionManager,
                 scenario: str,
                 scenario_args: Dict[str, Any] | None = None,
                 lpid: int = None,
                 collection_start_dttm: str = None
                 ):
        """Initialize the WB service.

        Args:
            config: Application configuration
            db_manager: Database manager instance
            scenario: Scenario name
            scenario_args: Scenario-specific arguments
            lpid: Load process ID for tracking this run
            collection_start_dttm: Collection start datetime in ISO format
        """
        super().__init__(config, db_manager, scenario, scenario_args, lpid, collection_start_dttm)

        # Get the scenario configuration
        self._scenario = scenario
        self._scenario_args = scenario_args
        self._environment = self.config.get('environment')
        self._is_debug = self.config.get('debug')
        self._api_key = self.config.get('api_key')

        # Get the database manager
        self.db_manager = db_manager

        # Set the repository, settings repository, DTO and API client classes
        self._data_repository_class = self._set_data_repository_class()
        self._api_settings_repo_class = self._set_api_settings_repo_class()

        self._data_dto_class = self._set_data_dto_class()
        self._api_settings_dto_class = self._set_api_settings_dto_class()

        self._api_client_class = self._set_api_client_class()
        self._data_model_class = self._set_data_model_class()
        self._api_settings_model_class = self._set_api_settings_model_class()

        # Initialize the repository, settings repository, DTO and API client
        self._init_repos()
        self._init_dtos()
        self._init_api_client()

    @staticmethod
    @abstractmethod
    def _set_data_repository_class():
        pass

    @staticmethod
    @abstractmethod
    def _set_api_settings_repo_class():
        pass

    @staticmethod
    @abstractmethod
    def _set_data_dto_class():
        pass

    @staticmethod
    @abstractmethod
    def _set_api_settings_dto_class():
        pass

    @staticmethod
    @abstractmethod
    def _set_api_client_class():
        pass

    @staticmethod
    @abstractmethod
    def _set_data_model_class():
        pass

    @staticmethod
    @abstractmethod
    def _set_api_settings_model_class():
        pass

    def _init_repos(self):
        db_conn = self.db_manager.get_connection()
        self._data_repository = self._data_repository_class(connection=db_conn)
        self._api_settings_repo = self._api_settings_repo_class(connection=db_conn)
        logger.info(f"Initialized repositories: {type(self._data_repository).__name__, type(self._api_settings_repo).__name__}",
                    extra={'log_data': {'event_code': 'INIT_SUCCESS'}}
                    )

    def _init_dtos(self):
        self._data_dto = self._data_dto_class(data_model=self._data_model_class)
        self._api_settings_dto = self._api_settings_dto_class(data_model=self._api_settings_model_class)
        logger.info(f"Initialized DTOs: {type(self._data_dto).__name__, type(self._api_settings_dto).__name__}",
                    extra={'log_data': {'event_code': 'INIT_SUCCESS'}}
                    )

    def _init_api_settings(self) -> Dict[str, Any]:
        """
        Get API settings from database or use defaults.

        Returns:
            WBApiSettings instance (from DB or with default values)
        """
        # Try to get settings from database (returns Row or None)
        db_row = self._api_settings_repo.retrieve(
            scenario=self._scenario,
            is_debug=self._is_debug
        )
        api_settings = self._api_settings_dto.process_data(db_row)

        logger.info(
            "API client settings: " + ", ".join(f"{k}:{v}" for k, v in api_settings.items()),
            extra={'log_data':
                {
                    'event_code': 'LOAD_CONFIG_SUCCESS',
                    'load_params_id': api_settings.get('load_params_id'),
                }
            }
        )
        return api_settings

    def _init_api_client(self):
        api_settings = self._init_api_settings()
        api_settings['api_key'] = self._api_key

        # Add/override scenario args to api settings
        api_settings.update(self._scenario_args or {})

        # Pass api_settings as a positional argument
        self._api_client = self._api_client_class(api_settings)
        logger.info(
            f"Initialized API client: {type(self._api_client).__name__}",
            extra={'log_data': {'event_code': 'INIT_SUCCESS'}}
        )



    def run(self) -> None | Exception:
        """
        - Fetch all product cards from the API,
        - process and clean raw data,
        - insert clean data into the database.
        """
        logger.info(f"Running {self._scenario} service: {self.__class__.__name__}",
                    extra={'log_data': {'event_code': 'LOAD_CONFIG_SUCCESS'}}
                    )
        try:
            if not self.collection_start_dttm:
                from datetime import datetime, timezone
                self.collection_start_dttm = datetime.now(timezone.utc).isoformat()

            raw_data = self._api_client.fetch_data()
            processed_data = self._data_dto.process_data(raw_data)

            # Insert clean data into the database
            self._data_repository.insert(
                data=processed_data,
                lpid=self.lpid,
                is_test_data=self._is_debug,
                collection_start_dttm=self.collection_start_dttm
            )

        except Exception as e:
            logger.error(f"Error during {self._scenario} service run: {type(e).__name__}.",
                         extra={'log_data': {'event_code': 'EXCEPTION'}}
                         )
            raise
