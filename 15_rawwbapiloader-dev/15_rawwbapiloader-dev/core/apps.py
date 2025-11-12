import importlib
import importlib.util
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from types import ModuleType
from typing import Dict, Any, Type

from core.db.connection import DatabaseConnectionManager
from core.services import BaseService
from core.utils.exceptions import LoadScenarioException
from config.settings import BASE_DIR
from scenarios.common.services import WBService

logger = logging.getLogger(__name__)


class BaseApp(ABC):
    def __init__(self, config: Dict[str, Any], scenario: str, scenario_args: Dict[str, Any]):
        self.config = config
        self.scenario = scenario
        self.scenario_args = scenario_args

    @abstractmethod
    def get_service_instance(self):
        pass


class WBApp(BaseApp):
    _base_dir = BASE_DIR
    _target_dir = 'scenarios'
    _filename = 'service.py'
    _class_suffix = 'Service'

    def __init__(
            self, 
            config: Dict[str, Any],
            scenario: str,
            scenario_args: Dict[str, Any],
            db_manager: DatabaseConnectionManager = None,
            lpid: int = None,
            collection_start_dttm: str = None
    ):
        super().__init__(config, scenario, scenario_args)
        
        # Use provided db_manager or create a new one if not provided
        if db_manager is None:
            self.db_manager = DatabaseConnectionManager(
                db_params=config.get('db_params')
            )
            self.lpid = self.db_manager.generate_lpid()
            logger.warning("Database manager was not provided, created a new one", extra={
                'log_data': {'event_code': 'LOAD_CONFIG_SUCCESS'}
            })
        else:
            self.db_manager = db_manager
            self.lpid = lpid or db_manager.generate_lpid()
        
        self.collection_start_dttm = collection_start_dttm
        
        logger.info("Initialized WBApp with LPID: %d", self.lpid, extra={
            'log_data': {
                'event_code': 'LOAD_CONFIG_SUCCESS',
                'data_load_scenario': self.scenario,
                'lpid': self.lpid
            }
        })

    def _get_service_file_path(self) -> Path:
        """Get the path to the scenario-specific service file."""
        # Build the full path to the service module
        module_path = (
                self._base_dir /
                self._target_dir /
                self.scenario /
                self._filename
        )

        if not module_path.exists():
            raise FileNotFoundError(f"Scenario service file not found: {module_path}")

        logger.info(f"Scenario service file found: {module_path}", extra={
            'log_data': {'event_code': 'LOAD_CONFIG_SUCCESS'}
        })
        return module_path

    def _get_module_name(self) -> str:
        # Create a unique module name
        module_name = f"{self._target_dir}.{self.scenario}.{self._filename}".replace(".py", "")
        logger.info(f"Module name: {module_name}", extra={
            'log_data': {'event_code': 'LOAD_CONFIG_SUCCESS'}
        })
        return module_name

    def _load_spec(self) -> ModuleType:
        # Load the module from file
        module_path = self._get_service_file_path()
        module_name = self._get_module_name()
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        if spec is None:
            raise ImportError(f"Could not load spec from {module_path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module


    def _parse_service_class(self, module: ModuleType) -> Type[WBService]:
        """Parse the service class from the module."""
        base = self.scenario.upper()
        class_name = "".join([base, self._class_suffix])

        try:
            service_class = getattr(module, class_name)


            if not issubclass(service_class, WBService):
                raise TypeError("Scenario service must inherit from WBService class")

            return service_class

        except AttributeError as e:
            available_classes = [name for name, obj in module.__dict__.items()
                              if isinstance(obj, type) and issubclass(obj, BaseService)]
            raise ImportError(
                f"Service class '{class_name}' not found in module. "
                f"Available service classes: {', '.join(available_classes) or 'None'}"
            ) from e

    def _load_scenario(self)-> WBService:
        """Dynamically load and initialize the scenario-specific service."""

        try:
           service_class = self._parse_service_class(
                module=self._load_spec()
            )

           return service_class(
               config=self.config,
               db_manager=self.db_manager,
               scenario=self.scenario,
               scenario_args=self.scenario_args,
               lpid=self.lpid,
               collection_start_dttm=self.collection_start_dttm
           )

        except (ImportError, AttributeError, TypeError) as e:
            raise LoadScenarioException(
                f"Failed to load service for scenario '{self.scenario}': {str(e)}"
            ) from e

    def get_service_instance(self):
        """Get the scenario-specific service instance.
        Args:


        Returns:
            BaseService: The scenario-specific service instance

        Raises:
            LoadScenarioException: If the scenario service cannot be loaded
        """
        try:
            return self._load_scenario()
        except LoadScenarioException as e:
            logger.error(str(e), extra={
                'log_data': {'event_code': 'LOAD_CONFIG_ERROR'}
            })
            raise