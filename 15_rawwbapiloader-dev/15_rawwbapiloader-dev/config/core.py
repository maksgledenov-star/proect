from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Any


from config.settings import ENVIRONMENT, DEBUG, ENV_CONFIG, SCENARIOS, BOT_NOTIFICATIONS, DB_DRIVER, DB_USER, \
    DB_PASSWORD

class BaseDBConfig(ABC):
    def __init__(self, **params):
        self.driver = params.get('driver')
        self.server = params.get('server')
        self.database = params.get('database')
        self.username = params.get('username')
        self.password = params.get('password')


    @abstractmethod
    def get_connection_string(self):
        pass


class WindowsAuthDBConfig(BaseDBConfig):
    def __init__(self, **params):
        super().__init__(**params)

    def get_connection_string(self):
        return (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"Trusted_Connection=yes;"
        )


class SQLAuthDBConfig(BaseDBConfig):
    def __init__(self, **params):
        super().__init__(**params)

    def get_connection_string(self):
        return (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
        )


class BaseAppConfig(ABC):
    def __init__(self, env: str, debug: bool):
        self.env = env
        self.debug = debug

    @abstractmethod
    def get_config(self):
        pass


class WBAppConfig(BaseAppConfig):
    def __init__(
            self,
            env: str,
            debug: bool
    ) -> None:
        super().__init__(
            env=env,
            debug=debug
        )

    def _get_db_params(self):
        return {
            'driver': DB_DRIVER,
            'server': ENV_CONFIG.get(self.env).get('DB_SERVER'),
            'database': ENV_CONFIG.get(self.env).get('DB_NAME'),
            'username': DB_USER,
            'password': DB_PASSWORD,
        }

    def _get_api_key(self):
        return ENV_CONFIG.get(self.env).get('API_KEY', '')

    def get_config(self) -> Dict[str, Any]:

        return {
            'db_params': self._get_db_params(),
            'api_key': self._get_api_key(),
            'debug': self.debug,
            'environment': self.env
        }


"""Command-line interface configuration and argument parsing."""
import argparse


class CliConfig:
    """Handle command-line interface configuration and argument parsing."""

    def __init__(self):
        self.parser = self._create_parser()

    def _create_parser(self) -> argparse.ArgumentParser:
        """Create and configure the argument parser."""
        parser = argparse.ArgumentParser(
            description='Wildberries API Data Loader',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            allow_abbrev=False
        )

        # Environment group
        env_group = parser.add_argument_group('environment arguments')
        env_group.add_argument(
            '--env',
            type=str,
            default=ENVIRONMENT,
            choices=ENV_CONFIG.keys(),
            help=f'Environment to run in. Available: {list(ENV_CONFIG.keys())}.'
        )
        env_group.add_argument(
            '--debug',
            action='store_true',
            default=DEBUG,
            help='Enable debug mode.'
        )
        env_group.add_argument(
            '--bot',
            action='store_true',
            default=BOT_NOTIFICATIONS,
            help='Enable bot notifications.'
        )

        # Scenario group
        scenario_group = parser.add_argument_group('scenario arguments')
        scenario_group.add_argument(
            '--scenario',
            type=str,
            required=True,
            choices=SCENARIOS.keys(),
            help=f'Scenario to run. Available: {list(SCENARIOS.keys())}.'
        )
        scenario_group.add_argument(
            '--scenario-args',
            nargs='+',
            metavar='KEY=VALUE',
            help='''Scenario arguments as key=value pairs.
            Example: --scenario-args date_from=2023-01-01 date_to=2023-12-31'''
        )

        return parser

    def parse_arguments(self) -> argparse.Namespace:
        """Parse and validate command line arguments."""
        args = self.parser.parse_args()
        self._process_scenario_args(args)
        self._validate_scenario_args(args)
        return args

    def _process_scenario_args(self, args: argparse.Namespace) -> None:
        """Process scenario arguments into a dictionary."""
        if not args.scenario_args:
            args.scenario_args = {}
            return

        scenario_args = {}
        for arg in args.scenario_args:
            if '=' not in arg:
                self.parser.error(f'Invalid argument format: {arg}. Expected key=value')
            key, value = arg.split('=', 1)
            if not key or not value:
                self.parser.error(f'Empty key or value in: {arg}')
            scenario_args[key] = value
        args.scenario_args = scenario_args

    def _validate_scenario_args(self, args: argparse.Namespace) -> None:
        """Validate that all required scenario arguments are provided."""
        scenario = SCENARIOS.get(args.scenario, {})
        required_args = scenario.get('required_args', {})

        missing_args = [
            (arg, desc) for arg, desc in required_args.items()
            if arg not in args.scenario_args
        ]

        if missing_args:
            missing_args_str = ', '.join(arg for arg, _ in missing_args)
            descriptions = [f"{arg}: {desc}" for arg, desc in missing_args]

            self.parser.error(
                f"Missing required scenario arguments: {missing_args_str}\n"
                f"Required arguments and their descriptions:\n" +
                "\n".join([f"- {desc}" for desc in descriptions])
            )


class Environment(str, Enum):
    DEVELOPMENT = "dev"
    PRODUCTION = "prod"
    TEST = "test"


class LoggingEventCodeEnum:
    app_start = 'APP_START'
    load_config_success = 'LOAD_CONFIG_SUCCESS'
    load_config_error = 'LOAD_CONFIG_ERROR'
    fetch_data_success = 'EXTRACT_DATA_SUCCESS'
    fetch_data_error = 'EXTRACT_DATA_ERROR'
    load_data_success = 'LOAD_DATA_SUCCESS'
    load_data_error = 'LOAD_DATA_ERROR'
    init_success = 'INIT_SUCCESS'
    init_error = 'INIT_ERROR'
    process_data_success = 'TRANSFORM_DATA_SUCCESS'
    process_data_error = 'TRANSFORM_DATA_ERROR'
    validation_error = 'VALIDATION_ERROR'
    validation_success = 'VALIDATION_SUCCESS'
    insert_data = 'INSERT_DATA_SUCCESS'
    insert_data_error = 'INSERT_DATA_ERROR'
    exception = 'EXCEPTION'
    api_error = 'API_ERROR'
    api_warning = 'API_WARNING'
    db_error = 'DB_ERROR'
    unknown_error = 'UNEXPECTED_ERROR'
    app_end = 'APP_FINISHED'







