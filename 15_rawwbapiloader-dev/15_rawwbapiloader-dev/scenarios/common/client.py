import logging
from abc import abstractmethod
from typing import Dict, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from core.clients import BaseAPIClient, BaseSession

logger = logging.getLogger(__name__)


class WBSession(BaseSession):
    def __init__(self, max_retries: int, backoff_factor: float, status_forcelist: list, allowed_methods: list, api_key: str):
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.status_forcelist = status_forcelist
        self.allowed_methods = allowed_methods
        self._api_key = api_key
        self.session = requests.Session()


    def get_session(self):
        self._set_session()
        return self.session

    def _setup_retry(self):
        return Retry(
            total=self.max_retries,
            read=self.max_retries,
            connect=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=self.status_forcelist,
            allowed_methods=self.allowed_methods
        )
    @property
    def _retry(self):
        return self._setup_retry()

    def _setup_adapter(self) -> None:
        adapter = HTTPAdapter(max_retries=self._retry)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)


    def _setup_headers(self):
        self.session.headers.update({
            'Authorization': self._api_key,
            'Content-Type': 'application/json',
        })

    def _set_session(self):
        self._setup_headers()
        self._setup_adapter()



class WBAPIClient(BaseAPIClient):
    """Client for interacting with Wildberries API v2 for product management.

    This client is initialized with pre-loaded API settings and is responsible
    solely for making HTTP requests to the Wildberries API.
    """

    def __init__(self, api_settings: Dict[str, Any]):
        """
        Initialize the WB API client with required API settings.

        Args:
            api_settings: Pre-loaded API settings containing connection details
        """

        # Initialize base client with settings
        super().__init__(api_settings)

        self.max_retries = api_settings['max_retries']
        self.backoff_factor = api_settings['backoff_factor']
        self.status_forcelist = api_settings['status_forcelist']
        self.allowed_methods = api_settings['allowed_methods']
        self._api_key = api_settings['api_key']

        # Initialize session after all variables are set
        self.session = self._set_session()


    def _set_session(self) -> requests.Session:
        """Create a session object for making requests (based on requests.Session)."""
        try:

            return WBSession(
                max_retries=self.max_retries,
                backoff_factor=self.backoff_factor,
                status_forcelist=self.status_forcelist,
                allowed_methods=self.allowed_methods,
                api_key=self._api_key
            ).get_session()
        except Exception as e:
            logger.error(
                f"Failed to create session: {str(e)}",
                extra={'log_data': {'event_code': 'LOAD_CONFIG_ERROR'}}
            )
            raise

    @abstractmethod
    def fetch_data(self):
        pass



