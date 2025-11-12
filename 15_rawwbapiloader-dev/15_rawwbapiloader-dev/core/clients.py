import logging
from abc import ABC, abstractmethod


logger = logging.getLogger(__name__)


class BaseAPIClient(ABC):
    def __init__(self, api_settings):
        self.api_settings = api_settings

    @abstractmethod
    def _set_session(self):
        pass

    @abstractmethod
    def fetch_data(self):
        pass


class BaseSession(ABC):

    @abstractmethod
    def _set_session(self):
        pass

    @abstractmethod
    def get_session(self):
        pass