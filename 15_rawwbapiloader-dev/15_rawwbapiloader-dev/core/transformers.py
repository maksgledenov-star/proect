import logging
from abc import abstractmethod, ABC
from typing import Any


logger = logging.getLogger(__name__)


class BaseDataTransformer(ABC):

    @abstractmethod
    def process_data(self, data: Any) -> Any:
        pass