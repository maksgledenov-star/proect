import logging
from abc import abstractmethod, ABC
from typing import Dict, Any

from core.db.connection import DatabaseConnectionManager


logger = logging.getLogger(__name__)


class BaseService(ABC):
    def __init__(
            self,
            config: Dict[str, Any],
            db_manager: DatabaseConnectionManager,
            scenario: str,
            scenario_args: Dict[str, Any],
            lpid: int,
            collection_start_dttm: str = None
    ):
        self.config = config
        self.db_manager = db_manager
        self.scenario = scenario
        self.scenario_args = scenario_args
        self.lpid = lpid
        self.collection_start_dttm = collection_start_dttm

    @abstractmethod
    def run(self) -> None | Exception :
        pass









