import logging
from typing import Dict, Any

from core.db.connection import DatabaseConnectionManager

from .client import WB17APIClient

from .models import ProductCardModel, WB17APISettingsModel
from .repositories import WB17Repository
from .transformers import WB17DataTransformer
from ..common.repositories import WBAPISettingsRepository
from ..common.services import WBService
from ..common.transformers import WBAPISettingsTransformer

logger = logging.getLogger(__name__)


class WB17Service(WBService):
    """Service class for handling WB17 scenario operations."""

    def __init__(self,
                 config: Dict[str, Any],
                 db_manager: DatabaseConnectionManager,
                 scenario: str,
                 scenario_args: Dict[str, Any] | None = None,
                 lpid: int = None,
                 collection_start_dttm: str = None
        ):
        super().__init__(config, db_manager, scenario, scenario_args, lpid, collection_start_dttm)



    @staticmethod
    def _set_data_repository_class():
        return WB17Repository

    @staticmethod
    def _set_api_settings_repo_class():
        return WBAPISettingsRepository

    @staticmethod
    def _set_data_dto_class():
        return WB17DataTransformer

    @staticmethod
    def _set_api_settings_dto_class():
        return WBAPISettingsTransformer

    @staticmethod
    def _set_api_client_class():
        return WB17APIClient

    @staticmethod
    def _set_data_model_class():
        return ProductCardModel

    @staticmethod
    def _set_api_settings_model_class():
        return WB17APISettingsModel






