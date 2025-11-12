import logging
from typing import Dict, Any

from scenarios.common.transformers import WBDataTransformer

logger = logging.getLogger(__name__)


class WB24DataTransformer(WBDataTransformer):
    """Data transformation class for handling WB24 scenario operations."""


    def _clean_data(self, record: Dict[str, Any]) -> tuple:
        """Clean product data by removing invalid fields."""
        try:

            # Convert each card dictionary to a tuple in the correct order for the query
            return (
                record.get('nmID'),
                record.get('vendorCode'),
                self._to_json_string(record.get('sizes')),
                record.get ('currencyIsoCode4217'),
                record.get('discount'),
                record.get('clubDiscount'),
                record.get('editableSizePrice'),
                record.get('isBadTurnover')
            )
        except Exception as e:
            logger.error(f"Error cleaning product data: {e}\n",
                         extra={'log_data': {'event_code': 'PROCESS_DATA_ERROR'}}
                         )
            raise