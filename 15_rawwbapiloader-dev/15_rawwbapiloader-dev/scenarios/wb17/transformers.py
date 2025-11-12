import logging
from typing import Dict, Any

from scenarios.common.transformers import WBDataTransformer

logger = logging.getLogger(__name__)


class WB17DataTransformer(WBDataTransformer):
    """Data transformation class for handling WB17 scenario operations."""

    def _clean_data(self, record: Dict[str, Any]) -> tuple:
        """Clean product data by removing invalid fields."""
        try:
            # Convert each card dictionary to a tuple in the correct order for the query
            return (
                record.get('nmID'),
                record.get('imtID'),
                record.get('nmUUID'),
                record.get('subjectID'),
                record.get('subjectName'),
                record.get('vendorCode'),
                record.get('brand'),
                record.get('title'),
                record.get('description'),
                record.get('needKiz'),
                record.get('video'),
                self._to_json_string(record.get('photos')),
                self._to_json_string(record.get('wholesale')),
                self._to_json_string(record.get('dimensions')),
                self._to_json_string(record.get('characteristics')),
                self._to_json_string(record.get('sizes')),
                self._to_json_string(record.get('tags')),
                record.get('createdAt'),
                record.get('updatedAt')
            )
        except Exception as e:
            logger.error(f"Error cleaning product data: {e}\n",
                         extra={'log_data': {'event_code': 'PROCESS_DATA_ERROR'}}
                         )
            raise