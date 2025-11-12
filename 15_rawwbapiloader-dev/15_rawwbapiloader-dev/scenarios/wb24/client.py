import logging
from typing import Dict, Any, Optional, List, Union
from datetime import datetime, timezone, timedelta
import json
import time
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter, Retry

from core.utils.exceptions import APIRequestException
from scenarios.common.client import WBAPIClient
from .models import WB24ReportModel

logger = logging.getLogger(__name__)


class WB24APIClient(WBAPIClient):
    """Client for making requests to the WB24 API (Products and Prices report)."""
    
    def __init__(self, api_settings: Dict[str, Any]):
        """Initialize the WB24 API client with required API settings.

        Args:
            api_settings: Dictionary containing API configuration
        """
        super().__init__(api_settings)
        
        # Required settings
        self.load_params_id = api_settings['load_params_id']
        self.limit = api_settings.get('limit', 1000)  # Default to 1000 as per API docs
        self.offset = api_settings.get('offset', 0)
        self.api_url = api_settings['api_url']
        self.timeout = api_settings.get('timeout', 60)  # Default to 60 seconds
        self.max_retries = api_settings.get('max_retries', 3)
        self.backoff_factor = api_settings.get('backoff_factor', 0.5)
        self.status_forcelist = api_settings.get('status_forcelist', [429, 500, 502, 503, 504])
        self.allowed_methods = api_settings.get('allowed_methods', ['GET'])
        self._api_key = api_settings['api_key']
        
        # Set up session with retry logic
        self.session = self._set_session()
    
    def _build_query_params(self) -> Dict[str, Any]:
        """Build the query parameters for the API request.
        
        Args:
            offset: Offset for pagination
            
        Returns:
            Dictionary containing the query parameters
        """
        return {
            'limit': self.limit,
            'offset': self.offset
        }

    def _is_last_batch(self, batch: List[Dict[str, Any]]) -> bool:
        """Determine if this is the last batch of data."""

        # If we received fewer cards than the limit, it's the last batch
        return True if len(batch) < self.limit else False

    
    def fetch_data(self) -> List[Dict[str, Any]]:
        """
        Fetch all product data with automatic pagination.
        
        Returns:
            List of all product records
            
        Raises:
            Exception: If there's an error fetching or processing the data
        """
        all_products = []

        with self.session as s:
            while True:
                try:
                    # Build query parameters
                    params = self._build_query_params()

                    # Make the API request
                    response = s.get(
                        url=self.api_url,
                        params=params
                    )
                    response.raise_for_status()
                    response_data = response.json()

                    # Extract products from response
                    products = response_data.get('data', {}).get('listGoods', [])
                    if not products:
                        err_msg = "Empty response from the API call."
                        logger.error(f"{err_msg}",
                                     extra={'log_data': {
                                         'event_code': 'EXTRACT_DATA_ERROR',
                                         'load_params_id': self.load_params_id
                                     }}
                                     )
                        raise APIRequestException("Could not extract data.")

                    logger.info(f"Extracted {len(products)} products.",
                        extra={'log_data': {
                            'event_code': 'EXTRACT_DATA_SUCCESS',
                            'load_params_id': self.load_params_id
                        }}
                    )
                    # Add products to the result list
                    all_products.extend(products)

                    # Check if we've reached the end of the data
                    if self._is_last_batch(products):
                        logger.info(f"All data extracted: {len(all_products)} objects",
                                    extra={'log_data': {
                                        'event_code': 'EXTRACT_DATA_SUCCESS',
                                        'load_params_id': self.load_params_id
                                    }}
                                    )
                        return all_products

                    self.offset += len(products)
                    # Small delay to avoid hitting rate limits
                    time.sleep(0.5)

                except requests.exceptions.RequestException as e:
                    error_msg = e.response.json() if e.response else str(e)
                    logger.error(f"API error while fetching data: {error_msg}",
                                 extra={'log_data': {
                                     'event_code': 'API_ERROR',
                                     'load_params_id': self.load_params_id
                                 }}
                                 )
                    raise APIRequestException(error_msg)

                except Exception as e:
                    logger.error(
                        f"Unexpected error while fetching data: {str(e)}",
                        exc_info=True,
                        extra={
                            'log_data': {
                                'event_code': 'UNKNOWN_ERROR',
                                'load_params_id': self.load_params_id
                            }
                        }
                    )
                    raise APIRequestException(str(e))

