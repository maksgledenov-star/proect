import logging
from typing import Dict, Any, Optional, List
import requests

from scenarios.common.client import WBAPIClient

logger = logging.getLogger(__name__)


class WB17APIClient(WBAPIClient):
    """Client for making requests to the WB17 API."""
    def __init__(self, api_settings: Dict[str, Any]):
        """Initialize the WB17 API client with required API settings.

        Args:
            api_settings: Dictionary containing API configuration
        """
        super().__init__(api_settings)

        self.load_params_id = api_settings['load_params_id']
        self.limit = api_settings['limit']
        self.api_url = api_settings['api_url']
        self.timeout = api_settings['timeout']
        self.max_retries = api_settings['max_retries']
        self.backoff_factor = api_settings['backoff_factor']
        self.status_forcelist = api_settings['status_forcelist']
        self.allowed_methods = api_settings['allowed_methods']
        self._api_key = api_settings['api_key']

        self.session = self._set_session()
        
    def fetch_data(self) -> List[Dict[str, Any] | Exception]:
        """
        Get all product cards with automatic pagination.
        
        Returns:
            List of all product cards
        """
        all_cards = []
        updated_at = None
        nm_id = None


        with self.session as s:
            while True:
                try:
                    # Build request payload
                    payload = self._build_payload(updated_at, nm_id)
                    
                    # Make API request
                    response = s.post(
                        url=self.api_url,
                        json=payload,
                        timeout=self.timeout
                    )
                    response.raise_for_status()
                    response_data = response.json()
                    
                    # Process response
                    cards = response_data.get('cards', [])

                    if not cards:
                        err_msg = f"Empty response for payload: {payload}"
                        logger.error(f"{err_msg}",
                            extra={'log_data': {
                                'event_code': 'EXTRACT_DATA_ERROR',
                                'load_params_id': self.load_params_id
                            }}
                        )
                        raise Exception("Could not extract data.")
                        
                    all_cards.extend(cards)
                    logger.info(f"Extracted {len(cards)} cards",
                        extra={'log_data': {
                            'event_code': 'EXTRACT_DATA_SUCCESS',
                            'load_params_id': self.load_params_id
                        }}
                    )
                    
                    # Check if we've reached the end of the data
                    if self._is_last_batch(cards, response_data):
                        logger.info(f"All data extracted: {len(all_cards)}",
                            extra={'log_data': {
                                'event_code': 'EXTRACT_DATA_SUCCESS',
                                'load_params_id': self.load_params_id
                            }}
                        )
                        return all_cards
                        
                    # Update cursor for next request
                    last_card = cards[-1]
                    updated_at = last_card.get('updatedAt')
                    nm_id = last_card.get('nmID')
                    
                    # Validate we have necessary cursor values for next request
                    if updated_at is None or nm_id is None:
                        err_msg = f"Incomplete cursor data for pagination: {payload}"
                        logger.error(f"{err_msg}",
                            extra={'log_data': {
                                'event_code': 'EXTRACT_DATA_ERROR',
                                'load_params_id': self.load_params_id
                            }}
                        )
                        raise Exception("Could not extract data.")
                        
                except requests.exceptions.RequestException as e:
                    error_msg = e.response.json() if e.response else str(e)
                    logger.error(f"API error while fetching data: {error_msg}",
                        extra={'log_data': {
                            'event_code': 'API_ERROR',
                            'load_params_id': self.load_params_id
                        }}
                    )
                    raise

    def _build_payload(self, updated_at: Optional[str], nm_id: Optional[int]) -> Dict[str, Any]:
        """Build the request payload with cursor information."""
        return {
            "settings": {
                "cursor": {
                    "limit": self.limit,
                    "updatedAt": updated_at,
                    "nmID": nm_id
                },
                "filter": {
                    "withPhoto": -1
                }
            }
        }
        
    def _is_last_batch(self, cards: List[Dict], response_data: Dict) -> bool:
        """Determine if this is the last batch of data."""
        cursor = response_data.get('cursor', {})
        total = cursor.get('total')
        
        # If we received fewer cards than the limit, it's the last batch
        if len(cards) < self.limit:
            return True
            
        # If we've reached or exceeded the total count
        if total is not None and len(cards) > total:
            return True
            
        return False

