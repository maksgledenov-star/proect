import logging
from typing import Dict, Any, Type, List

from pydantic import ValidationError
from pyodbc import Row

from core.models import BaseModel
from core.transformers import BaseDataTransformer
from core.utils.exceptions import DataProcessingError, CustomValidationError

logger = logging.getLogger(__name__)



class WBDataTransformer(BaseDataTransformer):
    """Data transformation class for handling WB scenario operations."""

    modes = ['json', 'python']

    def __init__(self, data_model: Type[BaseModel] = None):
        self.data_model = data_model

    @staticmethod
    def _to_json_string(field: List | Dict | Any):
        """
        Convert a given field to a JSON string.

        Args:
            field: List or Dict or Any

        Returns:
            str: JSON string representation of the field
        """
        try:
            return str(field).replace("'", '"') if field else None
        except Exception as e:
            logger.error(f"Error converting field to JSON string: {e}",
                extra={'log_data': {'event_code': 'PROCESS_DATA_ERROR'}}
            )
            raise


    def _validate_data(self, data: Dict[str, Any] | Row) -> BaseModel:
        """
        Validate data using the data model.

        Args:
            data: Dictionary or Row object to validate

        Returns:
            BaseModel: Validated data model object
        """
        try:
            return self.data_model.model_validate(data)

        except ValidationError as e:
                error = e.errors()[0]# Get first error
                field_name = error.get('loc', 'unknown')
                error_msg = f"{error.get('msg', 'Validation failed')}: {field_name}"
                
                logger.error(
                    error_msg,
                    extra={'log_data': {'event_code': 'VALIDATE_DATA_ERROR'}}
                )
                raise CustomValidationError(error_msg)


    def _model_to_dict(self, model_obj: BaseModel, mode: str):
        """
        Convert a model object to a dictionary.

        Args:
            model_obj: BaseModel object to convert
            mode: Mode to use for conversion ('json' or 'python')

        Returns:
            dict: Dictionary representation of the model object
        """
        try:
            if mode not in self.modes:
                raise ValueError(f"Invalid mode: {mode}")

            return model_obj.model_dump(mode=mode)
        except Exception as e:
            logger.error(f"Error converting model to dictionary: {e}",
                         extra={'log_data': {'event_code': 'VALIDATE_DATA_ERROR'}}
                         )
            raise


    def _clean_data(self, data: Dict[str, Any]) -> Dict[str, Any] | None:
        raise NotImplementedError


    def process_data(self, data: List[Dict[str, Any]]) -> List[tuple] | Exception:
        """Process raw product data with flexible validation."""

        processed_data = []
        for record in data:
            try:
                model_obj = self._validate_data(record)
                # Convert model object to a dictionary
                python_dict = self._model_to_dict(model_obj, mode='json')
                # Clean the data
                clean_record = self._clean_data(python_dict)
                processed_data.append(clean_record)

            except Exception as e:
                logger.error(f"Error processing product data: {str(e)}",
                    extra={'log_data': {'event_code': 'TRANSFORM_DATA_ERROR'}}
                )
                raise DataProcessingError(str(e))

        if not processed_data:
            logger.warning(
                "No data to be inserted in the DB",
                extra={'log_data': {'event_code': 'INSERT_DATA_ERROR'}}
            )
            raise DataProcessingError("No data to be inserted in the DB")

        logger.info(f"Processed {len(processed_data)} objects",
                    extra={'log_data': {'event_code': 'TRANSFORM_DATA_SUCCESS'}}
                    )

        return processed_data

class WBAPISettingsTransformer(WBDataTransformer):
    """Data transformation class for handling API settings."""

    def __init__(self, data_model):
        super().__init__(data_model)
        self.default_model_obj = self.data_model()
        
    @staticmethod
    def _row_to_dict(row: Row) -> Dict[str, Any]:
        """Convert database row to dictionary."""
        try:
            return {
                "EndpointUrl": row.EndpointUrl,
                "LoadParamsId": row.Id,
                "IsTestData": row.IsTestData
            }
        except Exception as e:
            logger.error(f"Error converting row to dictionary: {e}",
                        extra={'log_data': {'event_code': 'TRANSFORM_DATA_ERROR'}}
                        )
            raise

    def process_data(self, data: Row | None) -> Dict[str, Any]:
        """
        Process database row into WBApiSettings model.

        Args:
            data: Database row or None if no settings found

        Returns:
            Dictionary containing WBAPISettings data (with defaults if data is None or invalid)
        """
        if data is None:
            logger.warning("No database row provided, using default WBAPISettings",
                         extra={'log_data': {'event_code': 'LOAD_CONFIG_ERROR'}}
                         )
            return self._get_default_settings()

        try:
            row_dict = self._row_to_dict(data)
            model_obj = self._validate_data(row_dict)
            return self._model_to_dict(model_obj, mode='python')

        except Exception as e:
            logger.error(f"Error processing API settings: {e}, using defaults",
                       extra={'log_data': {'event_code': 'TRANSFORM_DATA_ERROR'}}
                       )

        return self._get_default_settings()

    def _get_default_settings(self) -> Dict[str, Any]:
        """Helper method to get default settings as a dictionary."""
        return self._model_to_dict(self.default_model_obj, mode='python')