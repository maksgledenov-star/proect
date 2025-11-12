from typing import Iterable, Optional

from pydantic import Field

from core.models import BaseModel


class WBAPISettingsModel(BaseModel):
    """Model representing API settings from cfg.WBApiLoadParams table."""

    load_params_id: str = Field(alias="LoadParamsId",default=None)
    is_test_data: bool = Field(alias="IsTestData", default=False)
    api_url: str = Field(alias="EndpointUrl",default=None)
    timeout: int = Field(alias="RequestTimeout", ge=1, default=30)
    max_retries: int = Field(alias="MaxRetries", ge=0, default=5)
    backoff_factor: float = Field(alias="BackoffFactor", ge=0, default=0.3)
    status_forcelist: Iterable[int] = Field(alias="StatusForcelist", default=[])
    allowed_methods: list = Field(alias="AllowedMethods", default=["GET", "POST"])