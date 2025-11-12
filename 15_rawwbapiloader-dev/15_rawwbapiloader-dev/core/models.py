from pydantic import ConfigDict, BaseModel as BasePydanticModel


class BaseModel(BasePydanticModel):
    """Base model for all models."""
    model_config = ConfigDict(
        validate_by_name=True,
        extra='ignore',  # ← do NOT raise on extras
        validate_default=True,
        str_strip_whitespace=True,
        strict=True,
        from_attributes=True,
        validate_return=True,
    )