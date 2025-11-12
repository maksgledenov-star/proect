
from typing import List, Optional

from pydantic import Field

from core.models import BaseModel
from scenarios.common.models import WBAPISettingsModel


class WB24APISettingsModel(WBAPISettingsModel):
    """Model representing API settings from cfg.WBApiLoadParams table."""

    load_params_id: str = Field(alias="LoadParamsId", default='wb24_ProductsPricesReport')
    api_url: str = Field(
        alias="EndpointUrl",
        default="https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter"
    )
    status_forcelist: list = Field(alias="StatusForcelist", default=[429, 500, 502, 503, 504])
    limit: Optional[int] = Field(alias="Limit", ge=1, le=1000, default=1000)
    offset: Optional[int] = Field(alias="Offset", ge=0, default=0)


class SizeModel(BaseModel):
    """Model representing a product size."""

    sizeID: int = Field(alias="SizeID")
    price: int = Field(alias="Price")
    discountedPrice: float = Field(alias="DiscountedPrice")
    clubDiscountedPrice: float = Field(alias="ClubDiscountedPrice")
    techSizeName: str = Field(alias="TechSizeName")


class WB24ReportModel(BaseModel):
    """Model representing the WB24 report data structure."""

    nmID: int = Field(alias="NmID")
    vendorCode: str = Field(alias="VendorCode")
    sizes: List[SizeModel] = Field(alias="Sizes", default_factory=list)
    currencyIsoCode4217: str = Field(alias="CurrencyISOCode4217", default=None)
    discount: int = Field(alias="Discount")
    clubDiscount: int = Field(alias="ClubDiscount")
    editableSizePrice: bool = Field(alias="EditableSizePrice")
    isBadTurnover: Optional[bool] = Field(alias="IsBadTurnover", default=None)



