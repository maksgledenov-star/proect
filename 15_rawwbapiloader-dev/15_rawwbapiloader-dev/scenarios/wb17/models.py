from datetime import datetime
from typing import List, Optional, Any

from pydantic import Field, field_validator, ConfigDict
from pydantic import HttpUrl

from core.models import BaseModel
from scenarios.common.models import WBAPISettingsModel


class WB17APISettingsModel(WBAPISettingsModel):
    """Model representing API settings from cfg.WBApiLoadParams table."""

    load_params_id: str = Field(alias="LoadParamsId", default='wb17_ProductsReport')
    api_url: str = Field(alias="EndpointUrl", default="https://content-api.wildberries.ru/content/v2/get/cards/list")
    limit: int = Field(alias="Limit", ge=1, default=100)
    status_forcelist: list = Field(alias="StatusForcelist", default=[429, 500, 502, 503, 504])


class SizeInfoModel(BaseModel):
    """Model representing a product size."""

    chrtID: int = Field(alias="Числовой ID размера для данного артикула WB")
    techSize: str = Field(alias="Размер товара",description= "А, XXL, 57 и др.")
    wbSize: str = Field(alias="Российский размер товара")
    skus: List[str] = Field(
        alias="Баркод товара",
        description="example: 12345Ejf5",
        default_factory=list
    )

    
class CharacteristicsModel(BaseModel):
    """Model representing a product characteristic."""

    id: int = Field(alias="ID характеристики")
    name: str = Field(alias="Название характеристики")
    value: List[str | float | int] = Field(
        alias="Значение характеристики.",
        description=" Тип значения зависит от типа характеристики",
        default_factory=list
    )

    @field_validator('value', mode='before')
    @classmethod
    def ensure_list(cls, v: Any) -> List[str | float | int]:
        if v is None:
            return []
        if not isinstance(v, list):
            v = [v]
        return [str(item) if item is not None else "" for item in v]


class TagModel(BaseModel):
    """Model representing a product tag."""

    id: int = Field(alias="ID ярлыка")
    name: str = Field(alias="Название ярлыка")
    color: str = Field(
        alias="Цвет ярлыка",
        description="Доступные цвета:\
            D1CFD7 - серый\
            FEE0E0 - красный\
            ECDAFF - фиолетовый\
            E4EAFF - синий\
            DEF1DD - зеленый\
            FFECC7 - желтый"
    )


class DimensionsModel(BaseModel):
    """Model representing product dimensions."""

    length: float = Field(alias="Длина, см", description="см")  # in cm
    width: float = Field(alias="Ширина, см", description="см")  # in cm
    height: float = Field(alias="Высота, см", description="см")  # in cm
    weightBrutto: float = Field(alias="Вес, кг",description="Количество знаков после запятой <=3")
    isValid: bool = Field(
        alias="Потенциальная некорректность габаритов товара",
        description="true — не выявлена.\
                    isValid:true не гарантирует, что размеры указаны корректно. В отдельных случаях (например, при создании новой категории товаров) isValid:true \
                    будет возвращаться при любых значениях, кроме нулевых.\
                    false — указанные габариты значительно отличаются от средних по категории (предмету). Рекомендуется перепроверить, правильно ли указаны размеры\
                    товара в упаковке в сантиметрах. Функциональность карточки товара, в том числе начисление логистики и хранения, при этом ограничена не будет. \
                    Логистика и хранение продолжают начисляться — по текущим габаритам. Также isValid:false возвращается при отсутствии значений или нулевом значении любой стороны."
    )


class PhotoSizesModel(BaseModel):
    """Model representing different photo sizes."""

    big: Optional[HttpUrl] = Field(alias="URL фото 900x1200", default=None)
    c246x328: Optional[HttpUrl] = Field(alias="URL фото 246x328", default=None)
    c516x688: Optional[HttpUrl] = Field(alias="URL фото 516x688", default=None)
    square: Optional[HttpUrl] = Field(alias="URL фото 600x600", default=None)
    tm: Optional[HttpUrl] = Field(alias="URL фото 75x100", default=None)
    hq: Optional[HttpUrl] = Field(alias="HD", default=None)


class WholesaleInfoModel(BaseModel):
    """Model representing wholesale information for a product."""

    enabled: bool = Field(alias="Предназначена ли карточка товара для оптовой продажи")
    quantum: int = Field(alias="Количество единиц товара в упаковке")


class ProductCardModel(BaseModel):
    """Model representing a product card from WB API."""

    nmID: int = Field(alias="Артикул WB")
    imtID: int = Field(
        alias="ID объединённой карточки товара",
        description="Един для всех артикулов WB одной объединённой карточки товара. Есть у карточки товара, даже если она не объединена ни с одной другой карточкой")
    nmUUID: str = Field(alias="Внутренний технический ID карточки товара")
    subjectID: int = Field(alias="ID предмета")
    subjectName: str = Field(alias="Название предмета")
    vendorCode: str = Field(alias="Артикул продавца")
    brand: str = Field(alias="Бренд")
    title: str = Field(alias="Наименование товара")
    description: str = Field(alias="Описание товара")
    needKiz: bool = Field(
        alias="Нужен КИЗ",
        description="Требуется ли код маркировки для этого товара. false — не требуется, true — требуется"
    )
    photos: List[PhotoSizesModel] = Field(alias="Массив фото", default_factory=list)
    video: Optional[HttpUrl] = Field(alias="Видео", default=None)
    wholesale: Optional[WholesaleInfoModel] = Field(alias="Оптовая продажа", default=None)
    dimensions: Optional[DimensionsModel] = Field(alias="Габариты и вес товара c упаковкой, см и кг", default=None)
    characteristics: List[CharacteristicsModel] = Field(alias="Характеристики", default_factory=list)
    sizes: Optional[List[SizeInfoModel]] = Field(alias="Размеры", default_factory=list)
    tags: Optional[List[TagModel]] = Field(alias="Ярлыки", default_factory=list)
    createdAt: datetime = Field(alias="Дата и время создания",default=None)
    updatedAt: datetime = Field(alias="Дата и время изменения",default=None)

    @field_validator('createdAt','updatedAt', mode='before')
    @classmethod
    def ensure_datetime(cls, v: str | None) -> datetime | None:
        if v is None:
            return None
        # Parse the string into a datetime object
        return datetime.fromisoformat(v)