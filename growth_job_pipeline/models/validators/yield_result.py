import datetime

from pydantic import BaseModel, Extra

from growth_job_pipeline.models.enums.crop import Crop
from growth_job_pipeline.models.enums.weight_unit import WeightUnit


class YieldResult(BaseModel):
    """
    Represents a yield result. Immutable.
    Assumes weight can be a float, as no a priori reason to restrict to integers
    Attributes:
        date: datetime.date
        crop: Crop
        weight: float
        unit: WeightUnit
    """

    date: datetime.date
    crop: Crop
    weight: float
    unit: WeightUnit

    class Config:
        use_enum_values = True
        extra = Extra.forbid
        frozen = True
