import datetime

from pydantic import BaseModel, Extra

from growth_job_pipeline.shared_models.crop import Crop
from growth_job_pipeline.shared_models.weight_unit import WeightUnit


# assume weight can be a float - all current results are int, but not many results yet and floats
# can be envisaged in future
class YieldResult(BaseModel):
    """
    Represents a yield result. Immutable.
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
