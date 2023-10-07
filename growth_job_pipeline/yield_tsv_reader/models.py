import datetime
from enum import Enum

from pydantic import BaseModel, Extra

from growth_job_pipeline.shared_models import Crop


# if different units are used in future, might need normalisation to single output unit e.g. g -> kg, lbs -> kg etc
class WeightUnit(str, Enum):
    """
    Represents allowed weight unit
    """

    kg = "kg"


# assume weight can be a float - all current results are int, but not many results yet and floats
# can be envisaged in future
class YieldResult(BaseModel):
    """
    Represents a yield result. Mutable to allow for growth job id to be added after creation.
    Later assignments are validated.
    """

    date: datetime.date
    crop: Crop
    weight: float
    unit: WeightUnit
    growth_job_id: int | None

    class Config:
        use_enum_values = True
        extra = Extra.forbid
        validate_assignment = True
