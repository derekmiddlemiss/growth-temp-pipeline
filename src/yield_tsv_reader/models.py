import datetime
from enum import Enum

from pydantic import BaseModel, Extra


# extension likely needed in future for different crops
class Crop(str, Enum):
    basil = "basil"
    chille = "chille"
    potato = "potato"
    brocolli = "brocolli"


# if different units are used in future, might need normalisation to single output unit e.g. g -> kg, lbs -> kg etc
class WeightUnit(str, Enum):
    kg = "kg"


# assume weight can be a float - all current results are int, but not many results and floats envisaged in future
class YieldResult(BaseModel):
    date: datetime.date
    crop: Crop
    weight: float
    unit: WeightUnit

    class Config:
        use_enum_values = True
        extra = Extra.forbid
