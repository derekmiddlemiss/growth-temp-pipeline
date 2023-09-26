import datetime
from enum import Enum

from pydantic import BaseModel


class Type(Enum):
    temp = "temp"


class Unit(Enum):
    T = "T"
    F = "F"


# for now, validation is not conditional, as telemetry DB contains just one type of measurement: "temp"
# eventually, might want conditions, e.g. for type="temp", allowed units are ["T", "F"], while for
# type="humidity", allowed units are
class TelemetryEntry(BaseModel):
    timestamp: datetime.datetime
    type: Type
    value: float
    unit: Unit
