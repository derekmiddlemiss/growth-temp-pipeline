import datetime
from enum import Enum

from pydantic import BaseModel, Extra


class TelemetryMeasurementType(str, Enum):
    temp = "temp"


class TelemetryMeasurementUnit(str, Enum):
    C = "C"
    F = "F"


# for now, validation is not conditional, as telemetry DB contains just one type of measurement: "temp"
# eventually, might want conditions, e.g. for measurement_type="temp", allowed measurement_units are ["T", "F"],
# while for measurement_type="humidity", allowed measurement_units are ["percent", "gm^-3"]
class TelemetryEntry(BaseModel):
    """
    Represents a telemetry database entry. Immutable.
    Attributes:
        timestamp: datetime.datetime
        type: MeasurementType
        value: float
        unit: MeasurementUnit
    """

    timestamp: datetime.datetime
    type: TelemetryMeasurementType
    value: float
    unit: TelemetryMeasurementUnit

    class Config:
        use_enum_values = True
        extra = Extra.forbid
        frozen = True
