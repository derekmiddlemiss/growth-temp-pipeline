import datetime

from pydantic import BaseModel, Extra

from growth_job_pipeline.models.enums.telemetry_measurement_type import (
    TelemetryMeasurementType,
)
from growth_job_pipeline.models.enums.telemetry_measurement_unit import (
    TelemetryMeasurementUnit,
)


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
