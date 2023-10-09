import datetime

from pydantic import BaseModel, StrictInt

from growth_job_pipeline.models.enums.crop import Crop
from growth_job_pipeline.models.enums.weight_unit import WeightUnit
from growth_job_pipeline.models.enums.telemetry_measurement_unit import (
    TelemetryMeasurementUnit,
)
from growth_job_pipeline.models.enums.telemetry_measurement_type import (
    TelemetryMeasurementType,
)

output_columns = [
    "timestamp",
    "crop",
    "growth_job_id",
    "growth_job_start_date",
    "growth_job_end_date",
    "yield_recorded_date",
    "yield_weight",
    "yield_unit",
    "telemetry_measurement_type",
    "telemetry_measurement_unit",
    "telemetry_measurement_value",
]


class OutputRow(BaseModel):
    """
    Represents a row in the output file. Immutable.
    Attributes:
        timestamp: datetime.datetime
        crop: Crop
        growth_job_id: UUID
        growth_job_start_date: datetime.datetime
        growth_job_end_date: datetime.datetime
        yield_recorded_date: datetime.date
        yield_weight: float
        yield_unit: WeightUnit
        telemetry_measurement_type: TelemetryMeasurementType
        telemetry_measurement_unit: TelemetryMeasurementUnit
        telemetry_measurement_value: float
    """

    timestamp: datetime.datetime
    crop: Crop
    growth_job_id: StrictInt
    growth_job_start_date: datetime.datetime
    growth_job_end_date: datetime.datetime
    yield_recorded_date: datetime.date
    yield_weight: float
    yield_unit: WeightUnit
    telemetry_measurement_type: TelemetryMeasurementType
    telemetry_measurement_unit: TelemetryMeasurementUnit
    telemetry_measurement_value: float

    class Config:
        use_enum_values = True
        extra = "forbid"
        frozen = True
