import datetime

from pydantic import BaseModel, StrictInt, model_validator

from growth_job_pipeline.models.enums.crop import Crop
from growth_job_pipeline.models.enums.telemetry_measurement_type import (
    TelemetryMeasurementType,
)
from growth_job_pipeline.models.enums.telemetry_measurement_unit import (
    TelemetryMeasurementUnit,
)
from growth_job_pipeline.models.enums.weight_unit import WeightUnit
from growth_job_pipeline.utils import latest_datetime_possible_for_date

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

    @model_validator(mode="after")
    def date_ordering(self) -> "OutputRow":
        """
        Validates that growth_job_end_date is after growth_job_start_date
        Validates that yield_recorded_date is feasible given growth_job_end_date
        Validates that timestamp is between growth_job_start_date and growth_job_end_date
        :return: OutputRow
        """
        if self.growth_job_end_date <= self.growth_job_start_date:
            raise ValueError(f"end_date equal to or before start_date: {self}")
        if (
            latest_datetime_possible_for_date(self.yield_recorded_date)
            < self.growth_job_end_date
        ):
            raise ValueError(
                "growth_job_end_date after latest possible date for"
                f" yield_recorded_date: {self}"
            )
        if (
            not self.growth_job_start_date
            <= self.timestamp
            <= self.growth_job_end_date
        ):
            raise ValueError(
                "timestamp not between growth_job_start_date and"
                f" growth_job_end_date: {self}"
            )
        return self

    class Config:
        use_enum_values = True
        extra = "forbid"
        frozen = True
