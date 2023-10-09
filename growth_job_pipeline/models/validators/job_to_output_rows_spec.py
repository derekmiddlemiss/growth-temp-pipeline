import datetime

from pydantic import BaseModel, StrictInt, model_validator

from growth_job_pipeline.models.enums.crop import Crop
from growth_job_pipeline.models.enums.weight_unit import WeightUnit
from growth_job_pipeline.utils import latest_datetime_possible_for_date


class JobToOutputRowsSpec(BaseModel):
    """
    Represents a job to output rows spec. Immutable.
    Attributes:
        crop: Crop
        growth_job_start_date: datetime.datetime
        growth_job_end_date: datetime.datetime
        growth_job_id: StrictInt
        yield_recorded_date: datetime.date
        yield_weight: float
        yield_unit: WeightUnit
    """

    crop: Crop
    growth_job_start_date: datetime.datetime
    growth_job_end_date: datetime.datetime
    growth_job_id: StrictInt
    yield_recorded_date: datetime.date
    yield_weight: float
    yield_unit: WeightUnit

    @model_validator(mode="after")
    def date_ordering(self) -> "JobToOutputRowsSpec":
        """
        Validates that growth_job_end_date is after growth_job_start_date
        Validates that yield_recorded_date is feasible given growth_job_end_date
        :return: GrowthJob
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
        return self

    class Config:
        use_enum_values = True
        extra = "forbid"
        frozen = True
