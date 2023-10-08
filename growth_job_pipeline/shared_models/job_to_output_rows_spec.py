import datetime

from pydantic import BaseModel, Extra, StrictInt

from growth_job_pipeline.shared_models.crop import Crop
from growth_job_pipeline.shared_models.weight_unit import WeightUnit


class JobToOutputRowsSpec(BaseModel):
    """
    Represents output file. Immutable.
    Attributes:
        run_id: UUID
        yield_result: YieldResult
        matched_growth_job: GrowthJob
    """

    crop: Crop
    growth_job_start_date: datetime.datetime
    growth_job_end_date: datetime.datetime
    growth_job_id: StrictInt
    yield_recorded_date: datetime.date
    yield_weight: float
    yield_unit: WeightUnit

    class Config:
        use_enum_values = True
        extra = Extra.forbid
        frozen = True
