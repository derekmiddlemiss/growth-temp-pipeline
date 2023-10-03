import datetime
from pydantic import BaseModel, Extra, model_validator, StrictInt

from growth_job_pipeline.shared_models import Crop


class GrowthJob(BaseModel):
    crop: Crop
    start_date: datetime.datetime
    end_date: datetime.datetime
    id: StrictInt

    @model_validator(mode="after")
    def end_date_after_start_date(self) -> "GrowthJob":
        if self.end_date <= self.start_date:
            raise ValueError(f"end_date equal to or before start_date: {self}")
        return self

    class Config:
        use_enum_values = True
        extra = Extra.forbid
