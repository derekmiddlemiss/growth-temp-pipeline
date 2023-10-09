import datetime

from pydantic import BaseModel, StrictInt, model_validator

from growth_job_pipeline.models.enums.crop import Crop


class GrowthJob(BaseModel):
    """
    Represents a growth job. Immutable.
    Validated on creation to ensure end_date is after start_date if end_date is set
    Attributes:
        crop: Crop
        start_date: datetime.datetime
        end_date: datetime.datetime | None
        id: StrictInt
    """

    crop: Crop
    start_date: datetime.datetime
    end_date: datetime.datetime | None = None
    id: StrictInt

    @model_validator(mode="after")
    def end_date_after_start_date(self) -> "GrowthJob":
        """
        Validates that end_date is after start_date
        :return: GrowthJob
        """
        if self.end_date and self.end_date <= self.start_date:
            raise ValueError(f"end_date equal to or before start_date: {self}")
        return self

    class Config:
        use_enum_values = True
        extra = "forbid"
        frozen = True
