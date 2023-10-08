from datetime import datetime

from pydantic import BaseModel, Extra


class ConfigTimestamps(BaseModel):
    """
    Represents timestamps loaded from config. Immutable.
    Attributes:
        from_timestamp: datetime | None
        to_timestamp: datetime | None
    """

    from_timestamp: datetime | None
    to_timestamp: datetime | None

    class Config:
        extra = Extra.forbid
        frozen = True
