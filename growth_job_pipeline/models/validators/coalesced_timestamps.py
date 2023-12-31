from datetime import datetime

from pydantic import BaseModel


class CoalescedTimestamps(BaseModel):
    """
    Represents timestamps coalesced from config. Immutable.
    Attributes:
        from_timestamp: datetime
        to_timestamp: datetime
    """

    from_timestamp: datetime
    to_timestamp: datetime

    class Config:
        extra = "forbid"
        frozen = True
