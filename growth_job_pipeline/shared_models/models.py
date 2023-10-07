from datetime import datetime
from enum import Enum
from dataclasses import dataclass


# extension likely needed in future for different crops
class Crop(str, Enum):
    """
    Represents allowed crops
    """

    basil = "basil"
    chille = "chille"
    potato = "potato"
    brocolli = "brocolli"


@dataclass(frozen=True)
class ConfigTimestamps:
    """
    Represents timestamps loaded from config. Immutable.
    Attributes:
        from_timestamp: datetime | None
        to_timestamp: datetime | None
    """

    from_timestamp: datetime | None
    to_timestamp: datetime | None


@dataclass(frozen=True)
class CoalescedTimestamps:
    """
    Represents timestamps coalesced from config. Immutable.
    Attributes:
        from_timestamp: datetime
        to_timestamp: datetime
    """

    from_timestamp: datetime
    to_timestamp: datetime
