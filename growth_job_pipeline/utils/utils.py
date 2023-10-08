import datetime
import logging

from growth_job_pipeline.config import config
from growth_job_pipeline.shared_models.config_timestamps import (
    ConfigTimestamps,
)
from growth_job_pipeline.shared_models.coalesced_timestamps import (
    CoalescedTimestamps,
)

logger = logging.getLogger(__name__)


def get_config_timestamps() -> ConfigTimestamps:
    """
    Returns from_timestamp and to_timestamp (datetime.datetime or None) set in config
    :return: ConfigTimestamps
    """
    from_timestamp = config(
        "FROM_TIMESTAMP",
        default=None,
        cast=lambda x: None
        if x is None
        else datetime.datetime.fromisoformat(x),
    )
    to_timestamp = config(
        "TO_TIMESTAMP",
        default=None,
        cast=lambda x: None
        if x is None
        else datetime.datetime.fromisoformat(x),
    )
    return ConfigTimestamps(
        from_timestamp=from_timestamp, to_timestamp=to_timestamp
    )


def coalesce_run_timestamps(
    config_timestamps: ConfigTimestamps,
) -> CoalescedTimestamps:
    """
    Returns from_timestamp and to_timestamp (datetime.datetime) coalesced from config_timestamps
    Coalesce None from_timestamp to earliest possible datetime, and to_timestamp to latest possible datetime
    Check that to_timestamp is greater than from_timestamp or raise ValueError
    :param config_timestamps: ConfigTimestamps
    :return: CoalescedTimestamps
    """
    from_timestamp = config_timestamps.from_timestamp
    to_timestamp = config_timestamps.to_timestamp

    if from_timestamp is None:
        from_timestamp = datetime.datetime.min
    if to_timestamp is None:
        to_timestamp = datetime.datetime.max
    if to_timestamp <= from_timestamp:
        msg = f"to_timestamp={to_timestamp} less than or equal to from_timestamp={from_timestamp}"
        logger.error(msg)
        raise ValueError(msg)
    return CoalescedTimestamps(
        from_timestamp=from_timestamp, to_timestamp=to_timestamp
    )


def split_line_on_whitespace(line: str) -> list[str]:
    """
    Splits a line on whitespace
    Removes carriage return chars \r
    :param line: str
    :return: list[str]
    """
    # removes only carriage return chars \r
    # TODO @dsm fuller treatment might be needed if other MS-DOS control chars found in future
    return line.replace("\r", "").split()


def latest_datetime_possible_for_date(
    date: datetime.date,
) -> datetime.datetime:
    """
    Returns the latest possible datetime for a given date
    :param date: datetime.date
    :return: datetime.datetime
    """
    return datetime.datetime.combine(date, datetime.time.max)
