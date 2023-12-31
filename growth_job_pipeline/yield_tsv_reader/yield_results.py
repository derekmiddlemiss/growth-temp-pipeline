import datetime
import logging

from pydantic import ValidationError

from growth_job_pipeline.config import config
from growth_job_pipeline.models.validators.yield_result import YieldResult
from growth_job_pipeline.utils import (
    split_line_on_whitespace,
    latest_datetime_possible_for_date,
)

logger = logging.getLogger(__name__)


def get_ascending_yield_results(
    from_timestamp: datetime.datetime, to_timestamp: datetime.datetime
) -> list[YieldResult]:
    """
    Returns a list of YieldResult objects from the yield results file
    Filters out any yield results outside of the from_timestamp -> to_timestamp range
    Assumes that yield results logged at latest possible datetime for a given date
    :param from_timestamp: datetime.datetime
    :param to_timestamp: datetime.datetime
    :return: list[YieldResult]
    """
    try:
        with open(config("YIELD_RESULTS_FILE"), "r") as file:
            column_names = split_line_on_whitespace(file.readline())
            lines = [split_line_on_whitespace(line) for line in file]

        results = [
            YieldResult(**dict(zip(column_names, line))) for line in lines
        ]
    except IOError as e:
        logger.error(
            "Cannot read from yield results"
            f" file={config('YIELD_RESULTS_FILE')}"
        )
        raise e
    except ValidationError as e:
        logger.error(f"Error {e}. Cannot validate yield results.")
        raise e

    filtered_results = [
        result
        for result in results
        if from_timestamp
        <= latest_datetime_possible_for_date(result.date)
        < to_timestamp
    ]

    return sorted(filtered_results, key=lambda result: result.date)
