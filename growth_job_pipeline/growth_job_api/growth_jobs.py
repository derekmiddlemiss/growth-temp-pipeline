from __future__ import annotations
import datetime
import logging

import backoff
import pydantic
import requests

from typing import TYPE_CHECKING

from growth_job_pipeline.config import config
from growth_job_pipeline.models.validators.growth_job import GrowthJob

if TYPE_CHECKING:
    from growth_job_pipeline.models.enums.crop import Crop

logger = logging.getLogger(__name__)


@backoff.on_exception(backoff.expo, requests.RequestException, max_tries=3)
def get_time_filtered_growth_jobs_for_crop(
    from_timestamp: datetime.datetime,
    to_timestamp: datetime.datetime,
    crop: Crop,
) -> list[GrowthJob]:
    """
    Fetches growth jobs from API and filters to those completed in from -> to range for crop
    :param from_timestamp: datetime.datetime
    :param to_timestamp: datetime.datetime
    :param crop: Crop
    :return: list[GrowthJob]
    """
    response = requests.get(config("GROWTH_JOBS_API_URL"))
    response.raise_for_status()
    # generally speaking, would also expect to be storing/fetching an API key from config in prod
    # would probably also expect https rather than http
    # also might expect to handle API pagination - keep fetching until no next link
    # also might expect to handle API rate limiting
    # TODO @dsm ideally could we ask API maintainers to implement query params for filtering?
    try:
        jobs = [GrowthJob(**obj) for obj in response.json()]

        # get any jobs completed in from -> to range for crop
        filtered_jobs = [
            job
            for job in jobs
            if from_timestamp <= job.end_date < to_timestamp
            and job.crop == crop
        ]
        logger.info(
            f"Fetched {len(filtered_jobs)} from"
            f" {config('GROWTH_JOBS_API_URL')} from"
            f" timestamp={from_timestamp} to timestamp={to_timestamp} for"
            f" crop={crop}"
        )
        return filtered_jobs
    except requests.RequestException as e:
        logger.error(
            f"Error: {e}. Cannot fetch growth jobs from"
            f" {config('GROWTH_JOBS_API_URL')}"
        )
        raise e
    except pydantic.ValidationError as e:
        logger.error(f"Error: {e} validating growth jobs.")
        raise e
