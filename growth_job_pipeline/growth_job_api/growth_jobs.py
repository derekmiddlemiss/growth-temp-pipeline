import datetime
import logging

import backoff
import pydantic
import requests

from growth_job_pipeline.config import config
from .models import GrowthJob

logger = logging.getLogger(__name__)


@backoff.on_exception(backoff.expo, requests.RequestException, max_tries=3)
def get_growth_jobs(
    from_timestamp: datetime.datetime, to_timestamp: datetime.datetime
) -> list[GrowthJob]:
    response = requests.get(config("GROWTH_JOBS_API_URL"))
    response.raise_for_status()
    # generally speaking, would also expect to be storing/fetching an API key from config in prod
    # would probably also expect https rather than http
    # also might expect to handle API pagination - keep fetching until no next link
    try:
        jobs = [GrowthJob(**obj) for obj in response.json()]

        # get any jobs completed in from -> to range
        filtered_jobs = [
            job
            for job in jobs
            if from_timestamp <= job.end_date < to_timestamp
        ]
        logger.info(
            f"Fetched {len(filtered_jobs)} from {config('GROWTH_JOBS_API_URL')} from timestamp={from_timestamp} to timestamp={to_timestamp}"
        )
        return filtered_jobs
    except pydantic.ValidationError as e:
        logger.error(f"Error: {e} validating growth jobs.")
        raise e
