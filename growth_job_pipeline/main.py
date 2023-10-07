import logging

from growth_job_pipeline.config import config
from growth_job_pipeline.growth_job_api import get_growth_jobs
from growth_job_pipeline.logger import setup_logger
from growth_job_pipeline.telemetry_db import (
    MeasurementUnit,
    MeasurementType,
    telemetry_entries_batcher,
)
from growth_job_pipeline.telemetry_db.db import get_cursor
from growth_job_pipeline.utils import (
    get_config_timestamps,
    coalesce_run_timestamps,
)
from growth_job_pipeline.yield_tsv_reader import get_yield_results

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    setup_logger()

    run_timestamps = get_config_timestamps()
    coalesced_timestamps = coalesce_run_timestamps(run_timestamps)

    from_timestamp = coalesced_timestamps.from_timestamp
    to_timestamp = coalesced_timestamps.to_timestamp

    yield_results = get_yield_results(from_timestamp, to_timestamp)

    growth_jobs = get_growth_jobs(from_timestamp, to_timestamp)
    yield_results = get_yield_results(from_timestamp, to_timestamp)

    cursor = get_cursor()
    telemetry_batches = telemetry_entries_batcher(
        cursor=cursor,
        type_to_fetch=MeasurementType(config("MEASUREMENT_TYPE")),
        unit_to_fetch=MeasurementUnit(config("MEASUREMENT_UNIT")),
        batch_size=config("TELEMETRY_DB_BATCH_SIZE", cast=int),
        from_timestamp=from_timestamp,
        to_timestamp=to_timestamp,
    )

    for batch in telemetry_batches:
        print("TB", len(batch))
