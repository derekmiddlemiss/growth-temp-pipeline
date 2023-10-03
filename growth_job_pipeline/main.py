import datetime

from growth_job_pipeline.config import config
from growth_job_pipeline.growth_job_api import get_growth_jobs
from growth_job_pipeline.logger import setup_logger
from growth_job_pipeline.telemetry_db import (
    MeasurementUnit,
    MeasurementType,
    telemetry_entries_batcher,
)
from growth_job_pipeline.yield_tsv_reader import yield_results_batcher

if __name__ == "__main__":
    setup_logger()

    growth_jobs = get_growth_jobs()

    yield_batches = yield_results_batcher(batch_size=2)
    for batch in yield_batches:
        print("YB", len(batch))

    telemetry_batches = telemetry_entries_batcher(
        type_to_fetch=MeasurementType(config("MEASUREMENT_TYPE")),
        unit_to_fetch=MeasurementUnit(config("MEASUREMENT_UNIT")),
        batch_size=config("TELEMETRY_DB_BATCH_SIZE", cast=int),
        since_timestamp=config(
            "SINCE_TIMESTAMP",
            default=None,
            cast=lambda x: None
            if x is None
            else datetime.datetime.fromisoformat(x),
        ),
    )
    for batch in telemetry_batches:
        print("TB", len(batch))
