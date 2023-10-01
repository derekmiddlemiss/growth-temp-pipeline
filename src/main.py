import datetime
import os

from logger import setup_logger
from src.config import config
from src.telemetry_db.models import MeasurementUnit, MeasurementType
from telemetry_db.db import telemetry_entries_batcher
from yield_tsv_reader.yield_results import yield_results_batcher

if __name__ == "__main__":
    setup_logger()

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
