import datetime
import os

from logger import setup_logger
from src.config import config
from src.telemetry_db.models import MeasurementUnit, MeasurementType
from telemetry_db.db import telemetry_entries_since_timestamp_batcher

if __name__ == "__main__":
    setup_logger()
    batches = telemetry_entries_since_timestamp_batcher(
        type_to_fetch=MeasurementType(config("MEASUREMENT_TYPE")),
        unit_to_fetch=MeasurementUnit(config("MEASUREMENT_UNIT")),
        batch_size=config("TELEMETRY_DB_BATCH_SIZE", cast=int)
        # timestamp=datetime.datetime(2021, 1, 1, 0, 0, 0)
    )
    for batch in batches:
        print(len(batch))
