import datetime
import logging
from collections.abc import Generator
from typing import Iterable

import backoff
import pyodbc
from pydantic import ValidationError

from growth_job_pipeline.config import config
from growth_job_pipeline.models.validators.telemetry_entry import (
    TelemetryEntry,
)
from growth_job_pipeline.models.enums.telemetry_measurement_unit import (
    TelemetryMeasurementUnit,
)
from growth_job_pipeline.models.enums.telemetry_measurement_type import (
    TelemetryMeasurementType,
)

logger = logging.getLogger(__name__)


@backoff.on_exception(backoff.expo, pyodbc.Error, max_tries=3)
def get_telemetry_db_cursor() -> pyodbc.Cursor:
    """
    Returns a cursor for the telemetry DB
    :return: pyodbc.Cursor
    """
    # sudo apt install unixodbc
    # curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc
    # curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
    # sudo apt-get update
    # sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
    # TODO @dsm alpine setup at https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server
    connection_string = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={config('TELEMETRY_DB_HOST')},{config('TELEMETRY_DB_PORT', cast=int)};"
        f"DATABASE={config('TELEMETRY_DB_NAME')};"
        f"UID={config('TELEMETRY_DB_USERNAME')};"
        f"PWD={config('TELEMETRY_DB_PASSWORD')};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes"
    )
    try:
        connection = pyodbc.connect(connection_string)
        logger.info("Connected to telemetry DB.")
        return connection.cursor()
    except pyodbc.Error as e:
        logger.error(f"Could not connect to telemetry DB. Error: {e}")
        raise e


def get_row_count(
    cursor: pyodbc.Cursor,
    from_timestamp: datetime.datetime,
    to_timestamp: datetime.datetime,
    type_to_fetch: TelemetryMeasurementType,
    unit_to_fetch: TelemetryMeasurementUnit,
) -> int:
    query = """
        SELECT COUNT(*)
        FROM dbo.telemetry
        WHERE timestamp >= ? AND timestamp <= ? AND type = ? AND unit = ?
    """
    try:
        return cursor.execute(
            query, (from_timestamp, to_timestamp, type_to_fetch, unit_to_fetch)
        ).fetchone()[0]
    except pyodbc.Error as e:
        logger.error(
            f"Could not fetch row count from telemetry DB. Error: {e}"
        )
        raise e


def get_validated_entries(
    column_names: list[str], rows: list[Iterable], num_batches_fetched: int
) -> list[TelemetryEntry]:
    try:
        return [TelemetryEntry(**dict(zip(column_names, row))) for row in rows]
    except ValidationError as e:
        logger.error(
            f"Error: {e} validating telemetry DB rows. Batches"
            f" fetched={num_batches_fetched}"
        )
        raise e


def telemetry_entries_batcher(
    cursor: pyodbc.Cursor,
    type_to_fetch: TelemetryMeasurementType,
    unit_to_fetch: TelemetryMeasurementUnit,
    from_timestamp: datetime.datetime,
    to_timestamp: datetime.datetime,
    batch_size=1000,
) -> Generator[list[TelemetryEntry], None, None]:
    row_count = get_row_count(
        cursor=cursor,
        from_timestamp=from_timestamp,
        to_timestamp=to_timestamp,
        type_to_fetch=type_to_fetch,
        unit_to_fetch=unit_to_fetch,
    )
    logger.info(
        f"{row_count} rows to fetch from timestamp={from_timestamp} to"
        f" timestamp={to_timestamp} for type={type_to_fetch.value},"
        f" unit={unit_to_fetch.value}"
    )

    num_batches_fetched = 0
    while row_count > 0:
        query = """
            SELECT *
            FROM dbo.telemetry
            WHERE timestamp >= ? AND timestamp <= ? AND type = ? AND unit = ?
            ORDER BY timestamp ASC
            OFFSET ? ROWS FETCH FIRST ? ROWS ONLY;
        """
        try:
            rows = cursor.execute(
                query,
                (
                    from_timestamp,
                    to_timestamp,
                    type_to_fetch,
                    unit_to_fetch,
                    num_batches_fetched * batch_size,
                    batch_size,
                ),
            ).fetchall()
            column_names = [
                column_spec[0] for column_spec in cursor.description
            ]
        except pyodbc.Error as e:
            logger.error(
                f"Error: {e} fetching batch from telemetry DB. Batches"
                f" fetched={num_batches_fetched}"
            )
            raise e

        entries = get_validated_entries(
            column_names, rows, num_batches_fetched
        )
        num_batches_fetched += 1
        row_count -= len(entries)
        logger.debug(
            f"Batch number={num_batches_fetched}, rows_in_batch={len(entries)}"
        )
        yield entries
