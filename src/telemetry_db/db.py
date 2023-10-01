import datetime
import logging
from collections.abc import Generator

import pyodbc
from pydantic import ValidationError

from src.config import config
from src.telemetry_db.models import (
    TelemetryEntry,
    MeasurementUnit,
    MeasurementType,
)

logger = logging.getLogger(__name__)


# def handle_datetime2(dt2_value: bytes) -> datetime.datetime:
#     tup = struct.unpack("<6hI", dt2_value)
#     return datetime.datetime(tup[0], tup[1], tup[2],
#                              hour=tup[3], minute=tup[4], second=tup[5],
#                              microsecond=math.floor(tup[6] / 1000.0 + 0.5))


def get_cursor() -> pyodbc.Cursor:
    # sudo apt install unixodbc
    # curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc
    # curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
    # sudo apt-get update
    # sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
    # TODO @dsm alpine setup at https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server
    # TODO @dsm turning off SSL encryption here not ideal - hopefully won't be an issue on Alpine
    connection_string = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={config('TELEMETRY_DB_HOST')},{config('TELEMETRY_DB_PORT', cast=int)};"
        f"DATABASE={config('TELEMETRY_DB_NAME')};"
        f"UID={config('TELEMETRY_DB_USERNAME')};"
        f"PWD={config('TELEMETRY_DB_PASSWORD')};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes"
    )
    try:
        connection = pyodbc.connect(connection_string)
        # connection.add_output_converter(pyodbc.SQL_TYPE_TIMESTAMP, handle_datetime2)
        logger.info("Connected to telemetry DB.")
        return connection.cursor()
    except pyodbc.Error as e:
        logger.error(
            f"Could not connect to telemetry DB. Error: {e}", stack_info=True
        )
        raise e


def get_row_count(
    cursor: pyodbc.Cursor,
    query_timestamp: datetime.datetime,
    type_to_fetch: MeasurementType,
    unit_to_fetch: MeasurementUnit,
) -> int:
    query = """
        SELECT COUNT(*)
        FROM dbo.telemetry
        WHERE timestamp >= ? AND type = ? AND unit = ?
    """
    try:
        return cursor.execute(
            query, (query_timestamp, type_to_fetch, unit_to_fetch)
        ).fetchone()[0]
    except pyodbc.Error as e:
        logger.error(
            f"Could not fetch row count from telemetry DB. Error: {e}",
            stack_info=True,
        )
        raise e


def telemetry_entries_batcher(
    type_to_fetch: MeasurementType,
    unit_to_fetch: MeasurementUnit,
    since_timestamp: datetime.datetime | None = None,
    batch_size=1000,
) -> Generator[list[TelemetryEntry], None, None]:
    cursor = get_cursor()
    if since_timestamp is None:
        query_timestamp = datetime.datetime.min
    else:
        query_timestamp = since_timestamp

    row_count = get_row_count(
        cursor, query_timestamp, type_to_fetch, unit_to_fetch
    )
    logger.info(
        f"{row_count} rows to fetch since timestamp={since_timestamp} "
        f"for type={type_to_fetch.value}, unit={unit_to_fetch.value}"
    )

    num_batches_fetched = 0
    while row_count > 0:
        query = """
            SELECT *
            FROM dbo.telemetry
            WHERE timestamp >= ? AND type = ? AND unit = ?
            ORDER BY (SELECT NULL)
            OFFSET ? ROWS FETCH FIRST ? ROWS ONLY;
        """
        try:
            rows = cursor.execute(
                query,
                (
                    query_timestamp,
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
                f"Error: {e} fetching batch from telemetry DB. Batches fetched={num_batches_fetched}",
                stack_info=True,
            )
            raise e
        try:
            entries = [
                TelemetryEntry(**dict(zip(column_names, row))) for row in rows
            ]
        except ValidationError as e:
            logger.error(
                f"Error: {e} validating telemetry DB rows. Batches fetched={num_batches_fetched}",
                stack_info=True,
            )
            raise e

        num_batches_fetched += 1
        row_count -= len(entries)
        logger.debug(
            f"Batch number={num_batches_fetched}, rows_in_batch={len(entries)}"
        )

        yield entries
