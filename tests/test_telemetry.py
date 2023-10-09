from __future__ import annotations

import pyodbc
import pytest
from typing import TYPE_CHECKING
from pydantic import ValidationError
from growth_job_pipeline.models.validators.telemetry_entry import (
    TelemetryEntry,
)
from growth_job_pipeline.telemetry_db.db import (
    get_row_count,
    get_validated_entries,
    telemetry_entries_batcher,
)

if TYPE_CHECKING:
    from pytest_mock.plugin import MockerFixture


def test_valid_telemetry_entry_instantiates(
    valid_timestamp,
    valid_measurement_type,
    valid_measurement_value,
    valid_measurement_unit,
) -> None:
    """
    Tests that valid telemetry entry instantiates
    :param valid_timestamp: datetime.datetime
    :param valid_measurement_type: MeasurementType
    :param valid_measurement_value: float
    :param valid_measurement_unit: MeasurementUnit
    :return: None
    """
    te = TelemetryEntry(
        timestamp=valid_timestamp,
        type=valid_measurement_type,
        value=valid_measurement_value,
        unit=valid_measurement_unit,
    )
    assert te.timestamp == valid_timestamp
    assert te.type == valid_measurement_type
    assert te.value == valid_measurement_value
    assert te.unit == valid_measurement_unit


def test_get_db_row_count(
    mocker: MockerFixture,
    valid_timestamp,
    valid_to_timestamp,
    valid_measurement_type,
    valid_measurement_unit,
) -> None:
    """
    Tests that get_row_count returns the correct row count
    :param mocker: MockerFixture
    :param valid_timestamp: datetime.datetime
    :param valid_to_timestamp: datetime.datetime
    :param valid_measurement_type: MeasurementType
    :param valid_measurement_unit: MeasurementUnit
    :return: None
    """
    cursor = mocker.MagicMock(spec=pyodbc.Cursor)
    cursor.execute().fetchone.return_value = (5,)
    assert (
        get_row_count(
            cursor=cursor,
            from_timestamp=valid_timestamp,
            to_timestamp=valid_to_timestamp,
            type_to_fetch=valid_measurement_type,
            unit_to_fetch=valid_measurement_unit,
        )
        == 5
    )
    query = """
        SELECT COUNT(*)
        FROM dbo.telemetry
        WHERE timestamp >= ? AND timestamp <= ? AND type = ? AND unit = ?
    """
    cursor.execute.assert_called_with(
        query,
        (
            valid_timestamp,
            valid_to_timestamp,
            valid_measurement_type,
            valid_measurement_unit,
        ),
    )


def test_get_db_row_error_raised_and_logged(
    mocker: MockerFixture,
    valid_timestamp,
    valid_to_timestamp,
    valid_measurement_type,
    valid_measurement_unit,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Tests that get_row_count raises and logs an error if pyodbc.Error is raised
    :param mocker: MockerFixture
    :param valid_timestamp: datetime.datetime
    :param valid_to_timestamp: datetime.datetime
    :param valid_measurement_type: MeasurementType
    :param valid_measurement_unit: MeasurementUnit
    :param caplog: LogCaptureFixture
    :return: None
    """
    cursor = mocker.MagicMock(spec=pyodbc.Cursor)
    cursor.execute = mocker.MagicMock(side_effect=pyodbc.Error)
    with pytest.raises(pyodbc.Error):
        get_row_count(
            cursor=cursor,
            from_timestamp=valid_timestamp,
            to_timestamp=valid_to_timestamp,
            type_to_fetch=valid_measurement_type,
            unit_to_fetch=valid_measurement_unit,
        )
    assert (
        "ERROR" in caplog.text
        and "Could not fetch row count from telemetry DB" in caplog.text
    )


def test_get_validated_entries(
    valid_timestamp,
    valid_timestamp__later,
    valid_measurement_type,
    valid_measurement_value,
    valid_measurement_unit,
) -> None:
    """
    Tests that get_validated_entries returns a list of TelemetryEntry
    :param valid_timestamp: datetime.datetime
    :param valid_timestamp__later: datetime.datetime
    :param valid_measurement_type: MeasurementType
    :param valid_measurement_value: float
    :param valid_measurement_unit: MeasurementUnit
    :return: None
    """
    column_names = ["timestamp", "type", "value", "unit"]
    row1 = (
        valid_timestamp,
        valid_measurement_type,
        valid_measurement_value,
        valid_measurement_unit,
    )
    row2 = (
        valid_timestamp__later,
        valid_measurement_type,
        valid_measurement_value,
        valid_measurement_unit,
    )
    entry1 = TelemetryEntry(**dict(zip(column_names, row1)))
    entry2 = TelemetryEntry(**dict(zip(column_names, row2)))
    rows = [row1, row2]
    assert get_validated_entries(
        column_names=column_names, rows=rows, num_batches_fetched=1
    ) == [entry1, entry2]


def test_get_validated_entries_raises_and_logs(
    invalid_timestamp__date_only,
    valid_measurement_type,
    valid_measurement_value,
    valid_measurement_unit,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Tests that get_validated_entries raises and logs an error if ValidationError is raised
    :param caplog: LogCaptureFixture
    :return: None
    """
    column_names = ["timestamp", "type", "value", "unit"]
    rows = [
        (
            invalid_timestamp__date_only,
            valid_measurement_type,
            valid_measurement_value,
            valid_measurement_unit,
        )
    ]
    with pytest.raises(ValidationError):
        get_validated_entries(
            column_names=column_names, rows=rows, num_batches_fetched=1
        )
    assert (
        "ERROR" in caplog.text
        and "Could not validate telemetry DB rows. Batches fetched=1"
        in caplog.text
    )


def test_telemetry_entries_batcher(
    mocker: MockerFixture,
    valid_timestamp,
    valid_timestamp__later,
    valid_to_timestamp,
    valid_measurement_type,
    valid_measurement_value,
    valid_measurement_unit,
    telemetry_entry,
    telemetry_entry__later,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Tests that telemetry_entries_batcher returns a list of TelemetryEntry
    :param mocker: MockerFixture
    :param valid_timestamp: datetime.datetime
    :param valid_timestamp__later: datetime.datetime
    :param valid_to_timestamp: datetime.datetime
    :param valid_measurement_type: MeasurementType
    :param valid_measurement_value: float
    :param valid_measurement_unit: MeasurementUnit
    :param telemetry_entry: TelemetryEntry
    :param telemetry_entry__later: TelemetryEntry
    :param caplog: LogCaptureFixture
    :return: None
    """
    caplog.set_level("INFO")
    mocker.patch(
        "growth_job_pipeline.telemetry_db.db.get_row_count",
        return_value=2,
    )
    cursor = mocker.MagicMock(spec=pyodbc.Cursor)
    cursor.description = [("timestamp",), ("type",), ("value",), ("unit",)]
    cursor.execute().fetchall.return_value = [
        (
            valid_timestamp,
            valid_measurement_type,
            valid_measurement_value,
            valid_measurement_unit,
        ),
        (
            valid_timestamp__later,
            valid_measurement_type,
            valid_measurement_value,
            valid_measurement_unit,
        ),
    ]
    batcher = telemetry_entries_batcher(
        cursor=cursor,
        type_to_fetch=valid_measurement_type,
        unit_to_fetch=valid_measurement_unit,
        from_timestamp=valid_timestamp,
        to_timestamp=valid_to_timestamp,
        batch_size=1000,
    )
    assert next(batcher) == [telemetry_entry, telemetry_entry__later]
    assert "2 rows to fetch" in caplog.text


def test_telemetry_entries_batcher_raises_and_logs(
    mocker: MockerFixture,
    valid_timestamp,
    valid_to_timestamp,
    valid_measurement_type,
    valid_measurement_unit,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Tests that telemetry_entries_batcher raises and logs an error if pyodbc.Error is raised
    :param mocker: MockerFixture
    :param valid_timestamp: datetime.datetime
    :param valid_to_timestamp: datetime.datetime
    :param valid_measurement_type: MeasurementType
    :param valid_measurement_unit: MeasurementUnit
    :param caplog: LogCaptureFixture
    :return: None
    """
    mocker.patch(
        "growth_job_pipeline.telemetry_db.db.get_row_count",
        return_value=2,
    )
    cursor = mocker.MagicMock(spec=pyodbc.Cursor, side_effect=pyodbc.Error)
    cursor.description = [("timestamp",), ("type",), ("value",), ("unit",)]
    cursor.execute = mocker.MagicMock(side_effect=pyodbc.Error)
    with pytest.raises(pyodbc.Error):
        batcher = telemetry_entries_batcher(
            cursor=cursor,
            type_to_fetch=valid_measurement_type,
            unit_to_fetch=valid_measurement_unit,
            from_timestamp=valid_timestamp,
            to_timestamp=valid_to_timestamp,
            batch_size=1000,
        )
        next(batcher)
    assert (
        "ERROR" in caplog.text
        and "Could not fetch batch from telemetry DB. Batches fetched=0"
        in caplog.text
    )
