import pytest
import datetime

from growth_job_pipeline.models.enums.telemetry_measurement_unit import (
    TelemetryMeasurementUnit,
)
from growth_job_pipeline.models.enums.telemetry_measurement_type import (
    TelemetryMeasurementType,
)


@pytest.fixture()
def valid_timestamp() -> datetime.datetime:
    """
    Returns a valid timestamp
    :return: datetime.datetime
    """
    return datetime.datetime.fromisoformat("2022-01-01T00:00:00")


@pytest.fixture()
def valid_timestamp__later() -> datetime.datetime:
    """
    Returns a valid timestamp - later
    :return: datetime.datetime
    """
    return datetime.datetime.fromisoformat("2022-01-01T00:00:30")


@pytest.fixture()
def invalid_timestamp__date_only() -> str:
    """
    Returns an invalid timestamp - date only
    :return: str
    """
    return "2022-01-01"


@pytest.fixture()
def invalid_timestamp__wrong_month() -> str:
    """
    Returns a from_timestamp with invalid month
    :return: str
    """
    return "2022-13-01T00:00:00"


@pytest.fixture()
def valid_to_timestamp() -> datetime.datetime:
    """
    Returns a valid to_timestamp, after all valid timestamps
    :return: datetime.datetime
    """
    return datetime.datetime.fromisoformat("2022-01-02T00:00:00")


@pytest.fixture()
def invalid_to_timestamp__early() -> datetime.datetime:
    """
    Returns an invalid to_timestamp, before all valid timestamps
    :return: datetime.datetime
    """
    return datetime.datetime.fromisoformat("2020-01-02T00:00:00")


@pytest.fixture()
def valid_measurement_type() -> TelemetryMeasurementType:
    """
    Returns a valid measurement type
    :return: str
    """
    return TelemetryMeasurementType("temp")


@pytest.fixture()
def invalid_measurement_type() -> str:
    """
    Returns an invalid measurement type
    :return: str
    """
    return "invalid_measurement_type"


@pytest.fixture()
def valid_measurement_unit() -> TelemetryMeasurementUnit:
    """
    Returns a valid measurement unit
    :return: str
    """
    return TelemetryMeasurementUnit("C")


@pytest.fixture()
def invalid_measurement_unit() -> str:
    """
    Returns an invalid measurement unit
    :return: str
    """
    return "invalid_measurement_unit"


@pytest.fixture()
def valid_measurement_value() -> float:
    """
    Returns a valid measurement value
    :return: float
    """
    return 12.9


@pytest.fixture()
def invalid_measurement_value() -> str:
    """
    Returns a valid measurement value
    :return: str
    """
    return "12.9.4"
