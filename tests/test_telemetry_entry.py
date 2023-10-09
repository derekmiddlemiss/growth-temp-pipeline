import pytest
from pydantic import ValidationError

from growth_job_pipeline.models.validators.telemetry_entry import (
    TelemetryEntry,
)


@pytest.mark.parametrize(
    "timestamp, _type, value, unit",
    [
        (
            "invalid_timestamp__date_only",
            "valid_measurement_type",
            "valid_measurement_value",
            "valid_measurement_unit",
        ),
        (
            "invalid_timestamp__wrong_month",
            "valid_measurement_type",
            "valid_measurement_value",
            "valid_measurement_unit",
        ),
        (
            "valid_timestamp",
            "invalid_measurement_type",
            "valid_measurement_value",
            "valid_measurement_unit",
        ),
        (
            "valid_timestamp",
            "valid_measurement_type",
            "invalid_measurement_value",
            "invalid_measurement_unit",
        ),
        (
            "valid_timestamp",
            "valid_measurement_type",
            "valid_measurement_value",
            "invalid_measurement_unit",
        ),
    ],
)
def test_invalid_telemetry_entries_raise(
    timestamp: str,
    _type: str,
    value: str,
    unit: str,
    request: pytest.FixtureRequest,
) -> None:
    """
    Tests that invalid telemetry entries raise ValidationError
    :param timestamp: str
    :param _type: str
    :param value: str
    :param unit: str
    :param request: pytest.FixtureRequest
    :return: None
    """
    with pytest.raises(ValidationError):
        timestamp = request.getfixturevalue(timestamp)
        _type = request.getfixturevalue(_type)
        value = request.getfixturevalue(value)
        unit = request.getfixturevalue(unit)
        TelemetryEntry(timestamp=timestamp, type=_type, value=value, unit=unit)
