import pytest
from pydantic import ValidationError
from growth_job_pipeline.telemetry_db.models import TelemetryEntry


@pytest.mark.parametrize(
    "timestamp, type, value, unit",
    [
        ("2022-01-01", "temp", 12.9, "C"),
        ("2022-01-01T00:00:00", "temps", 12.9, "C"),
    ],
)
def test_invalid_telemetry_entries_raise(
    timestamp: str, type: str, value: float, unit: str
):
    with pytest.raises(ValidationError):
        TelemetryEntry(timestamp=timestamp, type=type, value=value, unit=unit)
