import pytest
from pydantic import ValidationError

from growth_job_pipeline.models.validators.coalesced_timestamps import (
    CoalescedTimestamps,
)


@pytest.mark.parametrize(
    "from_timestamp, to_timestamp",
    [
        ("invalid_timestamp__date_only", "valid_timestamp"),
        ("invalid_timestamp__wrong_month", "valid_timestamp"),
        ("valid_timestamp", "invalid_timestamp__date_only"),
        ("valid_timestamp", "invalid_timestamp__wrong_month"),
    ],
)
def test_invalid_coalesced_timestamps_raise(
    from_timestamp, to_timestamp, request: pytest.FixtureRequest
) -> None:
    """
    Tests that invalid coalesced timestamps raise ValidationError
    :param from_timestamp:
    :param to_timestamp:
    :param request:
    :return: None
    """
    with pytest.raises(ValidationError):
        from_timestamp = request.getfixturevalue(from_timestamp)
        to_timestamp = request.getfixturevalue(to_timestamp)
        CoalescedTimestamps(
            from_timestamp=from_timestamp, to_timestamp=to_timestamp
        )
