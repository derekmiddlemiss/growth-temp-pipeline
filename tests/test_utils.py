import datetime

import pytest

from growth_job_pipeline.models.validators.coalesced_timestamps import (
    CoalescedTimestamps,
)
from growth_job_pipeline.models.validators.config_timestamps import (
    ConfigTimestamps,
)
from growth_job_pipeline.utils import (
    get_config_timestamps,
    coalesce_run_timestamps,
    split_line_on_whitespace,
    latest_datetime_possible_for_date,
)


def test_get_config_timestamps__defined(mocker):
    mocker.patch(
        "growth_job_pipeline.utils.utils.config",
        return_value="2021-01-01T00:00:00",
    )
    config_timestamps = get_config_timestamps()
    assert isinstance(config_timestamps, ConfigTimestamps)
    assert config_timestamps.from_timestamp == datetime.datetime(
        2021, 1, 1, 0, 0
    )
    assert config_timestamps.to_timestamp == datetime.datetime(
        2021, 1, 1, 0, 0
    )


def test_get_config_timestamps__undefined(mocker):
    mocker.patch(
        "growth_job_pipeline.utils.utils.config",
        return_value=None,
    )
    config_timestamps = get_config_timestamps()
    assert isinstance(config_timestamps, ConfigTimestamps)
    assert config_timestamps.from_timestamp is None
    assert config_timestamps.to_timestamp is None


def test_get_config_timestamps__invalid(mocker):
    mocker.patch(
        "growth_job_pipeline.utils.utils.config",
        return_value="invalid",
    )
    with pytest.raises(ValueError):
        get_config_timestamps()


def test_get_config_timestamps__from_null_to_defined(mocker):
    mocker.patch(
        "growth_job_pipeline.utils.utils.config",
        side_effect=[None, "2021-01-01T00:00:00"],
    )
    config_timestamps = get_config_timestamps()
    assert isinstance(config_timestamps, ConfigTimestamps)
    assert config_timestamps.from_timestamp is None
    assert config_timestamps.to_timestamp == datetime.datetime(
        2021, 1, 1, 0, 0
    )


def test_coalesce_run_timestamps__from_timestamp_defined_to_timestamp_defined():
    config_timestamps = ConfigTimestamps(
        from_timestamp=datetime.datetime(2021, 1, 1, 0, 0),
        to_timestamp=datetime.datetime(2021, 1, 2, 0, 0),
    )
    coalesced_timestamps = coalesce_run_timestamps(config_timestamps)
    assert isinstance(coalesced_timestamps, CoalescedTimestamps)
    assert coalesced_timestamps.from_timestamp == datetime.datetime(
        2021, 1, 1, 0, 0
    )
    assert coalesced_timestamps.to_timestamp == datetime.datetime(
        2021, 1, 2, 0, 0
    )


def test_coalesce_run_timestamps__from_timestamp_defined_to_timestamp_undefined():
    config_timestamps = ConfigTimestamps(
        from_timestamp=datetime.datetime(2021, 1, 1, 0, 0),
        to_timestamp=None,
    )
    coalesced_timestamps = coalesce_run_timestamps(config_timestamps)
    assert isinstance(coalesced_timestamps, CoalescedTimestamps)
    assert coalesced_timestamps.from_timestamp == datetime.datetime(
        2021, 1, 1, 0, 0
    )
    assert coalesced_timestamps.to_timestamp == datetime.datetime.max


def test_coalesce_run_timestamps__from_timestamp_undefined_to_timestamp_undefined():
    config_timestamps = ConfigTimestamps(
        from_timestamp=None,
        to_timestamp=None,
    )
    coalesced_timestamps = coalesce_run_timestamps(config_timestamps)
    assert isinstance(coalesced_timestamps, CoalescedTimestamps)
    assert coalesced_timestamps.from_timestamp == datetime.datetime.min
    assert coalesced_timestamps.to_timestamp == datetime.datetime.max


def test_coalesce_run_timestamps__to_timestamp_less_than_from_timestamp_raises(
    caplog: pytest.LogCaptureFixture,
):
    config_timestamps = ConfigTimestamps(
        from_timestamp=datetime.datetime(2021, 1, 2, 0, 0),
        to_timestamp=datetime.datetime(2021, 1, 1, 0, 0),
    )
    with pytest.raises(ValueError):
        coalesce_run_timestamps(config_timestamps)

    assert (
        caplog.records[0].levelname == "ERROR"
        and caplog.records[0].message
        == "to_timestamp=2021-01-01 00:00:00 less than or equal to"
        " from_timestamp=2021-01-02 00:00:00"
    )


def test_split_line_on_whitespace():
    line = "a b c"
    assert split_line_on_whitespace(line) == ["a", "b", "c"]


def test_split_line_on_whitespace__empty():
    line = ""
    assert split_line_on_whitespace(line) == []


def test_split_line_on_whitespace__removes_carriage_returns():
    line = "a b c\r"
    assert split_line_on_whitespace(line) == ["a", "b", "c"]


def test_latest_datetime_possible_for_date():
    assert latest_datetime_possible_for_date(
        datetime.date(2021, 1, 1)
    ) == datetime.datetime(2021, 1, 1, 23, 59, 59, 999999)
