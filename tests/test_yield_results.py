import datetime

import pytest
from pydantic import ValidationError
from pytest_mock import MockerFixture

from growth_job_pipeline.yield_tsv_reader import get_ascending_yield_results


@pytest.fixture()
def data_lines(
    valid_yield_recorded_date__job1,
    valid_yield_recorded_date__job2,
    valid_crop,
    valid_yield_weight,
    valid_weight_unit,
) -> list[str]:
    """
    Returns data lines for mock_open
    :param valid_yield_recorded_date__job1:
    :param valid_yield_recorded_date__job2:
    :param valid_crop:
    :param valid_yield_weight:
    :param valid_weight_unit:
    :return: list[str]
    """

    return [
        "date  crop  weight  unit",
        (
            f"{valid_yield_recorded_date__job1}  {valid_crop.value} "
            f" {valid_yield_weight}  {valid_weight_unit.value}"
        ),
        (
            f"{valid_yield_recorded_date__job2}  {valid_crop.value} "
            f" {valid_yield_weight}  {valid_weight_unit.value}"
        ),
    ]


@pytest.fixture()
def invalid_data_line(
    valid_yield_recorded_date__job1,
    invalid_crop,
    valid_yield_weight,
    valid_weight_unit,
) -> str:
    """
    Returns a data line with invalid crop
    :param valid_yield_recorded_date__job1:
    :param invalid_crop:
    :param valid_yield_weight:
    :param valid_weight_unit:
    :return:
    """
    return (
        f"{valid_yield_recorded_date__job1}  {invalid_crop} "
        f" {valid_yield_weight}  {valid_weight_unit.value}"
    )


def test_get_yield_results__happy_path(
    mocker: MockerFixture,
    data_lines,
    valid_timestamp,
    valid_to_timestamp,
    yield_result__job1,
    yield_result__job2,
):
    mocker.patch(
        "growth_job_pipeline.yield_tsv_reader.yield_results.config",
        return_value="whatever.csv",
    )
    yield_results_file_mock = mocker.mock_open(read_data="\n".join(data_lines))
    mocker.patch("builtins.open", yield_results_file_mock)
    assert get_ascending_yield_results(
        from_timestamp=valid_timestamp, to_timestamp=valid_to_timestamp
    ) == [yield_result__job1, yield_result__job2]


def test_get_yield_results__invalid_data_raises(
    mocker: MockerFixture,
    data_lines,
    invalid_data_line,
    valid_timestamp,
    valid_to_timestamp,
    caplog,
):
    mocker.patch(
        "growth_job_pipeline.yield_tsv_reader.yield_results.config",
        return_value="whatever.csv",
    )
    yield_results_file_mock = mocker.mock_open(
        read_data="\n".join(data_lines + [invalid_data_line])
    )
    mocker.patch("builtins.open", yield_results_file_mock)
    with pytest.raises(ValidationError):
        get_ascending_yield_results(
            from_timestamp=valid_timestamp, to_timestamp=valid_to_timestamp
        )
    assert (
        "ERROR" in caplog.text
        and "Cannot validate yield results" in caplog.text
    )


def test_get_yield_results__io_error(
    mocker: MockerFixture,
    data_lines,
    invalid_data_line,
    valid_timestamp,
    valid_to_timestamp,
    caplog,
):
    mocker.patch(
        "growth_job_pipeline.yield_tsv_reader.yield_results.config",
        return_value="whatever.csv",
    )
    mock = mocker.patch("builtins.open")
    mock.side_effect = IOError()
    with pytest.raises(IOError):
        get_ascending_yield_results(
            from_timestamp=valid_timestamp, to_timestamp=valid_to_timestamp
        )
    assert (
        "ERROR" in caplog.text
        and "Cannot read from yield results file" in caplog.text
    )
