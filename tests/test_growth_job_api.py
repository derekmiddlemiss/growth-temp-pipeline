import json

import pytest
import requests
from pydantic import ValidationError

from growth_job_pipeline.growth_job_api import (
    get_time_filtered_growth_jobs_for_crop,
)


@pytest.fixture()
def json_str_valid(
    valid_crop,
    valid_start_date__job1,
    valid_end_date__job1,
    valid_start_date__job2,
    valid_end_date__job2,
) -> str:
    """
    Returns a json string
    :return: str
    """
    return json.dumps(
        [
            {
                "id": 1,
                "crop": valid_crop,
                "start_date": valid_start_date__job1.isoformat(),
                "end_date": valid_end_date__job1.isoformat(),
            },
            {
                "id": 2,
                "crop": valid_crop,
                "start_date": valid_start_date__job2.isoformat(),
                "end_date": valid_end_date__job2.isoformat(),
            },
        ]
    )


@pytest.fixture()
def json_str_invalid(
    invalid_crop, valid_start_date__job1, valid_end_date__job1
) -> str:
    """
    Returns a json string with an invalid crop
    """
    return json.dumps(
        [
            {
                "id": 1,
                "crop": invalid_crop,
                "start_date": valid_start_date__job1.isoformat(),
                "end_date": valid_end_date__job1.isoformat(),
            }
        ]
    )


def test_get_time_filtered_growth_jobs_for_crop(
    response_mock,
    valid_timestamp,
    valid_to_timestamp,
    valid_crop,
    json_str_valid,
    growth_job_1,
    growth_job_2,
):
    """
    Tests get_time_filtered_growth_jobs_for_crop
    """
    with response_mock(
        f"GET http://localhost:8080/jobs -> 200 :{json_str_valid}"
    ):
        assert get_time_filtered_growth_jobs_for_crop(
            from_timestamp=valid_timestamp,
            to_timestamp=valid_to_timestamp,
            crop=valid_crop,
        ) == [growth_job_1, growth_job_2]


def test_get_time_filtered_growth_jobs_for_crop__no_crop_results(
    response_mock,
    valid_timestamp,
    valid_to_timestamp,
    valid_crop2,
    json_str_valid,
    growth_job_1,
    growth_job_2,
):
    """
    Tests get_time_filtered_growth_jobs_for_crop
    """
    with response_mock(
        f"GET http://localhost:8080/jobs -> 200 :{json_str_valid}"
    ):
        assert (
            get_time_filtered_growth_jobs_for_crop(
                from_timestamp=valid_timestamp,
                to_timestamp=valid_to_timestamp,
                crop=valid_crop2,
            )
            == []
        )


def test_get_time_filtered_growth_jobs_for_crop__late_from_to(
    response_mock,
    valid_timestamp__very_late,
    valid_to_timestamp__very_late,
    valid_crop,
    json_str_valid,
    growth_job_1,
    growth_job_2,
):
    """
    Tests get_time_filtered_growth_jobs_for_crop
    """
    with response_mock(
        f"GET http://localhost:8080/jobs -> 200 :{json_str_valid}"
    ):
        assert (
            get_time_filtered_growth_jobs_for_crop(
                from_timestamp=valid_timestamp__very_late,
                to_timestamp=valid_to_timestamp__very_late,
                crop=valid_crop,
            )
            == []
        )


def test_get_time_filtered_growth_jobs_for_crop__request_error_raises_logged(
    response_mock,
    valid_timestamp,
    valid_to_timestamp,
    valid_crop,
    caplog,
):
    """
    Tests get_time_filtered_growth_jobs_for_crop
    """
    with response_mock("GET http://localhost:8080/jobs -> 400"):
        with pytest.raises(requests.RequestException):
            get_time_filtered_growth_jobs_for_crop(
                from_timestamp=valid_timestamp,
                to_timestamp=valid_to_timestamp,
                crop=valid_crop,
            )
    assert "ERROR" in caplog.text


def test_get_time_filtered_growth_jobs_for_crop__invalid_json_raises_logged(
    response_mock,
    valid_timestamp,
    valid_to_timestamp,
    valid_crop,
    json_str_invalid,
    caplog,
):
    """
    Tests get_time_filtered_growth_jobs_for_crop
    """
    with response_mock(
        f"GET http://localhost:8080/jobs -> 200 :{json_str_invalid}"
    ):
        with pytest.raises(ValidationError):
            get_time_filtered_growth_jobs_for_crop(
                from_timestamp=valid_timestamp,
                to_timestamp=valid_to_timestamp,
                crop=valid_crop,
            )
    assert "ERROR" in caplog.text
