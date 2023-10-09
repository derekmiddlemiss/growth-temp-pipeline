import datetime

import pytest

from growth_job_pipeline.config import config
from growth_job_pipeline.main import (
    create_job_to_output_rows_spec,
    get_bounding_timestamps_for_specs,
    match_yield_results_growth_jobs_gen_specs,
)
from growth_job_pipeline.models.validators.coalesced_timestamps import (
    CoalescedTimestamps,
)
from growth_job_pipeline.models.validators.job_to_output_rows_spec import (
    JobToOutputRowsSpec,
)
from growth_job_pipeline.utils import latest_datetime_possible_for_date


def test_create_job_to_output_rows_spec(
    yield_result__job1, growth_job_1, job_to_output_rows_spec
) -> None:
    """
    Tests create_job_to_output_rows_spec
    :param yield_result__job1:
    :param growth_job_1:
    :param job_to_output_rows_spec:
    :return: None
    """

    assert (
        create_job_to_output_rows_spec(
            yield_result=yield_result__job1,
            growth_job=growth_job_1,
        )
        == job_to_output_rows_spec
    )


def test_get_bounding_timestamps_for_specs(
    job_to_output_rows_spec,
    job_to_output_rows_spec2,
    valid_start_date__job1,
    valid_end_date__job2,
) -> None:
    """
    Tests get_bounding_timestamps_for_specs
    :return: None
    """
    assert get_bounding_timestamps_for_specs(
        [job_to_output_rows_spec, job_to_output_rows_spec2]
    ) == CoalescedTimestamps(
        from_timestamp=valid_start_date__job1,
        to_timestamp=valid_end_date__job2,
    )


def test_match_yield_results_growth_jobs_gen_specs__happy_path(
    response_mock,
    json_str_valid,
    yield_result__job1,
    yield_result__job2,
    valid_timestamp,
    valid_to_timestamp,
    job_to_output_rows_spec,
    job_to_output_rows_spec2,
    mocker,
) -> None:
    """
    Tests match_yield_results_growth_jobs_gen_specs
    :param response_mock:
    :param json_str_valid:
    :param yield_result__job1:
    :param yield_result__job2:
    :param valid_timestamp:
    :param valid_to_timestamp:
    :param job_to_output_rows_spec:
    :param job_to_output_rows_spec2:
    :param mocker:
    :return: None
    """
    with response_mock(
        f"GET http://localhost:8080/jobs -> 200 :{json_str_valid}"
    ):
        assert match_yield_results_growth_jobs_gen_specs(
            all_yield_results_ascending=[
                yield_result__job1,
                yield_result__job2,
            ],
            from_timestamp=valid_timestamp,
            to_timestamp=valid_to_timestamp,
        ) == [job_to_output_rows_spec2, job_to_output_rows_spec]


def test_match_yield_results_growth_jobs_gen_specs_raises__unambiguous_match(
    response_mock,
    json_str_valid,
    yield_result__trouble_early,
    yield_result__job2,
    valid_timestamp,
    valid_to_timestamp,
    caplog,
) -> None:
    """
    Tests match_yield_results_growth_jobs_gen_specs
    :param response_mock:
    :param json_str_valid:
    :param yield_result__trouble_early:
    :param yield_result__job2:
    :param valid_timestamp:
    :param valid_to_timestamp:
    :return: None
    """
    with response_mock(
        f"GET http://localhost:8080/jobs -> 200 :{json_str_valid}"
    ):
        with pytest.raises(RuntimeError):
            match_yield_results_growth_jobs_gen_specs(
                all_yield_results_ascending=[
                    yield_result__trouble_early,
                    yield_result__job2,
                ],
                from_timestamp=valid_timestamp,
                to_timestamp=valid_to_timestamp,
            )
        assert "ERROR" in caplog.text
        assert "Cannot unambiguously assign yield result" in caplog.text
        assert "Num growth jobs found since last yield result=2" in caplog.text


def test_match_yield_results_growth_jobs_gen_specs_raises__no_growth_jobs(
    response_mock,
    json_str_valid,
    yield_result__trouble_early,
    yield_result__job2,
    valid_timestamp,
    valid_to_timestamp,
    caplog,
) -> None:
    """
    Tests match_yield_results_growth_jobs_gen_specs
    :param response_mock:
    :param json_str_valid:
    :param yield_result__trouble_early:
    :param yield_result__job2:
    :param valid_timestamp:
    :param valid_to_timestamp:
    :return: None
    """
    with response_mock("GET http://localhost:8080/jobs -> 200 :[]"):
        with pytest.raises(RuntimeError):
            match_yield_results_growth_jobs_gen_specs(
                all_yield_results_ascending=[
                    yield_result__trouble_early,
                    yield_result__job2,
                ],
                from_timestamp=valid_timestamp,
                to_timestamp=valid_to_timestamp,
            )
        assert "ERROR" in caplog.text
        assert "Cannot unambiguously assign yield result" in caplog.text
        assert "Num growth jobs found since last yield result=0" in caplog.text


def test_match_yield_results_growth_jobs_gen_specs_raises__late_yield_result(
    response_mock,
    json_str_valid,
    yield_result__trouble_late,
    job_to_output_rows_spec,
    job_to_output_rows_spec2,
    caplog,
) -> None:
    """
    Tests match_yield_results_growth_jobs_gen_specs
    :param response_mock:
    :param json_str_valid:
    :param yield_result__trouble_late:
    :param job_to_output_rows_spec:
    :param job_to_output_rows_spec2:
    :param mocker:
    :return: None
    """
    with response_mock("GET http://localhost:8080/jobs -> 200 :[]"):
        with pytest.raises(RuntimeError):
            match_yield_results_growth_jobs_gen_specs(
                all_yield_results_ascending=[yield_result__trouble_late],
                from_timestamp=latest_datetime_possible_for_date(
                    yield_result__trouble_late.date
                )
                - datetime.timedelta(days=10),
                to_timestamp=latest_datetime_possible_for_date(
                    yield_result__trouble_late.date
                )
                + datetime.timedelta(days=10),
            )
        assert "ERROR" in caplog.text
        assert "Cannot unambiguously assign yield result" in caplog.text
        assert (
            "Num growth jobs found search back minus"
            f" {config('MAX_DAYS_DELAY_GROWTH_JOB_YIELD_RESULT')} days=0"
            in caplog.text
        )
