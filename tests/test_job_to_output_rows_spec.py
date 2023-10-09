import pytest
from pydantic import ValidationError

from growth_job_pipeline.models.validators.config_timestamps import (
    ConfigTimestamps,
)
from growth_job_pipeline.models.validators.job_to_output_rows_spec import (
    JobToOutputRowsSpec,
)


@pytest.mark.parametrize(
    (
        "crop, growth_job_start_date, growth_job_end_date, growth_job_id,"
        " yield_recorded_date, yield_weight, yield_unit"
    ),
    [
        (
            "invalid_crop",
            "valid_start_date__job1",
            "valid_end_date__job1",
            "valid_growth_job_id",
            "valid_yield_recorded_date__job1",
            "valid_yield_weight",
            "valid_weight_unit",
        ),
        (
            "valid_crop",
            "invalid_timestamp__date_only",
            "valid_to_timestamp",
            "valid_growth_job_id",
            "valid_yield_recorded_date__job1",
            "valid_yield_weight",
            "valid_weight_unit",
        ),
        (
            "valid_crop",
            "valid_start_date__job1",
            "invalid_timestamp__date_only",
            "valid_growth_job_id",
            "valid_yield_recorded_date__job1",
            "valid_yield_weight",
            "valid_weight_unit",
        ),
        (
            "valid_crop",
            "valid_start_date__job1",
            "invalid_end_date__job1_early",
            "valid_growth_job_id",
            "valid_yield_recorded_date__job1",
            "valid_yield_weight",
            "valid_weight_unit",
        ),
        (
            "valid_crop",
            "valid_start_date__job1",
            "valid_end_date__job1",
            "invalid_growth_job_id",
            "valid_yield_recorded_date__job1",
            "valid_yield_weight",
            "valid_weight_unit",
        ),
        (
            "valid_crop",
            "valid_start_date__job1",
            "valid_end_date__job1",
            "valid_growth_job_id",
            "invalid_yield_recorded_date__month_wrong",
            "valid_yield_weight",
            "valid_weight_unit",
        ),
        (
            "valid_crop",
            "valid_start_date__job1",
            "valid_end_date__job1",
            "valid_growth_job_id",
            "invalid_yield_recorded_date__early",
            "valid_yield_weight",
            "valid_weight_unit",
        ),
        (
            "valid_crop",
            "valid_start_date__job1",
            "valid_end_date__job1",
            "valid_growth_job_id",
            "valid_yield_recorded_date__job1",
            "invalid_yield_weight",
            "valid_weight_unit",
        ),
        (
            "valid_crop",
            "valid_start_date__job1",
            "valid_end_date__job1",
            "valid_growth_job_id",
            "valid_yield_recorded_date__job1",
            "valid_yield_weight",
            "invalid_weight_unit",
        ),
    ],
)
def test_invalid_job_to_output_rows_specs_raise(
    crop,
    growth_job_start_date,
    growth_job_end_date,
    growth_job_id,
    yield_recorded_date,
    yield_weight,
    yield_unit,
    request: pytest.FixtureRequest,
) -> None:
    """
    Tests that invalid config timestamps raise ValidationError
    :param from_timestamp:
    :param to_timestamp:
    :param request:
    :return: None
    """
    with pytest.raises(ValidationError):
        crop = request.getfixturevalue(crop)
        growth_job_start_date = request.getfixturevalue(growth_job_start_date)
        growth_job_end_date = request.getfixturevalue(growth_job_end_date)
        growth_job_id = request.getfixturevalue(growth_job_id)
        yield_recorded_date = request.getfixturevalue(yield_recorded_date)
        yield_weight = request.getfixturevalue(yield_weight)
        yield_unit = request.getfixturevalue(yield_unit)
        JobToOutputRowsSpec(
            crop=crop,
            growth_job_start_date=growth_job_start_date,
            growth_job_end_date=growth_job_end_date,
            growth_job_id=growth_job_id,
            yield_recorded_date=yield_recorded_date,
            yield_weight=yield_weight,
            yield_unit=yield_unit,
        )
