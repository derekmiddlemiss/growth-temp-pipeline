import datetime
import json

import pytest

from growth_job_pipeline.models.enums.crop import Crop
from growth_job_pipeline.models.enums.telemetry_measurement_type import (
    TelemetryMeasurementType,
)
from growth_job_pipeline.models.enums.telemetry_measurement_unit import (
    TelemetryMeasurementUnit,
)
from growth_job_pipeline.models.enums.weight_unit import WeightUnit
from growth_job_pipeline.models.validators.growth_job import GrowthJob
from growth_job_pipeline.models.validators.job_to_output_rows_spec import (
    JobToOutputRowsSpec,
)
from growth_job_pipeline.models.validators.telemetry_entry import (
    TelemetryEntry,
)
from growth_job_pipeline.models.validators.yield_result import YieldResult


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
def valid_timestamp__very_late() -> datetime.datetime:
    """
    Returns a valid timestamp, should make most entities out-of-scope
    :return: datetime.datetime
    """
    return datetime.datetime.fromisoformat("2050-01-01T00:00:00")


@pytest.fixture()
def valid_to_timestamp__very_late() -> datetime.datetime:
    """
    Returns a valid timestamp, should make most entities out-of-scope
    :return: datetime.datetime
    """
    return datetime.datetime.fromisoformat("2050-01-02T00:00:00")


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
    return datetime.datetime.fromisoformat("2022-02-01T00:00:00")


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


@pytest.fixture()
def valid_yield_weight() -> float:
    """
    Returns a valid yield weight
    :return: float
    """
    return 1.0


@pytest.fixture()
def invalid_yield_weight() -> str:
    """
    Returns a valid yield weight
    :return: float
    """
    return "1.1.1"


@pytest.fixture()
def valid_weight_unit() -> WeightUnit:
    """
    Returns a valid weight unit
    :return: WeightUnit
    """
    return WeightUnit("kg")


@pytest.fixture()
def invalid_weight_unit() -> str:
    """
    Returns an invalid weight unit
    :return: str
    """
    return "invalid_weight_unit"


@pytest.fixture()
def telemetry_entry(
    valid_timestamp,
    valid_measurement_type,
    valid_measurement_value,
    valid_measurement_unit,
) -> TelemetryEntry:
    """
    Returns a valid telemetry entry
    :return: TelemetryEntry
    """
    return TelemetryEntry(
        timestamp=valid_timestamp,
        type=valid_measurement_type,
        value=valid_measurement_value,
        unit=valid_measurement_unit,
    )


@pytest.fixture()
def telemetry_entry__later(
    valid_timestamp__later,
    valid_measurement_type,
    valid_measurement_value,
    valid_measurement_unit,
) -> TelemetryEntry:
    """
    Returns a valid telemetry entry for later timestamp
    :return: TelemetryEntry
    """
    return TelemetryEntry(
        timestamp=valid_timestamp__later,
        type=valid_measurement_type,
        value=valid_measurement_value,
        unit=valid_measurement_unit,
    )


@pytest.fixture()
def valid_crop() -> Crop:
    """
    Returns a valid crop
    :return: str
    """
    return Crop("basil")


@pytest.fixture()
def valid_crop2() -> Crop:
    """
    Returns a valid crop
    :return: str
    """
    return Crop("potato")


@pytest.fixture()
def invalid_crop() -> str:
    """
    Returns an invalid crop
    :return: str
    """
    return "spaghetti"


@pytest.fixture()
def valid_start_date__job1() -> datetime.datetime:
    """
    Returns a valid start_date after all valid timestamps
    :return: datetime.datetime
    """
    return datetime.datetime.fromisoformat("2022-01-05T00:00:00")


@pytest.fixture()
def valid_end_date__job1() -> datetime.datetime:
    """
    Returns a valid end_date before all valid to timestamps
    :return: datetime.datetime
    """
    return datetime.datetime.fromisoformat("2022-01-10T00:00:00")


@pytest.fixture()
def invalid_end_date__job1_early() -> datetime.datetime:
    """
    Returns an invalid end_date before start_date
    :return: datetime.datetime
    """
    return datetime.datetime.fromisoformat("2022-01-01T00:00:00")


@pytest.fixture()
def valid_start_date__job2() -> datetime.datetime:
    """
    Returns a valid start_date after all valid timestamps
    :return: datetime.datetime
    """
    return datetime.datetime.fromisoformat("2022-01-11T00:00:00")


@pytest.fixture()
def valid_end_date__job2() -> datetime.datetime:
    """
    Returns a valid end_date before all valid to timestamps
    :return: datetime.datetime
    """
    return datetime.datetime.fromisoformat("2022-01-15T00:00:00")


@pytest.fixture()
def valid_growth_job_id() -> int:
    """
    Returns a valid growth_job_id
    :return: int
    """
    return 1


@pytest.fixture()
def valid_growth_job_id2() -> int:
    """
    Returns a valid growth_job_id
    :return: int
    """
    return 2


@pytest.fixture()
def growth_job_1(
    valid_growth_job_id,
    valid_crop,
    valid_start_date__job1,
    valid_end_date__job1,
) -> GrowthJob:
    """
    Returns a valid growth job
    :return: GrowthJob
    """
    return GrowthJob(
        id=valid_growth_job_id,
        crop=valid_crop,
        start_date=valid_start_date__job1,
        end_date=valid_end_date__job1,
    )


@pytest.fixture()
def growth_job_2(
    valid_growth_job_id2,
    valid_crop,
    valid_start_date__job2,
    valid_end_date__job2,
) -> GrowthJob:
    """
    Returns a valid growth job
    :return: GrowthJob
    """
    return GrowthJob(
        id=valid_growth_job_id2,
        crop=valid_crop,
        start_date=valid_start_date__job2,
        end_date=valid_end_date__job2,
    )


@pytest.fixture()
def valid_yield_recorded_date__job1() -> datetime.date:
    """
    Returns a valid yield_recorded_date
    After valid growth_job_end_dates
    :return: datetime.date
    """
    return datetime.date.fromisoformat("2022-01-12")


@pytest.fixture()
def valid_yield_recorded_date__job2() -> datetime.date:
    """
    Returns a valid yield_recorded_date
    After valid growth_job_end_dates
    :return: datetime.date
    """
    return datetime.date.fromisoformat("2022-01-17")


@pytest.fixture()
def invalid_yield_recorded_date__early() -> datetime.date:
    """
    Returns an invalid yield_recorded_date
    Before all valid growth_job_end_dates
    :return: datetime.date
    """
    return datetime.date.fromisoformat("2021-01-01")


@pytest.fixture()
def invalid_yield_recorded_date__very_late() -> datetime.date:
    """
    Returns an invalid yield_recorded_date
    Out of scope
    :return: datetime.date
    """
    return datetime.date.fromisoformat("2025-01-01")


@pytest.fixture()
def invalid_yield_recorded_date__month_wrong() -> str:
    """
    Returns an invalid yield_recorded_date
    Before all valid growth_job_end_dates
    :return: str
    """
    return "2021-13-01"


@pytest.fixture()
def invalid_growth_job_id() -> str:
    """
    Returns an invalid growth_job_id
    :return: int
    """
    return "one-ish"


@pytest.fixture()
def yield_result__job1(
    valid_yield_recorded_date__job1,
    valid_crop,
    valid_yield_weight,
    valid_weight_unit,
) -> YieldResult:
    """
    Returns a valid yield result
    :param valid_yield_recorded_date__job1:
    :param valid_crop:
    :param valid_yield_weight:
    :param valid_weight_unit:
    :return: YieldResult
    """

    return YieldResult(
        date=valid_yield_recorded_date__job1,
        crop=valid_crop,
        weight=valid_yield_weight,
        unit=valid_weight_unit,
    )


@pytest.fixture()
def yield_result__job2(
    valid_yield_recorded_date__job2,
    valid_crop,
    valid_yield_weight,
    valid_weight_unit,
) -> YieldResult:
    """
    Returns a valid yield result
    :param valid_yield_recorded_date__job2:
    :param valid_crop:
    :param valid_yield_weight:
    :param valid_weight_unit:
    :return: YieldResult
    """

    return YieldResult(
        date=valid_yield_recorded_date__job2,
        crop=valid_crop,
        weight=valid_yield_weight,
        unit=valid_weight_unit,
    )


@pytest.fixture()
def yield_result__trouble_early(
    invalid_yield_recorded_date__early,
    valid_crop,
    valid_yield_weight,
    valid_weight_unit,
) -> YieldResult:
    """
    Returns a valid yield result
    :param invalid_yield_recorded_date__early
    :param valid_crop:
    :param valid_yield_weight:
    :param valid_weight_unit:
    :return: YieldResult
    """

    return YieldResult(
        date=invalid_yield_recorded_date__early,
        crop=valid_crop,
        weight=valid_yield_weight,
        unit=valid_weight_unit,
    )


@pytest.fixture()
def yield_result__trouble_late(
    invalid_yield_recorded_date__very_late,
    valid_crop,
    valid_yield_weight,
    valid_weight_unit,
) -> YieldResult:
    """
    Returns a valid yield result
    :param invalid_yield_recorded_date__very_late:
    :param valid_crop:
    :param valid_yield_weight:
    :param valid_weight_unit:
    :return: YieldResult
    """

    return YieldResult(
        date=invalid_yield_recorded_date__very_late,
        crop=valid_crop,
        weight=valid_yield_weight,
        unit=valid_weight_unit,
    )


@pytest.fixture()
def yield_result__very_late(
    invalid_yield_recorded_date__early,
    valid_crop,
    valid_yield_weight,
    valid_weight_unit,
) -> YieldResult:
    """
    Returns a valid yield result
    :param invalid_yield_recorded_date__early
    :param valid_crop:
    :param valid_yield_weight:
    :param valid_weight_unit:
    :return: YieldResult
    """

    return YieldResult(
        date=invalid_yield_recorded_date__early,
        crop=valid_crop,
        weight=valid_yield_weight,
        unit=valid_weight_unit,
    )


@pytest.fixture()
def job_to_output_rows_spec(
    valid_crop,
    valid_growth_job_id,
    valid_start_date__job1,
    valid_end_date__job1,
    valid_yield_recorded_date__job1,
    valid_yield_weight,
    valid_weight_unit,
) -> JobToOutputRowsSpec:
    """
    Returns a valid job_to_output_rows_spec
    :param valid_crop:
    :param valid_growth_job_id:
    :param valid_start_date__job1
    :param valid_end_date__job1:
    :param valid_yield_recorded_date__job1:
    :param valid_yield_weight:
    :param valid_weight_unit:
    :return: JobToOutputRowsSpec
    """
    return JobToOutputRowsSpec(
        crop=valid_crop,
        growth_job_id=valid_growth_job_id,
        growth_job_start_date=valid_start_date__job1,
        growth_job_end_date=valid_end_date__job1,
        yield_recorded_date=valid_yield_recorded_date__job1,
        yield_weight=valid_yield_weight,
        yield_unit=valid_weight_unit,
    )


@pytest.fixture()
def job_to_output_rows_spec2(
    valid_crop,
    valid_growth_job_id2,
    valid_start_date__job2,
    valid_end_date__job2,
    valid_yield_recorded_date__job2,
    valid_yield_weight,
    valid_weight_unit,
) -> JobToOutputRowsSpec:
    """
    Returns a valid job_to_output_rows_spec
    :param valid_crop:
    :param valid_growth_job_id2:
    :param valid_start_date__job2
    :param valid_end_date__job2:
    :param valid_yield_recorded_date__job2:
    :param valid_yield_weight:
    :param valid_weight_unit:
    :return: JobToOutputRowsSpec
    """
    return JobToOutputRowsSpec(
        crop=valid_crop,
        growth_job_id=valid_growth_job_id2,
        growth_job_start_date=valid_start_date__job2,
        growth_job_end_date=valid_end_date__job2,
        yield_recorded_date=valid_yield_recorded_date__job2,
        yield_weight=valid_yield_weight,
        yield_unit=valid_weight_unit,
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
