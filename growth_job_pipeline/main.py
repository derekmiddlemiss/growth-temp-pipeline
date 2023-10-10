from __future__ import annotations

import csv
import datetime
import json
import logging
import os
from typing import TYPE_CHECKING
from uuid import uuid4, UUID

from growth_job_pipeline.config import config
from growth_job_pipeline.growth_job_api import (
    get_time_filtered_growth_jobs_for_crop,
)
from growth_job_pipeline.logger import setup_logger
from growth_job_pipeline.models.enums.telemetry_measurement_type import (
    TelemetryMeasurementType,
)
from growth_job_pipeline.models.enums.telemetry_measurement_unit import (
    TelemetryMeasurementUnit,
)
from growth_job_pipeline.models.validators.coalesced_timestamps import (
    CoalescedTimestamps,
)
from growth_job_pipeline.models.validators.job_to_output_rows_spec import (
    JobToOutputRowsSpec,
)
from growth_job_pipeline.models.validators.output_row import (
    output_columns,
    OutputRow,
)
from growth_job_pipeline.telemetry_db import (
    telemetry_entries_batcher,
)
from growth_job_pipeline.telemetry_db.db import get_telemetry_db_cursor
from growth_job_pipeline.utils import (
    get_config_timestamps,
    coalesce_run_timestamps,
    latest_datetime_possible_for_date,
)
from growth_job_pipeline.yield_tsv_reader import get_ascending_yield_results

if TYPE_CHECKING:
    from growth_job_pipeline.models.validators.config_timestamps import (
        ConfigTimestamps,
    )
    from growth_job_pipeline.models.validators.yield_result import YieldResult
    from growth_job_pipeline.models.validators.growth_job import GrowthJob
    from growth_job_pipeline.models.validators.telemetry_entry import (
        TelemetryEntry,
    )

logger = logging.getLogger(__name__)


def create_job_to_output_rows_spec(
    yield_result: YieldResult, growth_job: GrowthJob
) -> JobToOutputRowsSpec:
    """
    Creates a JobToOutputRowsSpec from a YieldResult and GrowthJob
    :param yield_result:
    :param growth_job:
    :return: JobToOutputRowsSpec
    """
    return JobToOutputRowsSpec(
        crop=yield_result.crop,
        growth_job_id=growth_job.id,
        growth_job_start_date=growth_job.start_date,
        growth_job_end_date=growth_job.end_date,
        yield_recorded_date=yield_result.date,
        yield_weight=yield_result.weight,
        yield_unit=yield_result.unit,
    )


def match_yield_results_growth_jobs_gen_specs(
    all_yield_results_ascending: list[YieldResult],
    from_timestamp: datetime.datetime,
    to_timestamp: datetime.datetime,
) -> list[JobToOutputRowsSpec]:
    """
    Matches yield results to growth jobs, returns a list of JobToOutputRowsSpecs
    Raises RuntimeError if a yield result cannot be matched to a single growth job
    :param all_yield_results_ascending: list[YieldResult]
    :param from_timestamp: datetime.datetime
    :param to_timestamp: datetime.datetime
    :return: list[JobToOutputRowsSpec]
    """

    # use a search horizon for growth jobs to avoid overburdening API
    # could remove if we'd prefer an exhaustive search before raising error
    MAX_DELAY_DAYS_SEARCH = config(
        "MAX_DAYS_DELAY_GROWTH_JOB_YIELD_RESULT", cast=int
    )

    # list comprehensions preserve order, so this should be ascending
    in_scope_yield_results = [
        yield_result
        for yield_result in all_yield_results_ascending
        if from_timestamp
        <= latest_datetime_possible_for_date(yield_result.date)
        < to_timestamp
    ]

    # go in reverse order, latest first
    # assumes yield results are sorted ascending, this should be assured on fetch
    job_to_output_rows_specs = []
    for this_yield_result in in_scope_yield_results[::-1]:
        # get index of this yield result in all_yield_results_ascending
        index = all_yield_results_ascending.index(this_yield_result)
        previous_yield_results_for_crop = [
            prev_yield_result
            for prev_yield_result in all_yield_results_ascending[:index]
            if prev_yield_result.crop == this_yield_result.crop
        ]
        latest_previous_yield_result_for_crop = (
            previous_yield_results_for_crop[-1]
            if previous_yield_results_for_crop
            else None
        )
        if latest_previous_yield_result_for_crop is not None:
            # there should be one previous growth job for crop between latest_previous_yield_result_for_crop and this_yield_result
            candidate_growth_jobs = get_time_filtered_growth_jobs_for_crop(
                from_timestamp=latest_datetime_possible_for_date(
                    latest_previous_yield_result_for_crop.date
                ),
                to_timestamp=latest_datetime_possible_for_date(
                    this_yield_result.date
                ),
                crop=this_yield_result.crop,
            )
            if len(candidate_growth_jobs) != 1:
                msg = (
                    "Cannot unambiguously assign yield"
                    f" result={this_yield_result} to a single growth job. Num"
                    " growth jobs found since last yield"
                    f" result={len(candidate_growth_jobs)}"
                )
                logger.error(msg)
                raise RuntimeError(msg)
            job_to_output_rows_spec = create_job_to_output_rows_spec(
                yield_result=this_yield_result,
                growth_job=candidate_growth_jobs[0],
            )
            job_to_output_rows_specs.append(job_to_output_rows_spec)

        else:
            # no previous yield result for crop - new crop or pipeline startup?
            search_from_timestamp = latest_datetime_possible_for_date(
                this_yield_result.date
            ) - datetime.timedelta(days=MAX_DELAY_DAYS_SEARCH)
            candidate_growth_jobs = get_time_filtered_growth_jobs_for_crop(
                from_timestamp=search_from_timestamp,
                to_timestamp=latest_datetime_possible_for_date(
                    this_yield_result.date
                ),
                crop=this_yield_result.crop,
            )
            if len(candidate_growth_jobs) != 1:
                msg = (
                    "Cannot unambiguously assign yield"
                    f" result={this_yield_result} to a single growth job. Num"
                    " growth jobs found search back minus"
                    f" {MAX_DELAY_DAYS_SEARCH} days={len(candidate_growth_jobs)}"
                )
                logger.error(msg)
                raise RuntimeError(msg)
            job_to_output_rows_spec = create_job_to_output_rows_spec(
                yield_result=this_yield_result,
                growth_job=candidate_growth_jobs[0],
            )
            job_to_output_rows_specs.append(job_to_output_rows_spec)

    return job_to_output_rows_specs


def get_bounding_timestamps_for_specs(
    job_to_output_rows_specs: list[JobToOutputRowsSpec],
) -> CoalescedTimestamps:
    """
    Returns the earliest start date and latest end date for a list of JobToOutputRowsSpecs
    :param job_to_output_rows_specs:
    :return: CoalescedTimestamps
    """

    job_start_dates = [
        spec.growth_job_start_date for spec in job_to_output_rows_specs
    ]
    job_end_dates = [
        spec.growth_job_end_date for spec in job_to_output_rows_specs
    ]
    return CoalescedTimestamps(
        from_timestamp=min(job_start_dates), to_timestamp=max(job_end_dates)
    )


def setup_run_output_dir(
    run_id: UUID, run_timestamp: datetime.datetime
) -> str:
    """
    Creates run output dir and returns path
    :param run_id: UUID
    :param run_timestamp: datetime.datetime
    :return: str
    """

    output_dir_path = config("OUTPUT_DIR")
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)

    run_output_dir_path = os.path.join(
        output_dir_path, f"{run_timestamp.isoformat()}_{str(run_id)}"
    )
    if os.path.exists(run_output_dir_path):
        msg = f"Output dir {run_output_dir_path} already exists"
        logger.error(msg)
        raise FileExistsError(msg)
    os.makedirs(run_output_dir_path)

    return run_output_dir_path


def write_run_data(
    run_id: UUID,
    run_output_dir_path: str,
    config_timestamps: ConfigTimestamps,
    coalesced_timestamps: CoalescedTimestamps,
    run_timestamp: datetime.datetime,
    telemetry_type_to_fetch: TelemetryMeasurementType,
    telemetry_unit_to_fetch: TelemetryMeasurementUnit,
    job_to_output_rows_specs: list[JobToOutputRowsSpec],
) -> None:
    """
    Write run_data.json file in run_output_dir
    :param run_id: UUID
    :param run_output_dir_path: str
    :param config_timestamps: ConfigTimestamps
    :param coalesced_timestamps: CoalescedTimestamps
    :param run_timestamp: datetime.datetime
    :param telemetry_type_to_fetch: TelemetryMeasurementType
    :param telemetry_unit_to_fetch: TelemetryMeasurementUnit
    :param job_to_output_rows_specs: list[JobToOutputRowsSpec]
    :return: None
    """

    run_data = {
        "deploy_environment": config("DEPLOY_ENVIRONMENT"),
        "run_id": str(run_id),
        "run_timestamp": run_timestamp.isoformat(),
        "telemetry_measurement_type": telemetry_type_to_fetch.value,
        "telemetry_measurement_unit": telemetry_unit_to_fetch.value,
        "config_timestamps": config_timestamps.model_dump(mode="json"),
        "coalesced_timestamps": coalesced_timestamps.model_dump(mode="json"),
        "max_days_delay_growth_job_yield_result": config(
            "MAX_DAYS_DELAY_GROWTH_JOB_YIELD_RESULT", cast=int
        ),
        "num_yield_results": len(job_to_output_rows_specs),
        "growth_job_ids_found_with_yields": [
            spec.growth_job_id for spec in job_to_output_rows_specs
        ],
        "growth_job_crops_found_with_yields": [
            spec.crop for spec in job_to_output_rows_specs
        ],
    }

    with open(
        os.path.join(run_output_dir_path, f"run_data_{str(run_id)}.json"), "w"
    ) as file:
        file.write(json.dumps(run_data, indent=4))


def telemetry_entry_to_output_rows(
    dict_writer: csv.DictWriter,
    telemetry_entry: TelemetryEntry,
    job_to_output_rows_specs: list[JobToOutputRowsSpec],
) -> None:
    """
    Writes telemetry entry to output rows
    :param dict_writer: csv.DictWriter
    :param telemetry_entry: TelemetryEntry
    :param job_to_output_rows_specs: list[JobToOutputRowsSpec]
    :return: None
    """
    for spec in job_to_output_rows_specs:
        if (
            spec.growth_job_start_date
            <= telemetry_entry.timestamp
            <= spec.growth_job_end_date
        ):
            output_row = OutputRow(
                timestamp=telemetry_entry.timestamp,
                crop=spec.crop,
                growth_job_id=spec.growth_job_id,
                growth_job_start_date=spec.growth_job_start_date,
                growth_job_end_date=spec.growth_job_end_date,
                yield_recorded_date=spec.yield_recorded_date,
                yield_weight=spec.yield_weight,
                yield_unit=spec.yield_unit,
                telemetry_measurement_type=telemetry_entry.type,
                telemetry_measurement_unit=telemetry_entry.unit,
                telemetry_measurement_value=telemetry_entry.value,
            )
            dict_writer.writerow(output_row.model_dump(mode="json"))


def main() -> None:
    run_id = uuid4()
    run_timestamp = datetime.datetime.now()
    run_output_dir_path = setup_run_output_dir(
        run_id=run_id, run_timestamp=run_timestamp
    )
    setup_logger(run_output_dir_path=run_output_dir_path, run_id=run_id)
    config_timestamps = get_config_timestamps()
    coalesced_timestamps = coalesce_run_timestamps(
        config_timestamps=config_timestamps
    )
    telemetry_type_to_fetch = TelemetryMeasurementType(
        config("MEASUREMENT_TYPE")
    )
    telemetry_unit_to_fetch = TelemetryMeasurementUnit(
        config("MEASUREMENT_UNIT")
    )

    from_timestamp = coalesced_timestamps.from_timestamp
    to_timestamp = coalesced_timestamps.to_timestamp

    all_yield_results_ascending = get_ascending_yield_results(
        from_timestamp=datetime.datetime.min, to_timestamp=to_timestamp
    )
    job_to_output_rows_specs = match_yield_results_growth_jobs_gen_specs(
        all_yield_results_ascending=all_yield_results_ascending,
        from_timestamp=from_timestamp,
        to_timestamp=to_timestamp,
    )

    write_run_data(
        run_id=run_id,
        run_output_dir_path=run_output_dir_path,
        config_timestamps=config_timestamps,
        coalesced_timestamps=coalesced_timestamps,
        run_timestamp=run_timestamp,
        telemetry_type_to_fetch=telemetry_type_to_fetch,
        telemetry_unit_to_fetch=telemetry_unit_to_fetch,
        job_to_output_rows_specs=job_to_output_rows_specs,
    )

    if not job_to_output_rows_specs:
        msg = (
            "No matched yield results and growth jobs found for timestamp"
            f" range {from_timestamp} to {to_timestamp}"
        )
        logger.warning(msg)
        exit(0)

    telemetry_bounding_timestamps = get_bounding_timestamps_for_specs(
        job_to_output_rows_specs=job_to_output_rows_specs
    )

    db_cursor = get_telemetry_db_cursor()
    telemetry_batches = telemetry_entries_batcher(
        cursor=db_cursor,
        type_to_fetch=telemetry_type_to_fetch,
        unit_to_fetch=telemetry_unit_to_fetch,
        batch_size=config("TELEMETRY_DB_BATCH_SIZE", cast=int),
        from_timestamp=telemetry_bounding_timestamps.from_timestamp,
        to_timestamp=telemetry_bounding_timestamps.to_timestamp,
    )

    output_file = os.path.join(
        run_output_dir_path,
        f"data_{telemetry_type_to_fetch.value}_{telemetry_unit_to_fetch.value}_{str(run_id)}.csv",
    )
    if os.path.exists(output_file):
        msg = f"Output file {output_file} already exists"
        logger.error(msg)
        raise FileExistsError(msg)
    with open(output_file, "w") as file:
        writer = csv.DictWriter(file, fieldnames=output_columns)
        writer.writeheader()
        for batch in telemetry_batches:
            for telemetry_entry in batch:
                telemetry_entry_to_output_rows(
                    dict_writer=writer,
                    telemetry_entry=telemetry_entry,
                    job_to_output_rows_specs=job_to_output_rows_specs,
                )


if __name__ == "__main__":
    main()
