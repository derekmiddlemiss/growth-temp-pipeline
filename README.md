## Introduction

A pipeline to pull yield results from a local TSV file (headers `date`,
`crop`, `weight` `unit`) and match them to growth job results pulled from
the growth jobs API (endpoint `/jobs`). Matching is inexact as yield results
do not include the `id` of the gorwth job they correspond to. Instead, the
matching is done by date and crop, raising an error if ambiguous matches
result. Telemetry entries corresponding to the requested telemetry measurement
type and unit (e.g. `temp`, `C`) are then pulled in batches from the telemetry
DB and used to populate the output file.

Output file headers and sample:
```
timestamp,crop,growth_job_id,growth_job_start_date,growth_job_end_date,yield_recorded_date,yield_weight,yield_unit,telemetry_measurement_type,telemetry_measurement_unit,telemetry_measurement_value
2021-10-05T11:40:00,basil,1,2021-10-05T11:40:00,2021-10-07T21:04:00,2021-10-07,28.0,kg,temp,C,21.53
2021-10-05T11:40:30,basil,1,2021-10-05T11:40:00,2021-10-07T21:04:00,2021-10-07,28.0,kg,temp,C,21.73
2021-10-05T11:41:00,basil,1,2021-10-05T11:40:00,2021-10-07T21:04:00,2021-10-07,28.0,kg,temp,C,22.33
```

A separate directory labelled by a unique job id and run timestamp is created for each run
. Output files written to this directory:
* `<job_id>.log` - all log output
* `data_<measurement_type>_<measurement_unit>_<job_id>.csv` - output file
* `run_data_<job_id>.json` - run metadata

## Principles
* Data is validated using `pydantic` validators on ingress, egress and internal processing
* All yield results are read from the file - it's assumed that plant scientists will append to this file but not delete
  (__important__: yield result to growth job matching will fail if previous yield results are deleted)
* Place minimal strain on the telemetry DB by pulling telemetry entries in batches. Ditto, place minimal
    strain on the growth jobs API by pulling growth jobs as needed from the API. Note
    that for the latter, the API currently does not provide an endpoint or query params
    allowing requests to filter by `crop`, `growth_job_start_date` and `growth_job_end_date` so no
    efficiencies result, as all growth jobs are pulled then filtered (TODO added).
* FROM_TIMESTAMP and TO_TIMESTAMP environment variables can be used to run the pipeline in
  windowed mode as a periodic job: they are used to filter yield results on which matching
  then proceeds. If not set, all available yield results are processed.

## Usage
The following environment variables are available to configure the pipeline:

| Variable                | Required | Options                | Default                    |
|-------------------------|----------|------------------------|----------------------------|
| `DEPLOY_ENVIRONMENT`    | No       | `staging`, `production` | `staging`                  |
| `OUTPUT_DIR`            | No       |                        | `growth_job_pipeline_data` |
| `FROM_TIMESTAMP`        | No       |                        | `datetime.datetime.min`    |
| `TO_TIMESTAMP`          | No       |                        | `datetime.datetime.max`    |
| `YIELD_RESULTS_FILE`    | Yes      |                        | None                       |
| `TELEMETRY_DB_USERNAME` | Yes      |                        | None                       |
| `TELEMETRY_DB_PASSWORD` | Yes      |                        | None                       |
| `MEASUREMENT_TYPE`      | Yes      | `temp`                  | None                       |
| `MEASUREMENT_UNIT`      | Yes      | `C`, `F`                | None                       |

Currently only temperature measurements are supported in the telemetry DB, but
the pipeline can be extended to support other measurement types and units. In the Dockerized
run, `OUTPUT_DIR` should be set as the container mount point set in the `docker run` command

The config variable `MAX_DAYS_DELAY_GROWTH_JOB_YIELD_RESULT` is set in the code to 180 days.
This represents the longest permitted delay from the end of a growth job to the yield result being
logged, and should aid efficient matching if the suggested changes to the API are made. Storing
growth ids with yield results would remove the need for this setting.

## Installation and running

The pipeline has been tested with python3.11. To install:

* Installation of the MS-SQL Server drivers may be needed: see instructions at
https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server
* Install the `poetry` build tool and package manager (see up to date instructions at
https://python-poetry.org/docs/#installation), currently `curl -sSL https://install.python-poetry.org | python3 -`
* Test `poetry` is installed correctly by running `poetry --version`
* Clone the repo and `cd` into it
* To run: set required environment variables and run `poetry run python -m growth_job_pipeline.main` (NB you
can either set the environment variables in your shell or prepend them to the command above, e.g. `OUTPUT_DIR=/tmp poetry run python -m growth_job_pipeline.main`)

For development work:
* A `.pre-commit-config.yaml` config is provided for use with `pre-commit` (https://pre-commit.com/)
* Test coverage and quality are monitored by GitHub workflows. Direct push to `main` is not allowed,
go via branches and PRs.

## TODOs
* Suggest to plant scientists they start to record the `growth_job_id` in the
  yield results file, this would make matching yield results to growth jobs exact
* Discuss with API developers the efficiencies that could result by implementing
  endpoints or query params that allow filtering growth jobs by crop, start date and
  end date
* Dockerize the pipeline to avoid the need to install the MS-SQL Server drivers
* Complete unit tests for validators
