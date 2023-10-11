FROM python:3.11-buster as builder

LABEL authors="derekmiddlemiss@gmail.com"

RUN pip install poetry==1.6.1

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR /pipeline

COPY pyproject.toml poetry.lock ./
RUN touch README.md

RUN poetry install --without dev --no-root && rm -rf $POETRY_CACHE_DIR

FROM python:3.11-slim-buster as runtime

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update -y \
    && apt-get install -y --no-install-recommends build-essential unixodbc-dev curl gnupg2
RUN curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc
RUN curl https://packages.microsoft.com/config/debian/10/prod.list | tee /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && ACCEPT_EULA=Y apt-get install -y mssql-tools18
ENV PATH="$PATH:/opt/mssql-tools18/bin"
RUN apt-get install -y unixodbc-dev libgssapi-krb5-2

ENV VIRTUAL_ENV=/pipeline/.venv PATH="/pipeline/.venv/bin:$PATH"

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}

COPY growth_job_pipeline ./growth_job_pipeline

ENTRYPOINT ["python", "-m", "growth_job_pipeline.main"]
