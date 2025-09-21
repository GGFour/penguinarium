# Dagster App

This service provides Dagster jobs for extracting metadata and running statistical checks on the [Home Credit Default Risk](https://www.kaggle.com/competitions/home-credit-default-risk/data) dataset. The pipelines currently operate on CSV extracts that have been downloaded locally.

## Getting started locally

0. Put data into ./dagster_app/data/home_credit/**.csv

1. Configure Dagster to use the shared PostgreSQL database:

   ```bash
   export DAGSTER_HOME="$(pwd)/dagster_home"
   export DAGSTER_POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
   export DAGSTER_POSTGRES_PORT="${POSTGRES_PORT:-5432}"
   export DAGSTER_POSTGRES_DB="${POSTGRES_DB:-penguinarium}"
   export DAGSTER_POSTGRES_USER="${POSTGRES_USER:-penguinarium}"
   export DAGSTER_POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-changeme}"
   ```

   These variables align with the values used by Docker Compose so the Dagster instance
   shares the same PostgreSQL database as the rest of the application. The
   `dagster_home/dagster.yaml` file configures Dagster's run, event log, and schedule
   storage to use this database via the `dagster-postgres` plugin (`PostgresRunStorage`,
   `PostgresEventLogStorage`, `PostgresScheduleStorage`). Make sure the
   `dagster-postgres` dependency is installed (it's declared in `pyproject.toml`).

2. Install dependencies with [uv](https://github.com/astral-sh/uv):

   ```bash
   cd dagster_app
   uv sync
   ```

3. Execute the metadata job:

   ```bash
   uv run dagster job execute -m dagster_app -j metadata_job
   ```

4. Execute the statistics job:

   ```bash
   uv run dagster job execute -m dagster_app -j statistics_job
   ```

5. Generate synthetic LLM alerts:

   ```bash
   uv run dagster job execute -m dagster_app -j llm_pipeline
   ```

6. Can also run

   ```bash
   uv run dagster-webserver -m dagster_app -h 0.0.0.0 -p 3000
   ```

## Tests

Run the Dagster service tests with:

```bash
uv run --extra test pytest
```

By default the jobs look for CSV files in `dagster_app/data/home_credit`. The location can be overridden via the `DATASET_DIR` environment variable or by configuring the `dataset_dir` op config when running a job.

Both jobs persist their outputs directly into the shared PostgreSQL database
used by the Django application. The metadata job (``metadata_job``) creates or
updates records in the ``pulling_datasource``, ``pulling_tablemetadata``,
``pulling_fieldmetadata``, and ``pulling_fieldrelation`` tables. The statistics
job (``statistics_job``) augments the JSON payload stored on each
``pulling_fieldmetadata`` record with the latest computed metrics.
