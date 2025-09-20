# Dagster App

This service provides Dagster jobs for extracting metadata and running statistical checks on the [Home Credit Default Risk](https://www.kaggle.com/competitions/home-credit-default-risk/data) dataset. The pipelines currently operate on CSV extracts that have been downloaded locally.

## Getting started locally

0. Put data into ./dagster_app/data/home_credit/**.csv

1. Install dependencies with [uv](https://github.com/astral-sh/uv):

   ```bash
   cd dagster_app
   uv sync
   ```

2. Execute the metadata job:

   ```bash
   uv run dagster job execute -m dagster_app -j metadata_job
   ```

3. Execute the statistics job:

   ```bash
   uv run dagster job execute -m dagster_app -j statistics_job
   ```

4. Can also run

   ```bash
   uv run dagster-webserver -m dagster_app -h 0.0.0.0 -p 3000
   ```

By default the jobs look for CSV files in `dagster_app/data/home_credit`. The location can be overridden via the `DATASET_DIR` environment variable or by configuring the `dataset_dir` op config when running a job.

Generated artifacts are stored in `dagster_app/storage/metadata` and `dagster_app/storage/statistics`.
