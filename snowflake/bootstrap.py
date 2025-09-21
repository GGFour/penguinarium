import os
import time
from typing import Any

import snowflake.connector


def _connect() -> snowflake.connector.SnowflakeConnection:
    connect_args: dict[str, Any] = {
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "host": os.environ.get("SNOWFLAKE_INTERNAL_HOST", "127.0.0.1"),
        "port": int(os.environ["SNOWFLAKE_PORT"]),
        "protocol": os.environ.get("SNOWFLAKE_PROTOCOL", "http"),
        "session_parameters": {"CLIENT_OUT_OF_BAND_TELEMETRY_ENABLED": False},
        "network_timeout": 5,
    }

    database = os.environ.get("SNOWFLAKE_DATABASE")
    schema = os.environ.get("SNOWFLAKE_SCHEMA")
    warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")

    if database:
        connect_args["database"] = database
    if schema:
        connect_args["schema"] = schema
    if warehouse:
        connect_args["warehouse"] = warehouse

    return snowflake.connector.connect(**connect_args)


def ensure_dataset() -> None:
    database = os.environ.get("SNOWFLAKE_DATABASE", "DEMO_DB")
    schema = os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
    table = os.environ.get("SNOWFLAKE_TABLE", "CUSTOMERS")

    for attempt in range(1, 31):
        try:
            with _connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"CREATE OR REPLACE DATABASE {database}")
                    cur.execute(f"CREATE OR REPLACE SCHEMA {database}.{schema}")
                    cur.execute(f"USE DATABASE {database}")
                    cur.execute(f"USE SCHEMA {schema}")
                    cur.execute(
                        f"CREATE OR REPLACE TABLE {table} ("
                        "CUSTOMER_ID INT,"
                        "FULL_NAME STRING,"
                        "TOTAL_PURCHASE NUMERIC"
                        ")"
                    )
                    cur.execute(
                        "INSERT INTO {table} (CUSTOMER_ID, FULL_NAME, TOTAL_PURCHASE) VALUES "
                        "(1, 'Alice Adams', 125.50),"
                        "(2, 'Bob Brown', 310.00),"
                        "(3, 'Carol Clark', 58.75)"
                        .format(table=table)
                    )
                    conn.commit()
                break
        except Exception:  # pragma: no cover - bootstrapping resilience
            if attempt == 30:
                raise
            time.sleep(1)
    else:
        raise RuntimeError("Unable to initialise Fake Snowflake dataset")


if __name__ == "__main__":
    ensure_dataset()
