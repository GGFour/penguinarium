from pathlib import Path

import yaml


CONFIG_PATH = Path(__file__).resolve().parents[1] / "dagster_home" / "dagster.yaml"
EXPECTED_ENV_VARS = {
    "username": {"env": "DAGSTER_POSTGRES_USER"},
    "password": {"env": "DAGSTER_POSTGRES_PASSWORD"},
    "hostname": {"env": "DAGSTER_POSTGRES_HOST"},
    "db_name": {"env": "DAGSTER_POSTGRES_DB"},
    "port": {"env": "DAGSTER_POSTGRES_PORT"},
}


def _load_config():
    with CONFIG_PATH.open("r", encoding="utf-8") as config_file:
        return yaml.safe_load(config_file)


def test_dagster_config_file_exists():
    assert CONFIG_PATH.exists(), "dagster.yaml must be present for Dagster to start"


def test_storages_use_shared_postgres():
    config = _load_config()

    expected_storages = {
        "run_storage": ("dagster_postgres.run_storage", "PostgresRunStorage"),
        # Module path changed: dagster_postgres.event_log_storage -> dagster_postgres.event_log
        "event_log_storage": ("dagster_postgres.event_log", "PostgresEventLogStorage"),
        "schedule_storage": ("dagster_postgres.schedule_storage", "PostgresScheduleStorage"),
    }

    for storage_key, (module_name, class_name) in expected_storages.items():
        assert storage_key in config, f"Missing configuration for {storage_key}"

        storage_config = config[storage_key]
        assert storage_config.get("module") == module_name
        assert storage_config.get("class") == class_name

        postgres_config = storage_config.get("config", {}).get("postgres_db")
        assert postgres_config is not None, f"{storage_key} must use the shared PostgreSQL database"

        assert postgres_config == EXPECTED_ENV_VARS, (
            "PostgreSQL configuration must reference shared environment variables"
        )


def test_no_dedicated_dagster_database_reference():
    config_text = CONFIG_PATH.read_text(encoding="utf-8")
    assert "dagsterdb" not in config_text.lower(), "Configuration should not reference a separate dagster database"
