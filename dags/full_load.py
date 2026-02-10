from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


DATA_DIR = Path("/opt/airflow/data")
DATA_PATH = DATA_DIR / "IOT-temp_clean.csv"

POSTGRES_CONN_ID = "warehouse_postgres"

with DAG(
    dag_id="full_load_iot_temp",
    start_date=datetime(2025, 2, 1),
    schedule=None,
    catchup=False,
    tags=["load", "postgres"],
) as dag:

    @task
    def create_table():
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.run("""
            CREATE SCHEMA IF NOT EXISTS staging;

            CREATE TABLE IF NOT EXISTS staging.iot_temp (
                "id" TEXT PRIMARY KEY,
                "room_id/id" TEXT,
                "noted_date" DATE,
                "temp" INTEGER,
                "out/in" TEXT
            );
        """)

    @task
    def full_load() -> int:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        hook.run('TRUNCATE staging.iot_temp;')

        copy_sql = """
        COPY staging.iot_temp ("id", "room_id/id", "noted_date", "temp", "out/in")
        FROM STDIN WITH (FORMAT csv, HEADER true);
        """

        hook.copy_expert(copy_sql, str(DATA_PATH))

        cnt = hook.get_first("SELECT COUNT(*) FROM staging.iot_temp;")[0]
        return int(cnt)

    create = create_table()
    load = full_load()

    create >> load
