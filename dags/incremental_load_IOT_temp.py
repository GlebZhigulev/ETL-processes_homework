from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


DATA_DIR = Path("/opt/airflow/data")
DATA_PATH = DATA_DIR / "IOT-temp_clean.csv"

POSTGRES_CONN_ID = "warehouse_postgres"
DAYS = 5

with DAG(
    dag_id="incremental_load_iot_temp",
    start_date=datetime(2025, 2, 1),
    schedule=None,
    catchup=False,
    tags=["load", "postgres", "incremental"],
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
    def incremental_load() -> int:
        if not DATA_PATH.exists():
            raise FileNotFoundError(f"CSV not found: {DATA_PATH}")

        df = pd.read_csv(DATA_PATH, encoding="utf-8")

        df["noted_date"] = pd.to_datetime(df["noted_date"], errors="coerce").dt.date
        df = df[df["noted_date"].notna()].copy()

        max_d = df["noted_date"].max()
        cutoff = max_d - timedelta(days=DAYS)
        df = df[df["noted_date"] >= cutoff].copy()

        if df.empty:
            return 0

        rows = list(
            df[["id", "room_id/id", "noted_date", "temp", "out/in"]]
            .itertuples(index=False, name=None)
        )

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.executemany("""
                    INSERT INTO staging.iot_temp ("id", "room_id/id", "noted_date", "temp", "out/in")
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT ("id") DO NOTHING;
                """, rows)
            conn.commit()
        finally:
            conn.close()

        return len(rows)

    create_table() >> incremental_load()