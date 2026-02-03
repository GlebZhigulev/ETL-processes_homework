from __future__ import annotations

from datetime import datetime
from pathlib import Path
import json

import pandas as pd
from airflow import DAG
from airflow.decorators import task


DATA_DIR = Path("/opt/airflow/data")

INPUT_PATH = DATA_DIR / "IOT-temp.csv"
CLEAN_CSV_PATH = DATA_DIR / "IOT-temp_clean.csv"
TOP_JSON_PATH = DATA_DIR / "top_hot_cold.json"

COL_DATE = "noted_date"
COL_TEMP = "temp"
COL_INOUT = "out/in"
IN_VALUE = "in"


def parse_date(s: pd.Series) -> pd.Series:
    dt = pd.to_datetime(s, errors="coerce")
    return dt.dt.date


with DAG(
    dag_id="transform_IOT-temp_dataset",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["transform", "csv", "json"],
) as dag:

    @task
    def extract() -> pd.DataFrame:
        if not INPUT_PATH.exists():
            raise FileNotFoundError(f"Dataset not found: {INPUT_PATH}")

        return pd.read_csv(INPUT_PATH)

    @task
    def transform(df: pd.DataFrame) -> dict:
        df = df[df[COL_INOUT].astype(str).str.lower() == IN_VALUE].copy()

        df[COL_DATE] = parse_date(df[COL_DATE])
        df = df[df[COL_DATE].notna()].copy()

        df[COL_TEMP] = pd.to_numeric(df[COL_TEMP], errors="coerce")
        df = df[df[COL_TEMP].notna()].copy()

        p05 = df[COL_TEMP].quantile(0.05)
        p95 = df[COL_TEMP].quantile(0.95)
        df[COL_TEMP] = df[COL_TEMP].clip(lower=p05, upper=p95)

        daily = df.groupby(COL_DATE, as_index=False)[COL_TEMP].mean()

        top_hot = (
            daily.sort_values(COL_TEMP, ascending=False)
            .head(5)
            .to_dict(orient="records")
        )
        top_cold = (
            daily.sort_values(COL_TEMP, ascending=True)
            .head(5)
            .to_dict(orient="records")
        )

        CLEAN_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(CLEAN_CSV_PATH, index=False)

        return {
            "top_hot_5": top_hot,
            "top_cold_5": top_cold,
        }

    @task
    def save_json(result: dict) -> str:
        TOP_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(TOP_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)
        return str(TOP_JSON_PATH)

    df = extract()
    result = transform(df)
    save_json(result)