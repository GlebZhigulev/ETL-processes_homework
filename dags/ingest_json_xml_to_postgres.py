from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


DATA_DIR = Path("/opt/airflow/data")
JSON_PATH = DATA_DIR / "pets-data.json"
XML_PATH = DATA_DIR / "nutrition.xml"

POSTGRES_CONN_ID = "warehouse_postgres"


def to_int(x):
    try:
        return int(str(x).strip())
    except Exception:
        return None


def to_num(x):
    try:
        s = str(x).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def el_text(parent, tag: str) -> str | None:
    el = parent.find(tag)
    if el is None or el.text is None:
        return None
    return el.text.strip()


def el_attr(parent, tag: str, attr: str) -> str | None:
    el = parent.find(tag)
    if el is None:
        return None
    v = el.get(attr)
    return v.strip() if isinstance(v, str) else v


with DAG(
    dag_id="ingest_pets_and_nutrition",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["json", "xml", "postgres"],
) as dag:

    @task
    def extract_pets() -> list[dict]:
        if not JSON_PATH.exists():
            return []

        doc = json.loads(JSON_PATH.read_text(encoding="utf-8"))
        rows = []
        for p in doc.get("pets", []):
            rows.append(
                {
                    "name": p.get("name"),
                    "species": p.get("species"),
                    "birth_year": to_int(p.get("birthYear")),
                    "photo": p.get("photo"),
                    "fav_foods": p.get("favFoods", []),
                }
            )
        return rows

    @task
    def extract_daily_values() -> dict | None:
        if not XML_PATH.exists():
            return None

        root = ET.fromstring(XML_PATH.read_text(encoding="utf-8"))
        dv = root.find("daily-values")
        if dv is None:
            return None

        return {
            "total_fat_value": to_num(el_text(dv, "total-fat")),
            "total_fat_units": el_attr(dv, "total-fat", "units"),
            "saturated_fat_value": to_num(el_text(dv, "saturated-fat")),
            "saturated_fat_units": el_attr(dv, "saturated-fat", "units"),
            "cholesterol_value": to_num(el_text(dv, "cholesterol")),
            "cholesterol_units": el_attr(dv, "cholesterol", "units"),
            "sodium_value": to_num(el_text(dv, "sodium")),
            "sodium_units": el_attr(dv, "sodium", "units"),
            "carb_value": to_num(el_text(dv, "carb")),
            "carb_units": el_attr(dv, "carb", "units"),
            "fiber_value": to_num(el_text(dv, "fiber")),
            "fiber_units": el_attr(dv, "fiber", "units"),
            "protein_value": to_num(el_text(dv, "protein")),
            "protein_units": el_attr(dv, "protein", "units"),
        }

    @task
    def extract_foods() -> list[dict]:
        if not XML_PATH.exists():
            return []

        root = ET.fromstring(XML_PATH.read_text(encoding="utf-8"))

        foods = []
        for food in root.findall(".//food"):
            name = (el_text(food, "name") or "").strip()
            mfr = (el_text(food, "mfr") or "").strip()

            serving_value = to_num(el_text(food, "serving"))
            serving_units = el_attr(food, "serving", "units")
            if isinstance(serving_units, str):
                serving_units = serving_units.strip()

            calories_el = food.find("calories")
            calories_total = to_int(calories_el.get("total")) if calories_el is not None else None
            calories_fat = to_int(calories_el.get("fat")) if calories_el is not None else None

            vitamins = food.find("vitamins")
            minerals = food.find("minerals")

            foods.append(
                {
                    "name": name,
                    "mfr": mfr,
                    "serving_value": serving_value,
                    "serving_units": serving_units,
                    "calories_total": calories_total,
                    "calories_fat": calories_fat,
                    "total_fat": to_num(el_text(food, "total-fat")),
                    "saturated_fat": to_num(el_text(food, "saturated-fat")),
                    "cholesterol": to_num(el_text(food, "cholesterol")),
                    "sodium": to_num(el_text(food, "sodium")),
                    "carb": to_num(el_text(food, "carb")),
                    "fiber": to_num(el_text(food, "fiber")),
                    "protein": to_num(el_text(food, "protein")),
                    "vitamin_a": to_num(el_text(vitamins, "a")) if vitamins is not None else None,
                    "vitamin_c": to_num(el_text(vitamins, "c")) if vitamins is not None else None,
                    "mineral_ca": to_num(el_text(minerals, "ca")) if minerals is not None else None,
                    "mineral_fe": to_num(el_text(minerals, "fe")) if minerals is not None else None,
                }
            )
        return foods

    @task
    def load_pets(rows: list[dict]) -> int:
        if not rows:
            return 0

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        sql = """
        INSERT INTO staging.pets (name, species, birth_year, photo, fav_foods)
        VALUES (%(name)s, %(species)s, %(birth_year)s, %(photo)s, %(fav_foods)s)
        ON CONFLICT (name) DO UPDATE SET
          species = EXCLUDED.species,
          birth_year = EXCLUDED.birth_year,
          photo = EXCLUDED.photo,
          fav_foods = EXCLUDED.fav_foods,
          loaded_at = NOW();
        """
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()
        finally:
            conn.close()
        return len(rows)

    @task
    def load_daily_values(row: dict | None) -> int:
        if not row:
            return 0

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
        INSERT INTO staging.daily_values (
          total_fat_value, total_fat_units,
          saturated_fat_value, saturated_fat_units,
          cholesterol_value, cholesterol_units,
          sodium_value, sodium_units,
          carb_value, carb_units,
          fiber_value, fiber_units,
          protein_value, protein_units
        )
        VALUES (
          %(total_fat_value)s, %(total_fat_units)s,
          %(saturated_fat_value)s, %(saturated_fat_units)s,
          %(cholesterol_value)s, %(cholesterol_units)s,
          %(sodium_value)s, %(sodium_units)s,
          %(carb_value)s, %(carb_units)s,
          %(fiber_value)s, %(fiber_units)s,
          %(protein_value)s, %(protein_units)s
        );
        """
        hook.run("TRUNCATE staging.daily_values;")
        hook.run(sql, parameters=row)
        
        return 1

    @task
    def load_foods(rows: list[dict]) -> int:
        if not rows:
            return 0

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        sql = """
        INSERT INTO staging.nutrition_foods (
          name, mfr, serving_value, serving_units,
          calories_total, calories_fat,
          total_fat, saturated_fat, cholesterol, sodium, carb, fiber, protein,
          vitamin_a, vitamin_c, mineral_ca, mineral_fe
        )
        VALUES (
          %(name)s, %(mfr)s, %(serving_value)s, %(serving_units)s,
          %(calories_total)s, %(calories_fat)s,
          %(total_fat)s, %(saturated_fat)s, %(cholesterol)s, %(sodium)s, %(carb)s, %(fiber)s, %(protein)s,
          %(vitamin_a)s, %(vitamin_c)s, %(mineral_ca)s, %(mineral_fe)s
        )
        ON CONFLICT (name, mfr) DO UPDATE SET
          serving_value = EXCLUDED.serving_value,
          serving_units = EXCLUDED.serving_units,
          calories_total = EXCLUDED.calories_total,
          calories_fat = EXCLUDED.calories_fat,
          total_fat = EXCLUDED.total_fat,
          saturated_fat = EXCLUDED.saturated_fat,
          cholesterol = EXCLUDED.cholesterol,
          sodium = EXCLUDED.sodium,
          carb = EXCLUDED.carb,
          fiber = EXCLUDED.fiber,
          protein = EXCLUDED.protein,
          vitamin_a = EXCLUDED.vitamin_a,
          vitamin_c = EXCLUDED.vitamin_c,
          mineral_ca = EXCLUDED.mineral_ca,
          mineral_fe = EXCLUDED.mineral_fe,
          loaded_at = NOW();
        """
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()
        finally:
            conn.close()
            
        return len(rows)

    pets = extract_pets()
    daily = extract_daily_values()
    foods = extract_foods()

    load_pets(pets)
    load_daily_values(daily)
    load_foods(foods)
