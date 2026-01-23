CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.pets(
    pet_id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    species TEXT,
    birth_year INT,
    photo TEXT,
    fav_foods TEXT[],
    loaded_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_pets_name
  ON staging.pets(name);

CREATE TABLE IF NOT EXISTS staging.nutrition_foods(
    food_id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    mfr TEXT,
    serving_value NUMERIC,
    serving_units TEXT,

    calories_total INT,
    calories_fat INT,

    total_fat NUMERIC,
    saturated_fat NUMERIC,
    cholesterol NUMERIC,
    sodium NUMERIC,
    carb NUMERIC,
    fiber NUMERIC,
    protein NUMERIC,

    vitamin_a NUMERIC,
    vitamin_c NUMERIC,

    mineral_ca NUMERIC,
    mineral_fe NUMERIC,

    loaded_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_nutrition_foods_name_mfr
  ON staging.nutrition_foods(name, mfr);

CREATE TABLE IF NOT EXISTS staging.daily_values(
    daily_value_id BIGSERIAL PRIMARY KEY,

    total_fat_value NUMERIC,
    total_fat_units TEXT,

    saturated_fat_value NUMERIC,
    saturated_fat_units TEXT,

    cholesterol_value NUMERIC,
    cholesterol_units TEXT,

    sodium_value NUMERIC,
    sodium_units TEXT,
    
    carb_value NUMERIC,
    carb_units TEXT,

    fiber_value NUMERIC,
    fiber_units TEXT, 

    protein_value NUMERIC,
    protein_units TEXT,

    loaded_at TIMESTAMP NOT NULL DEFAULT NOW()
);

