CREATE TABLE inst767_final.nutrition_logs (
  food_name STRING,
  calories FLOAT64,
  protein FLOAT64,
  fat FLOAT64,
  carbs FLOAT64
);

CREATE TABLE inst767_final.usda_foods (
  description STRING,
  brand STRING,
  fdc_id STRING,
  category STRING,
  calories FLOAT64,
  protein FLOAT64,
  fat FLOAT64,
  carbs FLOAT64
);

CREATE TABLE inst767_final.exercises (
  exercise_id INT64,
  name STRING,
  category STRING,
  equipment STRING,
  description STRING
);
