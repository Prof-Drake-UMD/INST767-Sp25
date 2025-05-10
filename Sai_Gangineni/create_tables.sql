-- Exercises table
CREATE TABLE `your_dataset.exercises` (
  id INT64,
  name STRING,
  description STRING,
  category STRING,
  equipment ARRAY<STRING>,
  image_urls ARRAY<STRING>,
  last_update TIMESTAMP
);

-- Muscles dimension table
CREATE TABLE `your_dataset.muscles` (
  muscle_id INT64,
  muscle_name STRING
);

-- Association table: exercises → primary muscles
CREATE TABLE `your_dataset.exercise_primary_muscles` (
  exercise_id INT64,
  muscle_id INT64
);

-- Nutrition logs table
CREATE TABLE `your_dataset.nutrition_logs` (
  id STRING,
  food_name STRING,
  calories FLOAT64,
  total_fat FLOAT64,
  saturated_fat FLOAT64,
  protein FLOAT64,
  carbohydrates FLOAT64,
  sugars FLOAT64,
  serving_weight_grams FLOAT64,
  logged_at TIMESTAMP
);

-- Weather logs table
CREATE TABLE `your_dataset.weather_logs` (
  id STRING,
  city STRING,
  temperature FLOAT64,
  humidity FLOAT64,
  weather_description STRING,
  wind_speed FLOAT64,
  cloud_coverage INT64,
  logged_at TIMESTAMP
);
