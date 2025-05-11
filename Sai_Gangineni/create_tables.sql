CREATE TABLE nutrition_logs (
    food_name STRING,
    calories FLOAT64,
    protein FLOAT64,
    total_fat FLOAT64,
    carbohydrates FLOAT64
);

CREATE TABLE exercises (
    exercise_id INT64,
    name STRING,
    description STRING,
    category STRING,
    equipment STRING,
    primary_muscles STRING
);

CREATE TABLE usda_foods (
    fdcId INT64,
    description STRING,
    brandOwner STRING,
    foodCategory STRING,
    protein FLOAT64,
    fat FLOAT64,
    carbs FLOAT64,
    calories FLOAT64
);
