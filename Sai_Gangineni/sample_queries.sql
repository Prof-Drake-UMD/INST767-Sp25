-- 1. Top 5 foods with the highest protein per calorie ratio
SELECT
  food_name,
  calories,
  protein,
  ROUND(protein / NULLIF(calories, 0), 2) AS protein_per_calorie
FROM `inst767_final.nutrition_logs`
WHERE calories > 0
ORDER BY protein_per_calorie DESC
LIMIT 5;

-- 2. Exercises with 'arms' in their category and relevant USDA foods containing 'chicken'
SELECT
  e.name AS exercise_name,
  e.category,
  u.description AS usda_food
FROM `inst767_final.exercises` e
JOIN `inst767_final.usda_foods` u
  ON LOWER(e.category) = 'arms' AND LOWER(u.description) LIKE '%chicken%'
LIMIT 10;

-- 3. Nutrition logs that match USDA descriptions and provide more than 15g protein
SELECT
  n.food_name,
  u.description AS usda_match,
  n.protein
FROM `inst767_final.nutrition_logs` n
JOIN `inst767_final.usda_foods` u
  ON LOWER(n.food_name) LIKE CONCAT('%', LOWER(u.description), '%')
WHERE n.protein > 15
ORDER BY n.protein DESC
LIMIT 10;

-- 4. Count of exercises per equipment type
SELECT
  equipment,
  COUNT(*) AS exercise_count
FROM `inst767_final.exercise_logs`
GROUP BY equipment
ORDER BY exercise_count DESC
LIMIT 10;

-- 5. Join all three tables: foods from USDA that are logged in Nutritionix and matched to equipment-based exercises
SELECT
  u.description AS usda_food,
  n.food_name,
  e.name AS exercise_name,
  e.equipment
FROM `inst767_final.usda_foods` u
JOIN `inst767_final.nutrition_logs` n
  ON LOWER(n.food_name) LIKE CONCAT('%', LOWER(u.description), '%')
JOIN `inst767_final.exercises` e
  ON LOWER(e.equipment) LIKE '%dumbbell%'
LIMIT 10;
