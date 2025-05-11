-- Query 1. Find exercises that target major muscle groups and pair them with high-protein foods
SELECT
    e.name AS exercise_name,
    e.primary_muscles,
    n.food_name,
    n.protein
FROM exercises e
JOIN nutrition_logs n
  ON LOWER(n.food_name) LIKE CONCAT('%', LOWER(e.primary_muscles), '%')
WHERE n.protein > 10
ORDER BY n.protein DESC;

-- Query 2. Compare calories in branded vs natural foods for fitness-focused users
SELECT
    u.description AS usda_food,
    u.brandOwner,
    n.food_name,
    u.calories AS branded_calories,
    n.calories AS estimated_calories,
    ABS(u.calories - n.calories) AS calorie_difference
FROM usda_foods u
JOIN nutrition_logs n
  ON LOWER(u.description) LIKE CONCAT('%', LOWER(n.food_name), '%')
WHERE u.calories IS NOT NULL AND n.calories IS NOT NULL
ORDER BY calorie_difference DESC;

-- Query 3. Rank exercises by number of compatible nutrient-rich foods
SELECT
    e.name AS exercise,
    COUNT(n.food_name) AS matching_foods
FROM exercises e
JOIN nutrition_logs n
  ON LOWER(n.food_name) LIKE CONCAT('%', LOWER(e.primary_muscles), '%')
GROUP BY e.name
ORDER BY matching_foods DESC
LIMIT 10;

-- Query 4. Identify food categories in USDA that best support muscle-building
SELECT
    foodCategory,
    AVG(protein) AS avg_protein
FROM usda_foods
WHERE protein IS NOT NULL
GROUP BY foodCategory
ORDER BY avg_protein DESC
LIMIT 10;

-- Query 5. Recommend food matches for top compound exercises (multi-muscle)
SELECT
    e.name AS exercise,
    e.primary_muscles,
    n.food_name,
    n.calories,
    n.protein
FROM exercises e
JOIN nutrition_logs n
  ON LOWER(n.food_name) LIKE CONCAT('%', LOWER(e.primary_muscles), '%')
WHERE (e.primary_muscles LIKE '%chest%' OR e.primary_muscles LIKE '%legs%' OR e.primary_muscles LIKE '%back%')
  AND n.protein > 5
ORDER BY n.protein DESC;
