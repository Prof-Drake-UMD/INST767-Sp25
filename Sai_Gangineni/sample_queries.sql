-- Query 1: Recommend exercise categories for high-calorie days
WITH high_calorie_days AS (
  SELECT DATE(logged_at) AS day
  FROM `your_dataset.nutrition_logs`
  GROUP BY day
  HAVING AVG(calories) > (
    SELECT AVG(calories) FROM `your_dataset.nutrition_logs`
  )
)
SELECT DISTINCT e.category AS recommended_category
FROM `your_dataset.exercises` AS e
JOIN high_calorie_days AS h
ON TRUE
ORDER BY recommended_category;

-- Query 2: Analyze average calories per equipment type and weather
SELECT 
  equipment_name,
  w.weather_description,
  AVG(n.calories) AS avg_calories
FROM `your_dataset.exercises` AS e
JOIN UNNEST(e.equipment) AS equipment_name
JOIN `your_dataset.weather_logs` AS w
ON TRUE
JOIN `your_dataset.nutrition_logs` AS n
ON DATE(w.logged_at) = DATE(n.logged_at)
GROUP BY equipment_name, weather_description
ORDER BY avg_calories DESC;

-- Query 3: Recommend exercises targeting muscles relevant to high-protein days under specific weather
WITH high_protein_days AS (
  SELECT DATE(logged_at) AS day
  FROM `your_dataset.nutrition_logs`
  GROUP BY day
  HAVING AVG(protein) > (
    SELECT AVG(protein) FROM `your_dataset.nutrition_logs`
  )
)
SELECT DISTINCT e.category AS suggested_category
FROM `your_dataset.exercises` AS e
JOIN `your_dataset.exercise_primary_muscles` AS epm
ON e.id = epm.exercise_id
JOIN `your_dataset.muscles` AS m
ON epm.muscle_id = m.muscle_id
JOIN high_protein_days AS h
ON TRUE
JOIN `your_dataset.weather_logs` AS w
ON DATE(w.logged_at) = DATE(h.day)
WHERE w.weather_description LIKE '%rain%'
ORDER BY suggested_category;

-- Original Query 4: Top exercises targeting most muscles
SELECT e.name, COUNT(epm.muscle_id) AS num_primary_muscles
FROM `your_dataset.exercises` AS e
LEFT JOIN `your_dataset.exercise_primary_muscles` AS epm
ON e.id = epm.exercise_id
GROUP BY e.name
ORDER BY num_primary_muscles DESC
LIMIT 10;

-- Original Query 5: Count exercises by equipment
SELECT equipment_name, COUNT(*) AS num_exercises
FROM `your_dataset.exercises`, UNNEST(equipment) AS equipment_name
GROUP BY equipment_name
ORDER BY num_exercises DESC;
