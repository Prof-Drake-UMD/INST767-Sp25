SELECT
  c.year,
  c.period,
  c.month,
  c.cpi_value,
  e.industry,
  e.employment_level,
  u.unemployment_rate
FROM `inst767-project-481615.inst767_sp25.cpi_inflation` AS c
JOIN `inst767-project-481615.inst767_sp25.ces_employment` AS e
  ON c.year = e.year
 AND c.period = e.period
JOIN `inst767-project-481615.inst767_sp25.laus_unemployment` AS u
  ON c.year = u.year
 AND c.period = u.period
ORDER BY c.year DESC, c.period DESC, e.industry
LIMIT 200;
