-- 1. Get a monthly job posting trends by company, source, and category, including job counts and average salary --
SELECT 
  company_name,
  source,
  SUBSTR(posted_date, 1, 7) AS month,
  job_category,
  COUNT(*) AS job_count,
  AVG(
    (
      CAST(REGEXP_EXTRACT(REPLACE(salary, '$', ''), r'^([0-9.]+)') AS FLOAT64) +
      CAST(REGEXP_EXTRACT(REPLACE(salary, '$', ''), r'- ([0-9.]+)$') AS FLOAT64)
    ) / 2
  ) AS avg_salary
FROM `thermal-slice-458921-t9.job_data.standardized_jobs` 
WHERE posted_date IS NOT NULL
GROUP BY company_name, month, job_category, source
LIMIT 1000

-- 2. Top 10 (or less) companies hiring most in each job category this month --
SELECT
    job_category,
    company_name,
    source,
    SUBSTR(posted_date, 1, 7) AS month,
    COUNT(*) AS total_postings
FROM `thermal-slice-458921-t9.job_data.standardized_jobs` 
WHERE 
    posted_date IS NOT NULL
    AND SUBSTR(posted_date, 1, 7) = FORMAT_DATE('%Y-%m', CURRENT_DATE())
GROUP BY job_category, company_name, source, month
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY job_category
    ORDER BY total_postings DESC
) <= 10

-- 3. New companies in the job market this week, sorted by job counts in each company --
SELECT 
    company_name,
    COUNT(*) AS job_count
FROM `thermal-slice-458921-t9.job_data.standardized_jobs` 
WHERE 
    PARSE_DATE('%Y-%m-%d', SUBSTR(posted_date, 1, 10)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY company_name
ORDER BY job_count DESC


-- 4. Top APIs used for job data by job counts this month --
SELECT
    source,
    SUBSTR(posted_date, 1, 7) AS month,
    COUNT(*) AS job_count
FROM `thermal-slice-458921-t9.job_data.standardized_jobs` 
WHERE 
    posted_date IS NOT NULL
    AND SUBSTR(posted_date, 1, 7) = FORMAT_DATE('%Y-%m', CURRENT_DATE())
GROUP BY source, month
ORDER BY job_count DESC

-- 5. Salary distribution by job category and type --
SELECT
    job_category,
    job_type,
    MIN(CAST(REGEXP_EXTRACT(REPLACE(salary, '$', ''), r'([0-9.]+)') AS FLOAT64)) AS min_salary,
    MAX(CAST(REGEXP_EXTRACT(REPLACE(salary, '$', ''), r'([0-9.]+)') AS FLOAT64)) AS max_salary,
    AVG(CAST(REGEXP_EXTRACT(REPLACE(salary, '$', ''), r'([0-9.]+)') AS FLOAT64)) AS avg_salary
FROM `thermal-slice-458921-t9.job_data.standardized_jobs` 
WHERE 
    salary IS NOT NULL
    AND job_category IS NOT NULL
    AND job_type IS NOT NULL
    AND REGEXP_EXTRACT(REPLACE(salary, '$', ''), r'([0-9.]+)') IS NOT NULL
GROUP BY job_category, job_type