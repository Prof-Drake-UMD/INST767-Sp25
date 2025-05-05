-- 1. Get a monthly job posting trends by company and category, including job counts and average salary --
SELECT
    company_name,
    FORMAT_TIMESTAMP('%Y-%m', posted_date) AS month,
    job_category,
    COUNT(*) AS job_count,
    AVG(CAST(REGEXP_EXTRACT(REPLACE(salary, '$', ''), r'([0-9.]+)') AS FLOAT64)) AS avg_salary
FROM jobs
WHERE posted_date IS NOT NULL
GROUP BY company_name, month, job_category

-- 2. Top 10 companies hiring most in each job category this week --
SELECT
    job_category.name AS job_category,
    company.name AS company_name,
    COUNT(*) AS total_postings
FROM jobs
WHERE DATE(posted_date) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE()
GROUP BY job_category.name, company.name
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY job_category.name
    ORDER BY COUNT(*) DESC
) <= 10

-- 3. New companies in the job market this week--
SELECT DISTINCT company_name
FROM jobs
WHERE posted_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY company_name


-- 4. Top APIs used for job data by job counts this month --
SELECT
    source_api,
    COUNT(*) AS job_count
FROM jobs
WHERE posted_date >= DATE_TRUNC(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY source_api
ORDER BY job_count DESC

-- 5. Salary distribution by job category and type --
SELECT
    job_category,
    job_type,
    MIN(CAST(REGEXP_EXTRACT(REPLACE(salary, '$', ''), r'([0-9.]+)') AS FLOAT64)) AS min_salary,
    MAX(CAST(REGEXP_EXTRACT(REPLACE(salary, '$', ''), r'([0-9.]+)') AS FLOAT64)) AS max_salary,
    AVG(CAST(REGEXP_EXTRACT(REPLACE(salary, '$', ''), r'([0-9.]+)') AS FLOAT64)) AS avg_salary
FROM jobs
WHERE salary IS NOT NULL
GROUP BY job_category, job_type