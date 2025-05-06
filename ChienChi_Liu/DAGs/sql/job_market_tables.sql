-- Main Table: job listings --
CREATE TABLE jobs (
    source_api STRING,
    job_title STRING,
    job_description STRING,
    job_url STRING,
    posted_date TIMESTAMP,
    company_name STRING,
    job_category STRING,
    job_type STRING,
    salary STRUCT<
        min FLOAT64,
        max FLOAT64,
    >,
)
PARTITION BY DATE(posted_date)
CLUSTER BY source_api, job_type;


-- -- Aggregate Table: job postings statistics --
-- CREATE TABLE job_postings_statistics (
--     date DATE,
--     job_category STRING,
--     company_name STRING,
--     job_type STRING,
--     posting_count INT64,
--     avg_min_salary FLOAT64,
--     avg_max_salary FLOAT64,
--     salary_currency STRING,
-- )
-- PARTITION BY date
-- CLUSTER BY job_category, job_type;