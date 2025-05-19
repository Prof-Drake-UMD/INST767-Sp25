--1. top manufacturers causing the most SERIOUS adverse events for each month 

WITH monthly_counts AS (
  SELECT
    labeler_name AS manufacturer,
    FORMAT_DATE('%Y-%m', receivedate) AS report_month,
    COUNT(DISTINCT safetyreportid) AS report_count
  FROM `inst767-openfda-pg.openfda.combined_view`
  WHERE labeler_name IS NOT NULL
  GROUP BY manufacturer, report_month
),

ranked_manufacturers AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY report_month ORDER BY report_count DESC) AS rank_in_month
  FROM monthly_counts
)

SELECT *
FROM ranked_manufacturers
WHERE rank_in_month <= 3
ORDER BY report_month, rank_in_month;

--------------------------------------------------------------------------------------------------------------------------------

-- 2. Patient Demographics (Age/Sex) in Serious Events Across Top Countries

SELECT
  occurcountry,
  patientagegroup,
  patientsex,
  COUNT(DISTINCT safetyreportid) AS serious_event_count
FROM `inst767-openfda-pg.openfda.combined_view`
WHERE serious = 1 AND occurcountry is not null and patientagegroup is not null
GROUP BY occurcountry, patientagegroup, patientsex
ORDER BY serious_event_count DESC
LIMIT 50;

-- 3. Drugs and their indication(usage) with High Death Event Rate AND Involved in Recalls
-- This query surfaces drugs with significant death-related adverse events and prior recalls high risk(Class I), identifying high-risk medications .

WITH drug_stats AS (
  SELECT
    medicinalproduct,
    drugindication,
    COUNT(DISTINCT safetyreportid) AS total_events,
    Count(DISTINCT CASE WHEN seriousnessdeath = 1 THEN safetyreportid ELSE Null END) AS death_events,
    COUNT(DISTINCT termination_date) AS recall_events
  FROM `inst767-openfda-pg.openfda.combined_view`
  WHERE classification = 'Class I'
  GROUP BY medicinalproduct, drugindication
)
SELECT *,
  ROUND(death_events * 100.0 / total_events, 3) AS death_event_rate
FROM drug_stats
WHERE death_events >= 10 AND recall_events > 0
ORDER BY death_event_rate DESC, death_events DESC
LIMIT 15;




-- 4.  Most frequently reported adverse reactions (ARs) types for each drug indication
--  This can surface safety concerns within specific treatment contexts (e.g., reactions during cancer vs. diabetes treatment).

WITH reaction_counts AS (
  SELECT
    events.drugindication AS drug_use_category,
    reaction.reactionmeddrapt AS reported_reaction,
    COUNT(DISTINCT events.safetyreportid) AS report_count
  FROM `inst767-openfda-pg.openfda.combined_view` AS events,
    UNNEST(events.reaction) AS reaction
  WHERE events.drugindication IS NOT NULL
    AND reaction.reactionmeddrapt IS NOT NULL
    AND events.drugindication != 'Product used for unknown indication' 
  GROUP BY drug_use_category, reported_reaction
),

ranked_reactions AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY drug_use_category ORDER BY report_count DESC) AS rn
  FROM reaction_counts
)

SELECT
  drug_use_category,
  reported_reaction,
  report_count
FROM ranked_reactions
WHERE rn = 1
ORDER BY report_count DESC;


--------------------------------------------------------------------------------------------------------------------------------

--5. Identification of Top Manufacturers and Drugs Involved in Serious Off-Label Use Cases
-- Drugs with high counts under “off label use” could signal non-compliant marketing or need for prescriber education.
SELECT
  labeler_name,
  medicinalproduct AS drug_name,
  COUNT(DISTINCT safetyreportid) AS report_count
FROM `inst767-openfda-pg.openfda.combined_view` AS events
WHERE LOWER(drugindication) LIKE '%off label use%'
  AND labeler_name IS NOT NULL
  AND (
    seriousnesshospitalization = 1 OR
    seriousnesslifethreatening = 1 OR
    seriousnessdisabling = 1
  )
GROUP BY
  labeler_name,
  medicinalproduct,
  patientagegroup,
  drugindication
ORDER BY report_count desc

