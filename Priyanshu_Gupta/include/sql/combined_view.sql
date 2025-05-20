CREATE OR REPLACE VIEW `inst767-openfda-pg.openfda.ndc_combined_view` AS
WITH unnested_drugs AS (
  SELECT
    safetyreportid,
    occurcountry,
    receivedate,
    serious,
    seriousnessdeath,
    seriousnesslifethreatening,
    seriousnesshospitalization,
    seriousnessdisabling,
    reporterqualification,
    reportercountry,
    drug.drugcharacterizationdesc,
    activesubstancename,
    patientonsetage as patientage,
    patientsex,
    patientagegroup,
    
    drug.medicinalproduct,
    drug.product_ndc
  FROM `inst767-openfda-pg.openfda.drug_adverse_events`,
  UNNEST(drugsinfo) AS drug
)

SELECT
  ndc.product_ndc,
  ndc.generic_name,
  ndc.brand_name,
  ndc.labeler_name,
  ndc.dosage_form,
  ndc.route,
  ndc.marketing_category,
  ndc.marketing_start_date,

  recall.classification,
  recall.status,
  recall.state,
  recall.country,
--   recall.recalling_firm,
--   recall.reason_for_recall,
--   recall.recall_initiation_date,
  recall.termination_date,
  
  events.safetyreportid,
  events.occurcountry,
  events.receivedate,
  events.serious,
  events.reportercountry,
  events.drugcharacterization,
  events.medicinalproduct,
  events.seriousnessdeath,
  events.seriousnesslifethreatening,
  events.seriousnesshospitalization,
  events.seriousnessdisabling,
  events.reporterqualification,
  events.reportercountry,
  events.drugcharacterizationdesc,
  events.activesubstancename,
  events.patientage,
  events.patientsex,
  events.patientagegroup,
FROM unnested_drugs events
LEFT JOIN `inst767-openfda-pg.openfda.ndc-directory` ndc
    ON events.product_ndc = ndc.product_ndc
LEFT JOIN  `inst767-openfda-pg.openfda.recall-enforcements` recall
    ON events.product_ndc = recall.product_ndc
