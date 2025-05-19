CREATE TABLE `inst767-openfda-pg.openfda.recall-enforcements` (
  product_ndc STRING,
  classification STRING,
  status STRING,
  state STRING,
  country STRING,
  recalling_firm STRING,
  reason_for_recall STRING,
  recall_initiation_date DATETIME,
  termination_date DATETIME
);
