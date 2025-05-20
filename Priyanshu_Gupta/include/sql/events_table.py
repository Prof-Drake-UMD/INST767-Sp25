from google.cloud import bigquery
import os

import google.auth



client = bigquery.Client(project="inst767-openfda-pg")

print("Using project:", client.project)


schema = [
    bigquery.SchemaField("safetyreportid", "INT64"),
    bigquery.SchemaField("occurcountry", "STRING"),
    bigquery.SchemaField("receivedate", "DATE"),
    bigquery.SchemaField("serious", "INT64"),
    bigquery.SchemaField("reportercountry", "STRING"),
    bigquery.SchemaField("reporterqualification", "STRING"),
    bigquery.SchemaField("seriousnessdeath", "INT64"),
    bigquery.SchemaField("seriousnesslifethreatening", "INT64"),
    bigquery.SchemaField("seriousnesshospitalization", "INT64"),
    bigquery.SchemaField("seriousnessdisabling", "INT64"),
    bigquery.SchemaField("seriousnesscongenitalanomali", "INT64"),
    bigquery.SchemaField("patientonsetage", "INT64"),
    bigquery.SchemaField("patientagegroup", "STRING"),
    bigquery.SchemaField("patientsex", "STRING"),
    bigquery.SchemaField("patientweight", "FLOAT"),

    bigquery.SchemaField("reaction", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("reactionmeddrapt", "STRING"),
        bigquery.SchemaField("reactionoutcome", "INT64"),
        bigquery.SchemaField("reactionoutcomedesc", "STRING")
    ]),

    bigquery.SchemaField("drugsinfo", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("drugcharacterization", "INT64"),
        bigquery.SchemaField("drugcharacterizationdesc", "STRING"),
        bigquery.SchemaField("medicinalproduct", "STRING"),
        bigquery.SchemaField("product_ndc", "STRING"),
        bigquery.SchemaField("drugindication", "STRING"),
        bigquery.SchemaField("drugdosageform", "STRING"),
        bigquery.SchemaField("activesubstancename", "STRING"),
    ])
]

project="inst767-openfda-pg"
dataset_id="openfda"
table_id = "drug-adverse-events"




dataset = bigquery.Dataset(f"{project}.{dataset_id}")

#set dataset location
dataset.location = "US"  

# Create the dataset
client.create_dataset(dataset, exists_ok=True)

# Create table
table_id = f"{project}.{dataset_id}.{table_id}"
table = bigquery.Table(table_id, schema=schema)

#create table partitions
table.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY,
    field="receivedate"  
)
table = client.create_table(table, exists_ok=True)

print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

