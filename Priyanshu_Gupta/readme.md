# Overview

This project integrates multiple API data sources to retrieve and analyze drug-related information from OpenFDA apis. The APIs used in this project include:

OpenFDA Drug Events API

Product Labeling API

NDC Directory API

## API Data Sources and Expected Data

### 1. OpenFDA Drug Events API

#### -- Endpoint: 
https://api.fda.gov/drug/event.json

#### -- Inputs:
search (e.g., search=receivedate:20240101&patient.drug.medicinalproduct:"aspirin")

limit (number of records to return)

skip (number of records to skip for pagination workflow as there is limit of 1000 records per request)

#### -- Expected Output:
Adverse event reports associated with specific drugs, including patient reactions, drug names, and outcomes.

### 2. Product Labeling API
#### -- Endpoint: 
https://api.fda.gov/drug/label.json

#### -- Inputs:
search (e.g., search=openfda.brand_name:"Tylenol")

limit (number of records to return)

#### -- Expected Output:
Official drug labeling information, including dosage, side effects, indications, and warnings.

### 3. NDC Directory API

#### - Endpoint: 
https://api.fda.gov/drug/ndc.json

#### -- Inputs:
search (e.g., search=brand_name:"Advil")

limit (number of records to return)

#### -- Expected Output:

National Drug Code (NDC) information, including active ingredients, manufacturer details, and marketing status.
