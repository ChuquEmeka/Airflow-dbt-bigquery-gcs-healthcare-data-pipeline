-- Creating patient_data external table (CSV format)
CREATE OR REPLACE EXTERNAL TABLE `healthcare-data-project-442109.dev_healthcare_data.patient_data_external`
OPTIONS (
  format = 'CSV',
  uris = ['gs://healthcare-data-bucket-emeka/dev/patient_data.csv'],
  skip_leading_rows = 1
);

-- Creating ehr_data external table (JSON format)
CREATE OR REPLACE EXTERNAL TABLE `healthcare-data-project-442109.dev_healthcare_data.ehr_data_external`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://healthcare-data-bucket-emeka/dev/ehr_data.json']
);

-- Creating claims_data external table (Parquet format with explicit schema)
CREATE OR REPLACE EXTERNAL TABLE `healthcare-data-project-442109.dev_healthcare_data.claims_data_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://healthcare-data-bucket-emeka/dev/claims_data.parquet']
);
