-- 1. Create Athena external table using Parquet data stored in S3
CREATE EXTERNAL TABLE accounts_parquet_v5 (
    bank_name string,
    bank_id bigint,
    account_number string,
    entity_id string,
    entity_name string,
    account_pk string
)
PARTITIONED BY (bank_group int)
STORED AS PARQUET
LOCATION 's3://felipe-data-engineering-proyecto/processed/accounts_parquet_v5/';

-- 2. Load partitions from S3
MSCK REPAIR TABLE accounts_parquet_v5;

-- 3. Inspect table data
SELECT *
FROM accounts_parquet_v5
LIMIT 10;

-- 4. Export dataset to CSV for Looker Studio dashboard
CREATE TABLE accounts_csv_export
WITH (
    format = 'TEXTFILE',
    field_delimiter = ',',
    external_location = 's3://felipe-data-engineering-proyecto/export/accounts/'
) AS
SELECT *
FROM accounts_parquet_latest;