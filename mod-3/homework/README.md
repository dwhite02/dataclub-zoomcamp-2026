# dataclub-zoomcamp-2026
Data TalkData Engineering Zoomcamp 2026

## Loading the data

Using Kestra Flow
```
id: gcp_taxi_parquet
namespace: zoomcamp

inputs:
  - id: taxi
    type: SELECT
    values: [yellow, green]
    defaults: yellow

  - id: year
    type: SELECT
    values: ["2024"]
    defaults: "2024"

  - id: months
    type: MULTISELECT
    values: ["01","02","03","04","05","06","07","08","09","10","11","12"]
    defaults: ["01","02","03","04","05","06"]

variables:
  bucketName: "{{ kv('GCP_BUCKET_NAME') }}"
  baseUrl: "https://d37ci6vzurychx.cloudfront.net/trip-data/{{ inputs.taxi }}_tripdata_{{ inputs.year }}-"

tasks:
  - id: create_bucket
    type: io.kestra.plugin.gcp.gcs.CreateBucket
    name: "{{ render(vars.bucketName) }}"
    ifExists: SKIP

  - id: download_and_upload
    type: io.kestra.plugin.core.flow.ForEach
    values: "{{ inputs.months }}"
    concurrencyLimit: 4
    tasks:
      - id: download
        type: io.kestra.plugin.core.http.Download
        uri: "{{ render(vars.baseUrl) }}{{ taskrun.value }}.parquet"
        saveAs: "{{ inputs.taxi }}_tripdata_{{ inputs.year }}-{{ taskrun.value }}.parquet"

      - id: upload
        type: io.kestra.plugin.gcp.gcs.Upload
        from: "{{ currentEachOutput(outputs.download).uri }}"
        to: "gs://{{ render(vars.bucketName) }}/{{ inputs.taxi }}_tripdata_{{ inputs.year }}-{{ taskrun.value }}.parquet"

pluginDefaults:
  - type: io.kestra.plugin.gcp
    values:
      serviceAccount: "{{ secret('GCP_CREDS') }}"
      projectId: "{{ kv('GCP_PROJECT_ID') }}"
```

## BigQuery Setup

```sql
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `kestra-sandbox-486317.zoomcamp.external_yellow_tripdata_24`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://kestra-zoomcamp-dw-demo/yellow_tripdata_2024-*.parquet']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE kestra-sandbox-486317.zoomcamp.yellow_tripdata_24 AS
SELECT * FROM kestra-sandbox-486317.zoomcamp.external_yellow_tripdata_24;

```

## Question 1

```sql
Select COUNT(*) from `kestra-sandbox-486317.zoomcamp.external_yellow_tripdata_24`;
```

## Question 2
```sql
SELECT DISTINCT(PULocationID)
FROM kestra-sandbox-486317.zoomcamp.yellow_tripdata_24;

SELECT DISTINCT(PULocationID)
FROM kestra-sandbox-486317.zoomcamp.external_yellow_tripdata_24;
```

## Question 3
```sql
SELECT
  PULocationID
FROM kestra-sandbox-486317.zoomcamp.yellow_tripdata_24;

SELECT
  PULocationID,
  DOLocationID
FROM kestra-sandbox-486317.zoomcamp.yellow_tripdata_24;
```

## Question 4

```sql
Select COUNT(*) from `kestra-sandbox-486317.zoomcamp.external_yellow_tripdata_24` where fare_amount = 0;

```

## Question 5

```sql
CREATE OR REPLACE TABLE `kestra-sandbox-486317.zoomcamp.yellow_tripdata_24_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT *
FROM `kestra-sandbox-486317.zoomcamp.yellow_tripdata_24`;
```

## Question 6

```sql
SELECT DISTINCT VendorID
FROM `kestra-sandbox-486317.zoomcamp.yellow_tripdata_24`
WHERE tpep_dropoff_datetime >= TIMESTAMP('2024-03-01')
  AND tpep_dropoff_datetime <  TIMESTAMP('2024-03-16');

SELECT DISTINCT VendorID
FROM `kestra-sandbox-486317.zoomcamp.yellow_tripdata_24_optimized`
WHERE tpep_dropoff_datetime >= TIMESTAMP('2024-03-01')
  AND tpep_dropoff_datetime <  TIMESTAMP('2024-03-16');
```


## Question 9

```sql
SELECT COUNT(*)
FROM `kestra-sandbox-486317.zoomcamp.yellow_tripdata_24`;
```