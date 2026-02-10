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

## Question 1. Counting records

What is count of records for the 2024 Yellow Taxi Data?

- 65,623
- 840,402
- 20,332,093
- 85,431,289

```sql
Select COUNT(*) from `kestra-sandbox-486317.zoomcamp.external_yellow_tripdata_24`;
```

## Question 2. Data read estimation

Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.

What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 155.12 MB for the Materialized Table
- 2.14 GB for the External Table and 0MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table

```sql
SELECT DISTINCT(PULocationID)
FROM kestra-sandbox-486317.zoomcamp.yellow_tripdata_24;

SELECT DISTINCT(PULocationID)
FROM kestra-sandbox-486317.zoomcamp.external_yellow_tripdata_24;
```

## Question 3. Understanding columnar storage

Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table.

Why are the estimated number of Bytes different?

- BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.
- BigQuery duplicates data across multiple storage partitions, so selecting two columns instead of one requires scanning the table twice, doubling the estimated bytes processed.
- BigQuery automatically caches the first queried column, so adding a second column increases processing time but does not affect the estimated bytes scanned.
- When selecting multiple columns, BigQuery performs an implicit join operation between them, increasing the estimated bytes processed

```sql
SELECT
  PULocationID
FROM kestra-sandbox-486317.zoomcamp.yellow_tripdata_24;

SELECT
  PULocationID,
  DOLocationID
FROM kestra-sandbox-486317.zoomcamp.yellow_tripdata_24;
```

## Question 4. Counting zero fare trips

How many records have a fare_amount of 0?

- 128,210
- 546,578
- 20,188,016
- 8,333

```sql
Select COUNT(*) from `kestra-sandbox-486317.zoomcamp.external_yellow_tripdata_24` where fare_amount = 0;

```

## Question 5. Partitioning and clustering

What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

- Partition by tpep_dropoff_datetime and Cluster on VendorID
- Cluster on by tpep_dropoff_datetime and Cluster on VendorID
- Cluster on tpep_dropoff_datetime Partition by VendorID
- Partition by tpep_dropoff_datetime and Partition by VendorID

```sql
CREATE OR REPLACE TABLE `kestra-sandbox-486317.zoomcamp.yellow_tripdata_24_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT *
FROM `kestra-sandbox-486317.zoomcamp.yellow_tripdata_24`;
```

## Question 6. Partition benefits

Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?

Choose the answer which most closely matches.

- 12.47 MB for non-partitioned table and 326.42 MB for the partitioned table
- 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
- 5.87 MB for non-partitioned table and 0 MB for the partitioned table
- 310.31 MB for non-partitioned table and 285.64 MB for the partitioned table

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


## Question 9. Understanding table scans

Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

```sql
SELECT COUNT(*)
FROM `kestra-sandbox-486317.zoomcamp.yellow_tripdata_24`;
```