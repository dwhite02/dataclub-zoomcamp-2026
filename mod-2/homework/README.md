# dataclub-zoomcamp-2026
Data TalkData Engineering Zoomcamp 2026

## Question 1

Added this code below
```
- id: get_file_size
    type: io.kestra.plugin.core.storage.Size
    uri: "{{render(vars.data)}}"
```

## Question 3

```sql
SELECT Count(*) FROM `kestra-sandbox-486317.zoomcamp.yellow_tripdata` 
WHERE TIMESTAMP_TRUNC(tpep_pickup_datetime, DAY) >= TIMESTAMP("2020-01-01") 
AND TIMESTAMP_TRUNC(tpep_pickup_datetime, DAY) < TIMESTAMP("2021-01-01")
```

## Question 4
```sql
SELECT Count(*) FROM `kestra-sandbox-486317.zoomcamp.green_tripdata` 
WHERE TIMESTAMP_TRUNC(lpep_pickup_datetime, DAY) >= TIMESTAMP("2020-01-01") 
AND TIMESTAMP_TRUNC(lpep_pickup_datetime, DAY) < TIMESTAMP("2021-01-01")
```

## Question 5
```sql
SELECT Count(*) FROM `kestra-sandbox-486317.zoomcamp.yellow_tripdata` 
WHERE TIMESTAMP_TRUNC(tpep_pickup_datetime, DAY) >= TIMESTAMP("2021-03-01") 
AND TIMESTAMP_TRUNC(tpep_pickup_datetime, DAY) < TIMESTAMP("2021-04-01")
```