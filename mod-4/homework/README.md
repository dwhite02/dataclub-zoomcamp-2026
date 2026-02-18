# Question 4
```sql
SELECT
    pickup_zone as zone,
    SUM(total_amount) AS revenue
FROM `kestra-sandbox-486317.dbt_prod.fct_trips`
WHERE service_type = 'Green'
  AND EXTRACT(YEAR FROM pickup_datetime) = 2020
GROUP BY zone
ORDER BY revenue DESC
LIMIT 1;
```

# Question 5
```sql
SELECT
    SUM(total_monthly_trips) AS trips
FROM `kestra-sandbox-486317.dbt_prod.fct_monthly_zone_revenue`
WHERE service_type = 'Green'
  AND revenue_month = DATE('2019-10-01');
```

# Question 6

```sql
with source as (
    select * 
    from {{ source('trips_data_all', 'fhv_tripdata') }}
),

renamed as (
    select
        dispatching_base_num,
        pickup_datetime,
        dropoff_datetime,
        cast(PUlocationID as int64) as pickup_location_id,
        cast(DOlocationID as int64) as dropoff_location_id,
        sr_flag,
        affiliated_base_number
    from source
    where dispatching_base_num is not null
      and extract(year from pickup_datetime) = 2019
)
select * from renamed
```

```sql
SELECT COUNT(*)
FROM `kestra-sandbox-486317.dbt_prod.stg_fhv_tripdata`;

```
