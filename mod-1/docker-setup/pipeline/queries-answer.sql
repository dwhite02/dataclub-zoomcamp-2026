-- Counting short trips
SELECT
    COUNT(*) AS total_trips
FROM green_taxi_trips
WHERE trip_distance <= 1
  AND lpep_pickup_datetime >= TIMESTAMP '2025-11-01'
  AND lpep_pickup_datetime <  TIMESTAMP '2025-12-01';

-- Longest Trip Day
SELECT CAST(lpep_pickup_datetime as Date)
FROM green_taxi_trips
WHERE trip_distance < 100
ORDER BY trip_distance DESC
LIMIT 1;

-- Biggest pickup zone

SELECT
    z."Zone",
    COUNT(1)
FROM
    zones z,
    green_taxi_trips gtt
WHERE
    gtt."PULocationID" = z."LocationID"
	AND CAST(lpep_pickup_datetime AS DATE ) = DATE('2025-11-18')
GROUP BY
    z."Zone"
ORDER BY
    COUNT(1) DESC
LIMIT 1;

-- LArgetest tip

SELECT
    zdo."Zone" AS dropoff_zone,
    gtt.tip_amount
FROM green_taxi_trips gtt
JOIN zones zpu
    ON gtt."PULocationID" = zpu."LocationID"
JOIN zones zdo
    ON gtt."DOLocationID" = zdo."LocationID"
WHERE
    zpu."Zone" = 'East Harlem North'
    AND gtt.lpep_pickup_datetime >= TIMESTAMP '2025-11-01'
    AND gtt.lpep_pickup_datetime <  TIMESTAMP '2025-12-01'
ORDER BY
    gtt.tip_amount DESC
LIMIT 1;