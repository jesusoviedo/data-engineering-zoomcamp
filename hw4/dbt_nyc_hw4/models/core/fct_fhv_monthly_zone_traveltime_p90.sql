{{
    config(
        materialized='table'
    )
}}

WITH fhv_tripdata AS (
    SELECT
        *,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration
    FROM {{ ref('dim_fhv_trips') }}
), 
tripdata_percentile AS (
    SELECT 
        *,
        PERCENTILE_CONT(trip_duration, 0.90) OVER (PARTITION BY year, month, pickup_locationid, dropoff_locationid) AS p90_trip_duration,
    FROM fhv_tripdata
),
ranked_trips AS (
    SELECT 
        year,
        month,
        pickup_zone,
        dropoff_zone,
        p90_trip_duration,
        DENSE_RANK() OVER (PARTITION BY pickup_zone ORDER BY p90_trip_duration DESC) AS p90_rank
    FROM tripdata_percentile
    WHERE year = {{ var("year", 2000) }}
    AND month = {{ var("month", 1) }}
    AND lower(pickup_zone) IN {{ lower_list( var('list_zone_pickup', []) ) }} 
)
SELECT DISTINCT
    year,
    month,
    pickup_zone,
    dropoff_zone,
    FORMAT("%.2f",ROUND(p90_trip_duration, 2)) AS p90_trip_duration,
    p90_rank
FROM ranked_trips
WHERE p90_rank = {{ var("rank", 1) }} 
ORDER BY pickup_zone
-- dbt build --select <model_name> --vars '{ 'year': 2019, 'month': 11, 'rank': 2, list_zone_pickup: ['Newark Airport', 'SoHo', 'Yorkville East'] }'