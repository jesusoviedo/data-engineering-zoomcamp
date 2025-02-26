{{
    config(
        materialized='table'
    )
}}

WITH tripdata_filter AS (
    SELECT 
        service_type,
        year,
        month,
        fare_amount
    FROM {{ ref('fct_taxi_trips') }}
    WHERE fare_amount > 0
    AND trip_distance > 0
    AND lower(payment_type_description) in (lower('Cash'), lower('Credit Card'))
    AND year BETWEEN {{ var("year_start", 2000) }} AND {{ var("year_end", 2025) }}
),
tripdata_percentile AS (
    SELECT DISTINCT
        service_type,
        year,
        month,
        PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, year, month) AS p97_fare_amount,
        PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, year, month) AS p95_fare_amount,
        PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, year, month) AS p90_fare_amount
    FROM tripdata_filter
)
SELECT 
    service_type,
    year,
    month,
    FORMAT("%.2f",ROUND(p90_fare_amount, 1)) AS p90_fare_amount,
    FORMAT("%.2f",ROUND(p95_fare_amount, 1)) AS p95_fare_amount,
    FORMAT("%.2f",ROUND(p97_fare_amount, 1)) AS p97_fare_amount
FROM tripdata_percentile
ORDER BY year, month, service_type