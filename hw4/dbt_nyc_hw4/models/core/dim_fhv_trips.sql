{{
    config(
        materialized='table'
    )
}}

WITH fhv_tripdata AS (
    SELECT 
        *
    FROM {{ ref('stg_fhv_tripdata') }}
), 
dim_zones AS (
    SELECT 
        * 
    FROM {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
SELECT
    fhv_tripdata.tripid,
    fhv_tripdata.dispatching_base_num,
    fhv_tripdata.affiliated_base_number,
    fhv_tripdata.pickup_datetime,
    fhv_tripdata.dropoff_datetime,
    fhv_tripdata.pickup_locationid,
    pickup_zone.borough AS pickup_borough, 
    pickup_zone.zone AS pickup_zone, 
    fhv_tripdata.dropoff_locationid,
    dropoff_zone.borough AS dropoff_borough, 
    dropoff_zone.zone AS dropoff_zone,  
    fhv_tripdata.sr_flag,
    EXTRACT(YEAR from fhv_tripdata.pickup_datetime) AS year,
    EXTRACT(MONTH from fhv_tripdata.pickup_datetime) AS month
FROM fhv_tripdata
INNER JOIN dim_zones AS pickup_zone
ON fhv_tripdata.pickup_locationid = pickup_zone.locationid
INNER JOIN dim_zones AS dropoff_zone
ON fhv_tripdata.dropoff_locationid = dropoff_zone.locationid