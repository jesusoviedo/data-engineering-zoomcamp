{{
    config(
        materialized='table'
    )
}}

WITH tripdata_revenue AS (
    SELECT 
        SUM(total_amount) AS revenue_quarter,
        service_type,
        year,
        quarter
    FROM {{ ref('fct_taxi_trips') }}
    WHERE year BETWEEN {{ var("year_start", 2000) }} AND {{ var("year_end", 2025) }}
    GROUP BY service_type, year, quarter
),
tripdata_revenue_quarter AS (
    SELECT 
        service_type,
        CONCAT(year, '/Q', quarter) AS year_quarter,
        revenue_quarter AS current_revenue,
        LAG(revenue_quarter, 4, 0) OVER(PARTITION BY service_type ORDER BY year, quarter) AS previous_revenue,
        (revenue_quarter - LAG(revenue_quarter, 4, 0) OVER(PARTITION BY service_type ORDER BY year, quarter)) * 100 AS diff_current_previous_revenue
    FROM tripdata_revenue
),
tripdata_revenue_quarter_growth AS (
    SELECT
        service_type,
        year_quarter,
        --current_revenue,
        --previous_revenue,
        --diff_current_previous_revenue
        ROUND({{dbt_utils.safe_divide('diff_current_previous_revenue' , 'previous_revenue')}}, 2) as growth
        --ROUND((current_revenue - previous_revenue) * 100 / NULLIF(previous_revenue, 0), 2) AS growth
    FROM tripdata_revenue_quarter
)
SELECT 
    service_type,
    year_quarter,
    CONCAT( FORMAT("%.2f", COALESCE(growth, 0)) , '%') AS growth
FROM tripdata_revenue_quarter_growth
ORDER BY service_type, year_quarter
