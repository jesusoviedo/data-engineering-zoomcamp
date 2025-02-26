{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select 
    *,
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
  from {{ source('staging','fhv_data') }}
  where dispatching_base_num is not null
)
select 
    tripid,
    --string
    dispatching_base_num,
    affiliated_base_number,
    --timestamp
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(drop_off_datetime as timestamp) as dropoff_datetime,
    --integer
    {{ dbt.safe_cast("p_ulocation_id", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("d_olocation_id", api.Column.translate_type("integer")) }} as dropoff_locationid,
    {{ dbt.safe_cast("sr_flag", api.Column.translate_type("integer")) }} as sr_flag
FROM tripdata
-- dbt build --select <model_name> --vars '{'is_test_run': 'true'}'
{% if var('is_test_run', default=false) %}
limit 100
{% endif %}