{{
    config(
        materialized='view'
    )
}}


with tripdata as 
(
SELECT *
FROM {{ source('staging', 'fhv_tripdata') }}
WHERE dispatching_base_num IS NOT NULL
)
SELECT 
    dispatching_base_num,
    CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(dropOff_datetime AS TIMESTAMP) AS dropOff_datetime,
    PUlocationID,
    DOlocationID,
    SR_Flag,
    affiliated_base_number
FROM tripdata



