{{ config(materialized='table') }}

WITH fhv_trips AS (
    SELECT 
        dispatching_base_num,
        pickup_datetime,
        dropoff_datetime,
        PUlocationID,
        DOlocationID,
        SR_Flag,
        Affiliated_base_number,
        EXTRACT(YEAR FROM CAST(pickup_datetime AS TIMESTAMP)) AS year,
        EXTRACT(MONTH FROM CAST(pickup_datetime AS TIMESTAMP)) AS month
    FROM {{ ref('stg_fhv_tripdata') }}
)

SELECT 
    f.*,
    pz.borough AS pickup_borough,
    pz.zone AS pickup_zone,
    pz.service_zone AS pickup_service_zone,
    dz.borough AS dropoff_borough,
    dz.zone AS dropoff_zone,
    dz.service_zone AS dropoff_service_zone
FROM fhv_trips f
LEFT JOIN {{ ref('dim_zones') }} pz 
    ON f.PUlocationID = pz.locationid
LEFT JOIN {{ ref('dim_zones') }} dz 
    ON f.DOlocationID = dz.locationid

