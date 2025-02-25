{{ config(materialized='table') }}

WITH trip_durations AS (
    SELECT 
        *,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration
    FROM {{ ref('dim_fhv_trips') }}

)

SELECT 
    *,
    PERCENTILE_CONT(trip_duration, 0.90) 
        OVER (PARTITION BY year, month, PUlocationID, DOlocationID) AS p90_trip_duration
FROM trip_durations

