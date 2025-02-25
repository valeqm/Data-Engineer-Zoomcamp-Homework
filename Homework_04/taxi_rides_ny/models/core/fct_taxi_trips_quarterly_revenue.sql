{{ config(
    materialized='table'
) }}

WITH trips AS (
    SELECT 
        service_type,
        total_amount,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(QUARTER FROM pickup_datetime) AS quarter
    FROM {{ ref('fact_trips') }}
    WHERE EXTRACT(YEAR FROM pickup_datetime) IN (2019, 2020)
),

quarterly_revenue AS (
    SELECT
        service_type,
        year,
        quarter,
        CONCAT(year, '/Q', quarter) AS year_quarter, 
        SUM(total_amount) AS total_revenue
    FROM trips
    GROUP BY service_type, year, quarter
),

quarterly_revenue_yoy AS (
    SELECT
        q1.service_type,
        q1.year,
        q1.quarter,
        q1.year_quarter,
        q1.total_revenue,
        q2.total_revenue AS last_year_revenue,
        CASE 
            WHEN q2.total_revenue IS NOT NULL AND q2.total_revenue > 0 
            THEN ROUND(((q1.total_revenue - q2.total_revenue) / q2.total_revenue) * 100, 2)
            ELSE NULL
        END AS yoy_growth
    FROM quarterly_revenue q1
    LEFT JOIN quarterly_revenue q2
        ON q1.service_type = q2.service_type
        AND q1.year = q2.year + 1
        AND q1.quarter = q2.quarter
)

SELECT * FROM quarterly_revenue_yoy


