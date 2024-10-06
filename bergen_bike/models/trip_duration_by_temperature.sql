{{ config(
    materialized='view'
) }}

SELECT
    CASE
        WHEN temperature <= 0 THEN 'Freezing'
        WHEN temperature <= 10 THEN 'Cold'
        WHEN temperature <= 20 THEN 'Mild'
        ELSE 'Warm'
    END AS temperature_category,
    AVG(duration / 60) AS avg_duration_minutes
FROM {{ source('bergen_bike', 'bergen_bike_data') }}
GROUP BY temperature_category
ORDER BY avg_duration_minutes DESC
