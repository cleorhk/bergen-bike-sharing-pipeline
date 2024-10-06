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
    CASE
        WHEN precipitation > 0 THEN 'Rainy'
        ELSE 'Dry'
    END AS precipitation_condition,
    CASE
        WHEN wind_speed >= 10 THEN 'Windy'
        ELSE 'Calm'
    END AS wind_condition,
    COUNT(*) AS total_trips,
    AVG(duration / 60) AS avg_duration_minutes
FROM {{ source('bergen_bike', 'bergen_bike_data') }}
GROUP BY temperature_category, precipitation_condition, wind_condition
ORDER BY total_trips DESC
