{{ config(
    materialized='table'
) }}

SELECT
    CASE
        WHEN is_holiday = TRUE THEN 'Holiday'
        WHEN is_weekend = TRUE THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    COUNT(*) AS total_trips,
    AVG(duration / 60) AS avg_duration_minutes
FROM {{ source('bergen_bike', 'bergen_bike_data') }}
GROUP BY day_type
ORDER BY total_trips DESC
