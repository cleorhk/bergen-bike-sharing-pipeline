{{ config(
    materialized='view'
) }}

SELECT
    EXTRACT(HOUR FROM start_time) AS hour_of_day,
    COUNT(*) AS total_trips,
    AVG(duration / 60) AS avg_duration_minutes
FROM {{ source('bergen_bike', 'bergen_bike_data') }}
GROUP BY hour_of_day
ORDER BY hour_of_day
