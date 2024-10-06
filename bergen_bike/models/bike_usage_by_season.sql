{{ config(
    materialized='view'
) }}

SELECT
    season,
    COUNT(*) AS total_trips,
    AVG(duration / 60) AS avg_duration_minutes
FROM {{ source('bergen_bike', 'bergen_bike_data') }}
GROUP BY season
ORDER BY season
