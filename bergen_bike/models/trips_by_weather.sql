{{ config(
    materialized='table'
) }}

SELECT
    weather,
    COUNT(*) AS trip_count
FROM {{ source('bergen_bike', 'bergen_bike_data') }}
GROUP BY weather
ORDER BY trip_count DESC
