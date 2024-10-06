{{ config(
    materialized='table'
) }}

SELECT
    start_station_name,
    AVG(duration / 60) AS avg_duration_minutes
FROM {{ source('bergen_bike', 'bergen_bike_data') }}
GROUP BY start_station_name
