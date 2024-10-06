{{ config(
    materialized='table'
) }}

WITH start_trips AS (
    SELECT
        start_station_name AS station_name,
        COUNT(*) AS total_starts
    FROM {{ source('bergen_bike', 'bergen_bike_data') }}
    GROUP BY start_station_name
),

end_trips AS (
    SELECT
        end_station_name AS station_name,
        COUNT(*) AS total_ends
    FROM {{ source('bergen_bike', 'bergen_bike_data') }}
    GROUP BY end_station_name
)

SELECT
    COALESCE(start_trips.station_name, end_trips.station_name) AS station_name,
    COALESCE(total_starts, 0) AS total_starts,
    COALESCE(total_ends, 0) AS total_ends,
    (COALESCE(total_starts, 0) + COALESCE(total_ends, 0)) AS total_trips
FROM start_trips
FULL OUTER JOIN end_trips
ON start_trips.station_name = end_trips.station_name
ORDER BY total_trips DESC
LIMIT 10
