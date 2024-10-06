{{ config(
    materialized='view'
) }}

WITH base_data AS (
    SELECT
        start_time,
        end_time,
        duration,
        start_station_name,
        end_station_name,
        temperature,
        wind_speed,
        precipitation,
        weather,
        season,
        is_holiday,
        is_weekend
    FROM {{ source('bergen_bike', 'bergen_bike_data') }}
)

SELECT
    start_time,
    end_time,
    duration / 60 AS duration_minutes,  -- Convert trip duration from seconds to minutes
    start_station_name,
    end_station_name,
    temperature,
    wind_speed,
    precipitation,
    CASE
        WHEN temperature <= 0 THEN 'Freezing'
        WHEN temperature <= 10 THEN 'Cold'
        WHEN temperature <= 20 THEN 'Mild'
        ELSE 'Warm'
    END AS temperature_category,
    CASE
        WHEN wind_speed >= 10 THEN 'Windy'
        ELSE 'Calm'
    END AS wind_condition,
    CASE
        WHEN precipitation > 0 THEN 'Rainy'
        ELSE 'Dry'
    END AS precipitation_condition,
    season,
    is_holiday,
    is_weekend
FROM base_data
