{{
  config(
    materialized='materialized_view',
    unique_key="observation_date || '_' || location_name"
  )
}}

SELECT 
    l.location_name,
    l.city,
    l.country,
    DATE(w.observed_at) AS observation_date,
    COUNT(*) AS observation_count,
    AVG(w.temperature_celsius) AS avg_temperature_celsius,
    MIN(w.temperature_celsius) AS min_temperature_celsius,
    MAX(w.temperature_celsius) AS max_temperature_celsius,
    AVG(w.humidity_percent) AS avg_humidity_percent,
    AVG(w.pressure_hpa) AS avg_pressure_hpa,
    AVG(w.wind_speed_mps * 3.6) AS avg_wind_speed_kmh,
    MODE() WITHIN GROUP (ORDER BY w.conditions) AS most_common_conditions
FROM {{ ref('weather') }} w
JOIN {{ ref('location') }} l ON w.location_id = l.location_id
GROUP BY l.location_name, l.city, l.country, DATE(w.observed_at)

