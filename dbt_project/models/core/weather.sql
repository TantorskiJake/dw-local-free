{{
  config(
    materialized='table',
    schema='core'
  )
}}

SELECT * FROM core.weather

