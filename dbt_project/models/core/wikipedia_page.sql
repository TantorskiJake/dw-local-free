{{
  config(
    materialized='table',
    schema='core'
  )
}}

SELECT * FROM core.wikipedia_page WHERE is_current = true

