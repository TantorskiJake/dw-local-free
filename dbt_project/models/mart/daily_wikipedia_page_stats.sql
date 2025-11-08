{{
  config(
    materialized='materialized_view',
    unique_key="revision_date || '_' || page_title || '_' || page_language"
  )
}}

SELECT 
    wp.page_title,
    wp.page_language,
    DATE(r.revision_timestamp) AS revision_date,
    COUNT(*) AS revision_count,
    COUNT(DISTINCT r.revision_user) AS unique_editors,
    SUM(r.content_len) AS total_bytes_changed,
    AVG(r.content_len) AS avg_bytes_per_revision,
    MIN(r.content_len) AS min_revision_size,
    MAX(r.content_len) AS max_revision_size
FROM {{ ref('revision') }} r
JOIN {{ ref('wikipedia_page') }} wp ON r.page_id = wp.page_id
WHERE wp.is_current = true
GROUP BY wp.page_title, wp.page_language, DATE(r.revision_timestamp)

