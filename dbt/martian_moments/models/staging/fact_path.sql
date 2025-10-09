{{ config(
    materialized='view',
    tags='normalize'
) }}

SELECT
    CASE rover_name
        WHEN 'Perseverance' THEN 8
        WHEN 'Spirit' THEN 7
        WHEN 'Opportunity' THEN 6
        WHEN 'Curiosity' THEN 5
        ELSE 0
    END AS rover_id,
    sol,
    from_rmc,
    to_rmc,
    COALESCE(fcr.length, 0) AS length,
    sclk_start,
    sclk_end,
    CASE 
        WHEN length IS NULL OR length = 0 
            THEN 'Stationary Day'
        WHEN length < 5 
            THEN 'Minimal Movement'
        WHEN length < 20 
            THEN 'Short Travel'
        ELSE 
            'Long Travel'
    END as day_type,
    ingestion_date as ingestion_date
FROM 
    {{ source('MARS_SILVER', 'FLAT_COORDINATE_RESPONSE') }} fcr