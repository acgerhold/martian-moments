{{ config(
    materialized='view',
    tags='normalize'
) }}

SELECT
    sol,
    from_rmc,
    to_rmc,
    length,
    sclk_start,
    sclk_end,
    CASE 
        WHEN length IS NULL OR length = 0 THEN 'Stationary Day'
        WHEN length < 5 THEN 'Minimal Movement'
        WHEN length < 20 THEN 'Short Travel'
        ELSE 'Long Travel'
    END as day_type
FROM {{ source('MARS_SILVER', 'FLAT_COORDINATE_RESPONSE') }}