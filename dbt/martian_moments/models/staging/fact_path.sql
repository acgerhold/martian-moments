{{ config(
    materialized='view',
    tags='normalize'
) }}

SELECT
    (rover_id * 10000) + sol AS path_id,
    dro.rover_id,
    fcr.sol,
    fcr.from_rmc,
    fcr.to_rmc,
    fcr.length,
    fcr.sclk_start,
    fcr.sclk_end,
    CASE 
        WHEN length IS NULL OR length = 0 THEN 'Stationary Day'
        WHEN length < 5 THEN 'Minimal Movement'
        WHEN length < 20 THEN 'Short Travel'
        ELSE 'Long Travel'
    END as day_type
FROM 
    {{ source('MARS_SILVER', 'FLAT_COORDINATE_RESPONSE') }} fcr
JOIN 
    {{ source('MARS_SILVER', 'DIM_ROVERS') }} dro 
        ON fcr.rover_name = dro.rover_name