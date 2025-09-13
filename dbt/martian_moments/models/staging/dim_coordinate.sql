{{ config(
    materialized='view',
    tags='normalize'
) }}

SELECT
    (dro.rover_id * 1000000) + (sol * 100) + coord.index AS coordinate_id,
    dro.rover_id,
    coord.index AS waypoint_sequence,
    sol,
    GET(coord.value, 0) AS longitude,
    GET(coord.value, 1) AS latitude,
    GET(coord.value, 2) AS elevation
FROM 
    {{ source('MARS_SILVER', 'FLAT_COORDINATE_RESPONSE') }} fcr
CROSS JOIN 
    LATERAL FLATTEN(input => coordinates) coord
JOIN 
    {{ source('MARS_SILVER', 'DIM_ROVERS') }} dro 
        ON fcr.rover_name = dro.rover_name