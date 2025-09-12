{{ config(
    materialized='view',
    tags='normalize'
) }}

SELECT
    (sol * 10000) + coord.index AS coordinate_id,
    coord.index AS waypoint_sequence,
    sol,
    GET(coord.value, 0) AS longitude,
    GET(coord.value, 1) AS latitude,
    GET(coord.value, 2) AS elevation
FROM {{ source('MARS_SILVER', 'FLAT_COORDINATE_RESPONSE') }},
LATERAL FLATTEN(input => coordinates) coord