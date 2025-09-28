{{ config(
    materialized='view',
    tags='normalize'
) }}

SELECT
    rover_id,
    rover_name,
    rover_status,
    launch_date,
    landing_date
FROM 
    {{ source('MARS_SILVER', 'FLAT_PHOTO_RESPONSE') }}
GROUP BY
    rover_id,
    rover_name,
    rover_status,
    launch_date,
    landing_date
