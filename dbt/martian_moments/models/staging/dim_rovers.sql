{{ config(
    materialized='view',
    unique_keys='rover_id',
    tags='normalize'
) }}

SELECT DISTINCT
    rover_id,
    rover_name,
    rover_status,
    launch_date,
    landing_date
FROM 
    {{ source('MARS_SILVER', 'FLAT_PHOTO_RESPONSE') }}
