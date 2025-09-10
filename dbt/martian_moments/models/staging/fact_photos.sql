{{ config(
    materialized='view',
    unique_keys='image_id',
    tags='normalize'
) }}

SELECT DISTINCT
    image_id,
    camera_id,
    sol,
    rover_id,
    earth_date,
    img_src
FROM 
    {{ source('MARS_SILVER', 'FLAT_PHOTO_RESPONSE') }}
