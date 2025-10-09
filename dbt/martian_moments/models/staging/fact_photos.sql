{{ config(
    materialized='view',
    tags='normalize'
) }}

SELECT
    image_id,
    camera_id,
    sol,
    rover_id,
    earth_date,
    img_src,
    ingestion_date
FROM 
    {{ source('MARS_SILVER', 'FLAT_PHOTO_RESPONSE') }}