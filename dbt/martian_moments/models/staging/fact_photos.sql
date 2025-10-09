{{ config(
    materialized='view',
    tags='normalize'
) }}

SELECT
    image_id,
    camera_id AS nasa_camera_id,
    camera_name,
    sol,
    rover_id,
    earth_date,
    img_src,
    ingestion_date
FROM 
    {{ source('MARS_SILVER', 'FLAT_PHOTO_RESPONSE') }}