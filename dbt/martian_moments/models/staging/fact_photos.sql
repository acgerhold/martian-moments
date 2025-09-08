{{ config(
    materialized='view',
    unique_keys='image_id',
) }}

SELECT DISTINCT
    image_id,
    camera_id,
    sol,
    rover_id,
    img_src
FROM {{ source('MARS_SILVER', 'FLAT_PHOTO_RESPONSE') }}
