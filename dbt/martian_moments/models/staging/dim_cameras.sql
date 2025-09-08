{{ config(
    materialized='view',
    unique_keys='camera_id',
) }}

SELECT DISTINCT
    camera_id,
    rover_id,
    camera_name,
    camera_full_name
FROM {{ source('MARS_SILVER', 'FLAT_PHOTO_RESPONSE') }}
