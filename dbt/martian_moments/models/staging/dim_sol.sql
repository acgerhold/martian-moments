{{ config(
    materialized='view',
    unique_keys='sol',
) }}

SELECT DISTINCT
    sol,
    rover_id,
    earth_date
FROM {{ source('MARS_SILVER', 'FLAT_PHOTO_RESPONSE') }}
