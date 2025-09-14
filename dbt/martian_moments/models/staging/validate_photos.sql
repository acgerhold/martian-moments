{{ config(
    materialized='view',
    tags='validate'
) }}

SELECT
    fmr.rover_name as rover_name,
    sol.value:sol::int as sol,
    sol.value:earth_date::date as earth_date,
    LISTAGG(DISTINCT camera.value::string, ', ') as cameras_used,
    COUNT(DISTINCT camera.value::string) as camera_count,
    sol.value:total_photos::int as total_photos
FROM 
    {{ source('MARS_SILVER', 'FLAT_MANIFEST_RESPONSE') }} fmr,
LATERAL FLATTEN(input => parse_json(fmr.photos)) as sol,
LATERAL FLATTEN(input => sol.value:cameras) as camera
GROUP BY 
    fmr.rover_name,
    sol.value:sol::int,
    sol.value:earth_date::date,
    sol.value:total_photos::int