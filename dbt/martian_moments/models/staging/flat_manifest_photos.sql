{{ config(
    materialized='incremental',
    unique_key=['rover_name', 'sol'],
    incremental_strategy='merge',
    cluster_by=['rover_name', 'sol'],
    tags='flatten-inner'
) }}
    
SELECT
    fmr.rover_name,
    sol.value:sol::int as sol,
    sol.value:earth_date::date as earth_date,
    sol.value:total_photos::int as manifest_total_photos,
    ARRAY_SIZE(sol.value:cameras) as manifest_camera_count,
    fmr.ingestion_date
FROM 
    {{ source('MARS_SILVER', 'FLAT_MANIFEST_RESPONSE') }} fmr,
    LATERAL FLATTEN(input => parse_json(fmr.photos)) as sol
{% if is_incremental() %}
    WHERE fmr.ingestion_date > (SELECT MAX(ingestion_date) FROM {{ this }})
{% endif %}