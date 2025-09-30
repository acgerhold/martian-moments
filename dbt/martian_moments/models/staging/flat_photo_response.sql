{{ config(
    materialized='incremental',
    unique_key='image_id',
    incremental_strategy='append',
    cluster_by=['rover_id', 'sol', 'earth_date'],
    tags='flatten'
) }}

SELECT 
    photo.value:rover.id::int as rover_id,
    photo.value:rover.name::string as rover_name,
    photo.value:rover.landing_date::date as landing_date,
    photo.value:rover.launch_date::date as launch_date,
    photo.value:rover.status::string as rover_status,
    photo.value:sol::int as sol,
    photo.value:earth_date::date as earth_date,
    photo.value:camera.id::int as camera_id,
    photo.value:camera.name::string as camera_name,
    photo.value:camera.full_name::string as camera_full_name,
    photo.value:img_src::string as img_src,
    photo.value:id::int as image_id,
    rpr.filename as filename,
    rpr.ingestion_date as ingestion_date   
FROM 
    {{ source('MARS_BRONZE', 'RAW_PHOTO_RESPONSE') }} rpr,
    LATERAL FLATTEN(input => parse_json(rpr.photos)) AS photo
{% if is_incremental() %}
    WHERE rpr.ingestion_date > (SELECT MAX(ingestion_date) FROM {{ this }})
{% endif %}
