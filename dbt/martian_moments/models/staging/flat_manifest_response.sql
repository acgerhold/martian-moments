{{ config(
    materialized='incremental',
    unique_key='rover_name',
    incremental_strategy='merge',
    cluster_by=['rover_name'],
    tags='flatten'
) }}

SELECT
    manifest.value:name::string as rover_name,
    manifest.value:status::string as status,
    manifest.value:max_sol::int as max_sol,
    manifest.value:max_date::date as max_date,
    manifest.value:total_photos::int as total_photos,
    manifest.value:launch_date::date as launch_date,
    manifest.value:landing_date::date as landing_date,
    manifest.value:photos::variant as photos,
    rmr.ingestion_date as ingestion_date    
FROM 
    {{ source('MARS_BRONZE', 'RAW_MANIFEST_RESPONSE') }} rmr,
    LATERAL FLATTEN(input => parse_json(rmr.manifests)) as manifest
{% if is_incremental() %}
    WHERE rmr.ingestion_date > (SELECT MAX(ingestion_date) FROM {{ this }})
{% endif %}