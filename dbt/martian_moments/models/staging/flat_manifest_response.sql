{{ config(
    materialized='view',
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
    manifest.value:photos::variant as photos    
FROM 
    {{ source('MARS_BRONZE', 'RAW_MANIFEST_RESPONSE') }} rpr,
LATERAL FLATTEN(input => parse_json(rpr.manifests)) as manifest