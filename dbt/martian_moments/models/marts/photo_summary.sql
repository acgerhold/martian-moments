{{ config(
    materialized='incremental',
    unique_key=['rover_name', 'max_sol'],
    incremental_strategy='append',
    cluster_by=['rover_name', 'max_sol'],
    tags='aggregate'
) }}

SELECT 
    fmr.rover_name,
    fmr.status,
    fmr.launch_date,
    fmr.landing_date,
    fmr.max_sol,
    fmr.max_date,
    fmr.total_photos
FROM 
    {{ source('MARS_SILVER', 'FLAT_MANIFEST_RESPONSE') }} fmr
{% if is_incremental() %}
    WHERE fmr.ingestion_date > (SELECT MAX(ingestion_date) FROM {{ this }})
{% endif %}
