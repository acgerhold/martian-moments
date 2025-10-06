{{ config(
    materialized='incremental',
    unique_key=['rover_name', 'earth_date', 'sol'],
    incremental_strategy='append',
    cluster_by=['rover_name', 'earth_date', 'sol'],
    tags='aggregate'
) }}

SELECT 
    dro.rover_name,
    fph.earth_date,
    fph.sol,
    MAX(fph.ingestion_date) as ingestion_date,
    COUNT(DISTINCT fph.camera_id) AS cameras_used,
    COUNT(fph.image_id) AS total_photos
FROM 
    {{ source('MARS_SILVER', 'FACT_PHOTOS') }} fph
JOIN 
    {{ source('MARS_SILVER', 'DIM_ROVERS') }} dro ON fph.rover_id = dro.rover_id
{% if is_incremental() %}
WHERE 
    fph.ingestion_date > (SELECT MAX(ingestion_date) FROM {{ this }})
{% endif %}
GROUP BY 
    dro.rover_name, 
    fph.earth_date, 
    fph.sol
