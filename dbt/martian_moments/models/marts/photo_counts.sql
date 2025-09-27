{{ config(
    materialized='incremental',
    unique_key=['name', 'sol', 'camera'],
    incremental_strategy='merge',
    cluster_by=['name', 'sol', 'camera'],
    tags='aggregate'
) }}

SELECT 
    dro.rover_name AS name,
    fph.sol AS sol,
    dca.camera_full_name AS camera,
    dca.camera_name AS camera_abv,
    COUNT(fph.image_id) AS total_photos
FROM 
    {{ source('MARS_SILVER', 'FACT_PHOTOS') }} fph
JOIN 
    {{ source('MARS_SILVER', 'DIM_ROVERS') }} dro ON fph.rover_id = dro.rover_id
JOIN 
    {{ source('MARS_SILVER', 'DIM_CAMERAS') }} dca ON fph.camera_id = dca.camera_id
GROUP BY
    dro.rover_name,
    fph.sol,
    dca.camera_full_name,
    dca.camera_name
