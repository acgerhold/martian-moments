{{ config(
    materialized='incremental',
    unique_key=['name', 'sol', 'camera'],
    incremental_strategy='merge',
    cluster_by=['name', 'sol', 'camera'],
    tags='aggregate'
) }}

SELECT 
    dr.rover_name AS name,
    fp.sol as sol,
    dc.camera_full_name AS camera,
    dc.camera_name AS camera_abv,
    COUNT(fp.image_id) AS total_photos
FROM 
    {{ source('MARS_SILVER', 'FACT_PHOTOS') }} fp
JOIN 
    {{ source('MARS_SILVER', 'DIM_ROVERS') }} dr 
        ON fp.rover_id = dr.rover_id
JOIN 
    {{ source('MARS_SILVER', 'DIM_CAMERAS') }} dc 
        ON fp.camera_id = dc.camera_id
GROUP BY
    dr.rover_name,
    fp.sol,
    dc.camera_full_name,
    dc.camera_name