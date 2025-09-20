{{ config(
    materialized='incremental',
    unique_key=['rover_name', 'earth_date', 'sol'],
    incremental_strategy='merge',
    cluster_by=['rover_name', 'sol'],
    tags='aggregate'
) }}

SELECT 
    dr.rover_name AS rover_name,
    fp.earth_date AS earth_date,
    fp.sol AS sol,
    LISTAGG(DISTINCT dc.camera_name, ', ') AS camera_names,
    COUNT(DISTINCT dc.camera_name) AS cameras_used,
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
    fp.earth_date,
    fp.sol