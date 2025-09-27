{{ config(
    materialized='incremental',
    unique_key=['rover_name', 'earth_date', 'sol'],
    incremental_strategy='merge',
    cluster_by=['rover_name', 'sol'],
    tags='aggregate'
) }}

WITH camera_counts AS (
    SELECT 
        fph.rover_id,
        fph.earth_date,
        fph.sol,
        dca.camera_name,
        COUNT(*) as photos_per_camera
    FROM 
        {{ source('MARS_SILVER', 'FACT_PHOTOS') }} fph
    JOIN 
        {{ source('MARS_SILVER', 'DIM_CAMERAS') }} dca ON fph.camera_id = dca.camera_id
    GROUP BY 
        fph.rover_id, 
        fph.earth_date, 
        fph.sol, 
        dca.camera_name
)
SELECT 
    dro.rover_name,
    cc.earth_date,
    cc.sol,
    COUNT(DISTINCT cc.camera_name) AS cameras_used,
    COUNT(fph.image_id) as total_photos
FROM 
    camera_counts cc
JOIN
    {{ source('MARS_SILVER', 'FACT_PHOTOS') }} fph ON cc.rover_id = fph.rover_id AND cc.sol = fph.sol
JOIN 
    {{ source('MARS_SILVER', 'DIM_ROVERS') }} dro ON cc.rover_id = dro.rover_id
GROUP BY 
    dro.rover_name, 
    cc.earth_date, 
    cc.sol