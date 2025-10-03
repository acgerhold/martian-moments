{{ config(
    materialized='incremental',
    unique_key=['name', 'sol', 'travel_distance'],
    incremental_strategy='append',
    cluster_by=['name', 'sol', 'travel_distance'],
    tags='aggregate'
) }}

SELECT
    dro.rover_name AS name,
    COALESCE(fpa.sol, fph.sol) AS sol,
    COALESCE(fpa.day_type, 'Stationary') AS day_type,
    COALESCE(fpa.length, 0) AS travel_distance,
    COUNT(fph.image_id) AS total_photos,
    SUM(CASE WHEN dca.camera_category = 'Engineering' THEN 1 ELSE 0 END) AS engineering_photo_count,
    SUM(CASE WHEN dca.camera_category = 'Science' THEN 1 ELSE 0 END) AS science_photo_count,
    SUM(CASE WHEN dca.camera_category = 'Entry, Descent, and Landing' THEN 1 ELSE 0 END) AS edl_photo_count
FROM 
    {{ source('MARS_SILVER', 'FACT_PATH') }} fpa
JOIN
    {{ source ('MARS_SILVER', 'DIM_ROVERS') }} dro ON fpa.rover_id = dro.rover_id
LEFT JOIN
    {{ source('MARS_SILVER', 'FACT_PHOTOS') }} fph ON fpa.rover_id = fph.rover_id AND fpa.sol = fph.sol
LEFT JOIN
    {{ source('MARS_SILVER', 'DIM_CAMERAS') }} dca ON fph.camera_id = dca.camera_id
GROUP BY 
    dro.rover_name, 
    COALESCE(fpa.sol, fph.sol), 
    fpa.day_type, 
    fpa.length