{{ config(
    materialized='incremental',
    unique_key=['name', 'sol', 'image'],
    incremental_strategy='merge',
    cluster_by=['name', 'sol', 'image'],
    tags='aggregate'
) }}

SELECT
    dro.rover_name AS name,
    fph.sol AS sol,
    dca.camera_full_name,
    fpa.sclk_start AS travel_time_start,
    fpa.sclk_end AS travel_time_end,
    fph.img_src AS image,
    REGEXP_SUBSTR(fph.img_src, '_([0-9]{10})_', 1, 1, 'e', 1)::BIGINT AS photo_time,
    CASE
        WHEN photo_time BETWEEN travel_time_start AND travel_time_end 
            THEN TRUE 
        ELSE FALSE 
    END AS taken_during_travel
FROM 
    {{ source('MARS_SILVER', 'FACT_PHOTOS') }} fph
JOIN
    {{ source('MARS_SILVER', 'DIM_ROVERS')}} dro ON fph.rover_id = dro.rover_id
JOIN
    {{ source('MARS_SILVER', 'DIM_CAMERAS')}} dca on fph.camera_id = dca.camera_id
INNER JOIN 
    {{ source('MARS_SILVER', 'FACT_PATH') }} fpa ON fph.rover_id = fpa.rover_id AND fph.sol = fpa.sol
WHERE 
    fph.rover_id = 8