{{ config(
    materialized='incremental',
    unique_key=['name', 'sol', 'camera_full_name', 'travel_time_start', 'image'],
    incremental_strategy='merge',
    cluster_by=['name', 'sol', 'camera_full_name', 'travel_time_start', 'image'],
    tags='aggregate'
) }}

WITH photo_with_time AS (
    SELECT
        fph.rover_id,
        fph.sol,
        fph.camera_id,
        fph.img_src,
        REGEXP_SUBSTR(fph.img_src, '_([0-9]{10})_', 1, 1, 'e', 1)::BIGINT AS photo_time
    FROM 
        {{ source('MARS_SILVER', 'FACT_PHOTOS') }} fph
    WHERE 
        fph.rover_id = 8
)
SELECT
    dro.rover_name AS name,
    pwt.sol,
    dca.camera_full_name,
    fpa.sclk_start AS travel_time_start,
    fpa.sclk_end AS travel_time_end,
    pwt.img_src AS image,
    pwt.photo_time,
    pwt.photo_time BETWEEN fpa.sclk_start AND fpa.sclk_end AS taken_during_travel
FROM 
    photo_with_time pwt
JOIN 
    {{ source('MARS_SILVER', 'DIM_ROVERS') }} dro ON pwt.rover_id = dro.rover_id
JOIN 
    {{ source('MARS_SILVER', 'DIM_CAMERAS') }} dca ON pwt.camera_id = dca.camera_id
INNER JOIN 
    {{ source('MARS_SILVER', 'FACT_PATH') }} fpa ON pwt.rover_id = fpa.rover_id AND pwt.sol = fpa.sol