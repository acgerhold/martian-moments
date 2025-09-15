{{ config(
    materialized='incremental',
    unique_key=['name', 'sol', 'image_id'],
    incremental_strategy='merge',
    cluster_by=['name', 'sol', 'image_id'],
    tags='aggregate'
) }}

SELECT
    fpr.rover_name AS name,
    fpr.sol AS sol,
    fpr.image_id,
    CASE 
        WHEN fpr.rover_id = 8
            THEN REGEXP_SUBSTR(img_src, '_([0-9]{10})_', 1, 1, 'e', 1)::BIGINT      
        ELSE NULL
    END as photo_time,
    fpa.sclk_start AS travel_time_start,
    fpa.sclk_end AS travel_time_end,
    CASE
        WHEN photo_time BETWEEN travel_time_start AND travel_time_end 
            THEN TRUE 
        ELSE FALSE 
    END AS taken_during_travel,
    fpr.camera_full_name
FROM 
    {{ source('MARS_SILVER', 'FLAT_PHOTO_RESPONSE') }} fpr
INNER JOIN 
    {{ source('MARS_SILVER', 'FACT_PATH') }} fpa 
        ON fpr.rover_id = fpa.rover_id 
        AND fpr.sol = fpa.sol