{{ config(
    materialized='incremental',
    unique_key=['image_link'],
    incremental_strategy='append',
    cluster_by=['rover_name', 'sol_number', 'camera_name'],
    tags='aggregate'
) }}

WITH photo_with_time AS (
    SELECT
        fph.rover_id,
        fph.sol,
        fph.camera_name,
        fph.img_src,
        REGEXP_SUBSTR(fph.img_src, '_([0-9]{10})_', 1, 1, 'e', 1)::BIGINT AS photo_time,
        fph.ingestion_date
    FROM 
        {{ source('MARS_SILVER', 'FACT_PHOTOS') }} fph
    WHERE 
        fph.rover_id = 8
        {% if is_incremental() %}
            AND fph.ingestion_date > (SELECT MAX(ingestion_date) FROM {{ this }})
        {% endif %}
)
SELECT
    dro.rover_name AS rover_name,
    pwt.sol AS sol_number,
    dca.camera_name AS camera_name,
    fpa.sclk_start AS travel_time_start,
    fpa.sclk_end AS travel_time_end,
    pwt.img_src AS image_link,
    pwt.photo_time AS photo_time,
    pwt.photo_time BETWEEN fpa.sclk_start AND fpa.sclk_end AS taken_during_travel,
    pwt.ingestion_date AS ingestion_date
FROM 
    photo_with_time pwt
JOIN 
    {{ source('MARS_SILVER', 'DIM_ROVERS') }} dro ON pwt.rover_id = dro.rover_id
JOIN 
    {{ source('MARS_SILVER', 'DIM_CAMERAS') }} dca ON pwt.rover_id = dca.rover_id AND pwt.camera_name = dca.camera_name
INNER JOIN 
    {{ source('MARS_SILVER', 'FACT_PATH') }} fpa ON pwt.rover_id = fpa.rover_id AND pwt.sol = fpa.sol