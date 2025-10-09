{{ config(
    materialized='incremental',
    unique_key=['rover_name', 'sol_number', 'travel_distance'],
    incremental_strategy='append',
    cluster_by=['rover_name', 'sol_number', 'travel_distance'],
    tags='aggregate'
) }}

SELECT
    dro.rover_name AS rover_name,
    fph.sol AS sol_number,
    COALESCE(fpa.day_type, 'Stationary') AS day_type,
    COALESCE(fpa.length, 0) AS travel_distance,
    SUM(CASE WHEN dca.camera_category = 'Engineering' THEN 1 ELSE 0 END) AS engineering_photo_count,
    SUM(CASE WHEN dca.camera_category = 'Science' THEN 1 ELSE 0 END) AS science_photo_count,
    SUM(CASE WHEN dca.camera_category = 'Entry, Descent, and Landing' THEN 1 ELSE 0 END) AS edl_photo_count,
    MAX(fph.ingestion_date) AS ingestion_date
FROM 
    {{ source('MARS_SILVER', 'FACT_PHOTOS') }} fph
LEFT JOIN
    {{ source('MARS_SILVER', 'DIM_ROVERS') }} dro ON fph.rover_id = dro.rover_id
LEFT JOIN
    {{ source('MARS_SILVER', 'DIM_CAMERAS') }} dca ON fph.rover_id = dca.rover_id AND fph.camera_name = dca.camera_name
LEFT JOIN
    {{ source('MARS_SILVER', 'FACT_PATH') }} fpa ON fph.rover_id = fpa.rover_id AND fph.sol = fpa.sol
WHERE
    fph.rover_id = 8
{% if is_incremental() %}
    AND fph.ingestion_date > (SELECT MAX(ingestion_date) FROM {{ this }})
{% endif %}
GROUP BY 
    dro.rover_name, 
    fph.sol, 
    fpa.day_type, 
    fpa.length