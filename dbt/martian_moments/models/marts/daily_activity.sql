{{ config(
    materialized='incremental',
    unique_key=['name', 'sol', 'travel_distance'],
    incremental_strategy='append',
    cluster_by=['name', 'sol', 'travel_distance'],
    tags='aggregate'
) }}

SELECT
    dro.rover_name AS 'Rover Name',
    fph.sol AS 'Sol Number',
    fpa.day_type AS 'Day Type',
    fpa.length AS 'Travel Distance',
    SUM(CASE WHEN dca.camera_category = 'Engineering' THEN 1 ELSE 0 END) AS 'Engineering Photo Count',
    SUM(CASE WHEN dca.camera_category = 'Science' THEN 1 ELSE 0 END) AS 'Science Photo Count',
    SUM(CASE WHEN dca.camera_category = 'Entry, Descent, and Landing' THEN 1 ELSE 0 END) AS 'Entry, Descent, or Landing Photo Count',
    fph.ingestion_date AS ingestion_date
FROM 
    {{ source('MARS_SILVER', 'FACT_PHOTOS') }} fph
JOIN
    {{ source ('MARS_SILVER', 'DIM_ROVERS') }} dro ON fpa.rover_id = dro.rover_id
JOIN
    {{ source('MARS_SILVER', 'DIM_CAMERAS') }} dca ON fph.camera_id = dca.camera_id
LEFT JOIN
    {{ source('MARS_SILVER', 'FACT_PATH') }} fpa ON fph.rover_id = fpa.rover_id AND fph.sol = fpa.sol
{% if is_incremental() %}
WHERE 
    fpa.ingestion_date = (SELECT MAX(ingestion_date) FROM {{ this }})
{% endif %}
GROUP BY 
    dro.rover_name, 
    fph.sol, 
    fpa.day_type, 
    fpa.length