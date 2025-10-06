{{ config(
    materialized='view',
    tags='normalize'
) }}

SELECT
    CASE rover_name
        WHEN 'Perseverance' THEN 8
        WHEN 'Spirit' THEN 7
        WHEN 'Opportunity' THEN 6
        WHEN 'Curiosity' THEN 5
        ELSE 0
    END AS rover_id,
    rover_name,
    status,
    launch_date,
    landing_date,
    max_sol,
    max_date,
    total_photos,
    ingestion_date
FROM 
    {{ source('MARS_SILVER', 'FLAT_MANIFEST_RESPONSE') }}