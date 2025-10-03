{{ config(
    materialized='incremental',
    unique_key='rover_name',
    incremental_strategy='merge',
    cluster_by='rover_name',
    tags='aggregate'
) }}

WITH max_ingestion AS (
    SELECT 
        MAX(ingestion_date) AS max_ingestion_date
    FROM
        {{ source('MARS_SILVER', 'FACT_PHOTOS') }} fph
)

SELECT
    dro.rover_name,
    dro.rover_status,
    dro.launch_date,
    dro.landing_date,
    MAX(fph.sol) AS max_sol,
    MAX(fph.earth_date) AS max_date,
    COUNT(fph.image_id) AS total_photos
FROM 
    {{ source('MARS_SILVER', 'FACT_PHOTOS') }} fph
JOIN 
    {{ source('MARS_SILVER', 'DIM_ROVERS') }} dro ON fph.rover_id = dro.rover_id
{% if is_incremental() %}
WHERE 
    fph.ingestion_date > (SELECT max_ingestion_date FROM max_ingestion)
{% endif %}
GROUP BY
    dro.rover_name,
    dro.rover_status,
    dro.launch_date,
    dro.landing_date