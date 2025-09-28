{{ config(
    materialized='incremental',
    unique_key='name',
    incremental_strategy='merge',
    cluster_by='name',
    tags='aggregate'
) }}

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
GROUP BY
    dro.rover_name,
    dro.rover_status,
    dro.launch_date,
    dro.landing_date