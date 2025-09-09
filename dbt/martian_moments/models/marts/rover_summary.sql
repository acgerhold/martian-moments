{{ config(
    materialized='incremental',
    unique_keys=['name', 'status', 'launch_date', 'landing_date', 'max_sol', 'max_date', 'total_photos'],
    cluster_by='name',
    tags='aggregate'
) }}

SELECT
    dr.rover_name AS name,
    dr.rover_status AS status,
    dr.launch_date AS launch_date,
    dr.landing_date AS landing_date,
    MAX(fp.sol) AS max_sol,
    MAX(fp.earth_date) AS max_date,
    COUNT(fp.image_id) AS total_photos
FROM 
    {{ source('MARS_SILVER', 'FACT_PHOTOS') }} fp
JOIN 
    {{ source('MARS_SILVER', 'DIM_ROVERS') }} dr
        ON fp.rover_id = dr.rover_id
GROUP BY
    dr.rover_name,
    dr.rover_status,
    dr.launch_date,
    dr.landing_date
