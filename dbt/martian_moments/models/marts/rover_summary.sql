{{ config(
    materialized='incremental',
    unique_key='rover_name',
    incremental_strategy='merge',
    cluster_by='rover_name',
    tags='aggregate'
) }}

SELECT
    dro.rover_name,
    dro.rover_status,
    dro.launch_date,
    dro.landing_date,
    MAX(fph.sol) AS max_sol,
    MAX(fph.earth_date) AS max_date
FROM 
    {{ source('MARS_SILVER', 'FACT_PHOTOS') }} fph
JOIN 
    {{ source('MARS_SILVER', 'DIM_ROVERS') }} dro ON fph.rover_id = dro.rover_id
{% if is_incremental() %}
WHERE 
    fph.ingestion_date = (SELECT MAX(ingestion_date) FROM {{ source('MARS_SILVER', 'FACT_PHOTOS') }})
{% endif %}
GROUP BY
    dro.rover_name,
    dro.rover_status,
    dro.launch_date,
    dro.landing_date