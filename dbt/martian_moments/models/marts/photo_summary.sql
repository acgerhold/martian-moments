{{ config(
    materialized='incremental',
    unique_key=['rover_name', 'max_sol'],
    incremental_strategy='append',
    cluster_by=['rover_name', 'max_sol'],
    tags='aggregate'
) }}

SELECT 
    dro.rover_name,
    dro.status,
    dro.launch_date,
    dro.landing_date,
    dro.max_sol,
    dro.max_date,
    dro.total_photos,
    dro.ingestion_date
FROM 
    {{ source('MARS_SILVER', 'DIM_ROVERS') }} dro
{% if is_incremental() %}
    WHERE dro.ingestion_date > (SELECT MAX(ingestion_date) FROM {{ this }})
{% endif %}
