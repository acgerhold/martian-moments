{{ config(
    materialized='view',
    tags='validate'
) }}

WITH manifest_data AS (
    SELECT 
        rover_name,
        max_sol as manifest_max_sol,
        max_date as manifest_max_date,
        total_photos as manifest_total_photos
    FROM 
        {{ ref('flat_manifest_response') }}
),

actual_data AS (
    SELECT 
        rover_name,
        max_sol as actual_max_sol,
        max_date as actual_max_date,
        total_photos as actual_total_photos
    FROM 
        {{ ref('rover_summary') }}
),

validation_results AS (
    SELECT 
        m.rover_name,
        m.manifest_max_sol,
        COALESCE(a.actual_max_sol, 0) as actual_max_sol,
        m.manifest_max_sol - COALESCE(a.actual_max_sol, 0) as sol_gap,
        m.manifest_total_photos,
        COALESCE(a.actual_total_photos, 0) as actual_total_photos,
        m.manifest_total_photos - COALESCE(a.actual_total_photos, 0) as photo_gap,
        m.manifest_max_date,
        a.actual_max_date,
        CURRENT_TIMESTAMP() as validation_timestamp
    FROM 
        manifest_data m
    LEFT JOIN 
        actual_data a ON m.rover_name = a.rover_name
)

SELECT * FROM validation_results