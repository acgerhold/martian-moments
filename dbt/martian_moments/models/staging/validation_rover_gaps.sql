{{ config(
    materialized='view',
    tags='validate'
) }}

WITH manifest_data AS (
    SELECT 
        fmr.rover_name,
        fmr.max_sol as manifest_max_sol,
        fmr.max_date as manifest_max_date,
        fmr.total_photos as manifest_total_photos
    FROM 
        {{ source('MARS_SILVER', 'FLAT_MANIFEST_RESPONSE') }} fmr
),

actual_data AS (
    SELECT 
        rs.rover_name,
        rs.max_sol as actual_max_sol,
        rs.max_date as actual_max_date,
        rs.total_photos as actual_total_photos
    FROM 
        {{ source('MARS_GOLD', 'ROVER_SUMMARY') }} rs
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