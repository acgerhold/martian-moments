{{ config(
    materialized='view',
    tags='validate'
) }}

WITH manifest_sol_data AS (
    SELECT
        fmr.rover_name,
        sol.value:sol::int as sol,
        sol.value:earth_date::date as earth_date,
        sol.value:total_photos::int as manifest_total_photos,
        ARRAY_SIZE(sol.value:cameras) as manifest_camera_count
    FROM 
        {{ ref('flat_manifest_response') }} fmr,
        LATERAL FLATTEN(input => parse_json(fmr.photos)) as sol
),

actual_sol_data AS (
    SELECT 
        rover_name,
        sol,
        earth_date,
        total_photos as actual_total_photos,
        cameras_used as actual_camera_count
    FROM 
        {{ ref('camera_summary') }} cs
),

validation_results AS (
    SELECT 
        m.rover_name,
        m.sol,
        m.earth_date,
        m.manifest_total_photos,
        COALESCE(a.actual_total_photos, 0) as actual_total_photos,
        m.manifest_total_photos - COALESCE(a.actual_total_photos, 0) as photo_count_diff,
        m.manifest_camera_count,
        COALESCE(a.actual_camera_count, 0) as actual_camera_count,
        m.manifest_camera_count - COALESCE(a.actual_camera_count, 0) as camera_count_diff,
        CASE 
            WHEN a.rover_name IS NULL 
                THEN 'MISSING_SOL'
            WHEN m.manifest_total_photos != COALESCE(a.actual_total_photos, 0) 
                THEN 'PHOTO_COUNT_MISMATCH'
            WHEN m.manifest_camera_count != COALESCE(a.actual_camera_count, 0) 
                THEN 'CAMERA_COUNT_MISMATCH'
            ELSE 
                'VALID'
        END as validation_status,
        CURRENT_TIMESTAMP() as validation_timestamp
    FROM 
        manifest_sol_data m
    LEFT JOIN 
        actual_sol_data a ON m.rover_name = a.rover_name AND m.sol = a.sol
)

SELECT * FROM validation_results