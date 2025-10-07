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
        {{ source('MARS_SILVER', 'FLAT_MANIFEST_RESPONSE') }} fmr,
        LATERAL FLATTEN(input => parse_json(fmr.photos)) as sol
),

actual_sol_data AS (
    SELECT 
        fpr.rover_name,
        fpr.sol,
        fpr.earth_date,
        COUNT(DISTINCT fpr.camera_id) AS actual_camera_count,
        COUNT(fpr.image_id) AS actual_total_photos,
    FROM 
        {{ source('MARS_SILVER', 'FLAT_PHOTO_RESPONSE') }} fpr
    GROUP BY 
        fpr.rover_name, 
        fpr.earth_date, 
        fpr.sol
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