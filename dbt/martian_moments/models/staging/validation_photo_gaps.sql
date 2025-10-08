{{ config(
    materialized='view',
    tags='validate'
) }}

WITH manifest_sol_data AS (
    SELECT
        rover_name,
        sol,
        earth_date,
        manifest_total_photos,
        manifest_camera_count
    FROM 
        {{ source('MARS_SILVER', 'FLAT_MANIFEST_PHOTOS') }},
),

actual_sol_data AS (
    SELECT 
        rover_name,
        sol,
        earth_date,
        COUNT(DISTINCT camera_id) AS actual_camera_count,
        COUNT(image_id) AS actual_total_photos,
    FROM 
        {{ source('MARS_SILVER', 'FLAT_PHOTO_RESPONSE') }}
    GROUP BY 
        rover_name, 
        earth_date, 
        sol
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
    WHERE
        a.rover_name IS NULL
)

SELECT * FROM validation_results