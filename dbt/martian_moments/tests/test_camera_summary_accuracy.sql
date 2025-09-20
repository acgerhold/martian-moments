-- Test that camera summary data is consistent with manifest data
SELECT 
    rover_name,
    sol,
    validation_status,
    photo_count_diff,
    camera_count_diff
FROM {{ ref('validation_camera_gaps') }}
WHERE validation_status != 'VALID'
  AND validation_status != 'MISSING_SOL'  -- Allow missing sols (they'll be ingested)
  AND ABS(photo_count_diff) > 5  -- Allow small discrepancies