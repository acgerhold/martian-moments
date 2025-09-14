-- Test that no rover has significant photo count discrepancies
SELECT 
    rover_name,
    photo_gap,
    actual_total_photos,
    manifest_total_photos
FROM {{ ref('validation_rover_gaps') }}
WHERE ABS(photo_gap) > 100  -- Allow small discrepancies but flag large ones