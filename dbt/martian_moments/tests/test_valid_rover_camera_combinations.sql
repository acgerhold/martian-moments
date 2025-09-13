-- Test: Check that all photos have valid rover and camera combinations
SELECT 
    rover_name,
    camera_name,
    COUNT(*) as photo_count
FROM {{ ref('flat_photos_response') }}
WHERE 
    -- Invalid combinations (ex: Perseverance shouldn't have PANCAM)
    (rover_name = 'Perseverance' AND camera_name = 'PANCAM') OR
    (rover_name = 'Curiosity' AND camera_name = 'SUPERCAM') OR
    (rover_name IS NULL OR camera_name IS NULL)
GROUP BY rover_name, camera_name
HAVING COUNT(*) > 0
