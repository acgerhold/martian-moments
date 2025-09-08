-- Test: Check that sol values are reasonable for each rover
SELECT 
    rover_name,
    MIN(sol) as min_sol,
    MAX(sol) as max_sol,
    COUNT(*) as photo_count
FROM {{ ref('FLAT_PHOTOS_RESPONSE') }}
GROUP BY rover_name
HAVING 
    -- (ex: Perseverance mission started in 2021, shouldn't have sol > 1500 yet)
    (rover_name = 'Perseverance' AND MAX(sol) > 1500) OR
    (rover_name = 'Curiosity' AND MAX(sol) > 4000) OR
    MIN(sol) < 0
