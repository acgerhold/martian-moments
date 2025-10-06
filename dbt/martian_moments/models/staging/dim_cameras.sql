{{ config(
    materialized='view',
    tags='normalize'
) }}

SELECT
    CASE rover_name
        WHEN 'Perseverance' THEN 8
        WHEN 'Spirit' THEN 7
        WHEN 'Opportunity' THEN 6
        WHEN 'Curiosity' THEN 5
        ELSE 0
    END AS rover_id,
    camera.value::string as camera_name,
    ROW_NUMBER() OVER (ORDER BY camera_name, rover_id) as camera_id,
    CASE 
        WHEN camera.value::string IN (
            'NAVCAM_LEFT','NAVCAM_RIGHT','FRONT_HAZCAM_LEFT_A','FRONT_HAZCAM_RIGHT_A','REAR_HAZCAM_LEFT','REAR_HAZCAM_RIGHT',
            'NAVCAM', 'FHAZ', 'RHAZ', 'FHAZ_LEFT_B', 'FHAZ_RIGHT_B', 'RHAZ_LEFT_B', 'RHAZ_RIGHT_B', 'NAV_RIGHT_B', 'NAV_LEFT_B',
            'PANCAM') 
            THEN 'Engineering'
        WHEN camera.value::string IN (
            'MCZ_RIGHT', 'MCZ_LEFT', 'SHERLOC_WATSON', 'SUPERCAM_RMI', 'SKYCAM',
            'MINITES',
            'CHEMCAM', 'CHEMCAM_RMI', 'MAST', 'MAST_LEFT', 'MAST_RIGHT', 'MAHLI') 
            THEN 'Science'
        WHEN camera.value::string IN (
            'EDL_RUCAM', 'EDL_RDCAM', 'EDL_PUCAM1', 'EDL_PUCAM2', 'EDL_DDCAM',
            'ENTRY',
            'MARDI')
            THEN 'Entry, Descent, and Landing'
        ELSE 'Other'
    END AS camera_category,
FROM 
    {{ source('MARS_SILVER', 'FLAT_MANIFEST_RESPONSE')}} fmr,
    LATERAL FLATTEN(input => parse_json(fmr.photos)) AS photo,
    LATERAL FLATTEN(input => photo.value:cameras) AS camera
GROUP BY
    rover_name,
    camera_name,
    camera_category
