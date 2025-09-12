{{ config(
    materialized='view',
    tags='normalize'
) }}

SELECT DISTINCT
    camera_id,
    rover_id,
    camera_name,
    camera_full_name,
    CASE 
        WHEN camera_name IN (
            'NAVCAM_LEFT','NAVCAM_RIGHT','FRONT_HAZCAM_LEFT_A','FRONT_HAZCAM_RIGHT_A','REAR_HAZCAM_LEFT','REAR_HAZCAM_RIGHT',
            'NAVCAM', 'FHAZ', 'RHAZ',
            'PANCAM') 
            THEN 'Engineering'
        WHEN camera_name IN (
            'MCZ_RIGHT', 'MCZ_LEFT', 'SHERLOC_WATSON', 'SUPERCAM_RMI', 'SKYCAM',
            'MINITES',
            'CHEMCAM', 'MAST', 'MAHLI') 
            THEN 'Science'
        WHEN camera_name IN (
            'EDL_RUCAM', 'EDL_RDCAM', 'EDL_PUCAM1', 'EDL_PUCAM2', 'EDL_DDCAM',
            'ENTRY',
            'MARDI')
            THEN 'Entry, Descent, and Landing'
        ELSE 'Other'
    END AS camera_category
FROM 
    {{ source('MARS_SILVER', 'FLAT_PHOTO_RESPONSE') }}
