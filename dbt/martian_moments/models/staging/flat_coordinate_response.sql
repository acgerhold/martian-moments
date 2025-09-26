{{ config(
    materialized='view',
    tags='flatten'
) }}

SELECT 
    coordinate.value:rover_name::string as rover_name,
    coordinate.value:geometry.coordinates::array as coordinates,
    coordinate.value:properties.sol::int as sol,
    coordinate.value:properties.fromRMC::string as from_rmc,
    coordinate.value:properties.toRMC::string as to_rmc,
    coordinate.value:properties.length::float as length,
    coordinate.value:properties.SCLK_START::int as sclk_start,
    coordinate.value:properties.SCLK_END::int as sclk_end,
    rcr.ingestion_date as ingestion_date
FROM 
    {{ source('MARS_BRONZE', 'RAW_COORDINATE_RESPONSE') }} rcr,
    LATERAL FLATTEN(input => parse_json(coordinates)) as coordinate
WHERE 
    rcr.ingestion_date = (SELECT MAX(ingestion_date) FROM {{ source('MARS_BRONZE', 'RAW_COORDINATE_RESPONSE') }})