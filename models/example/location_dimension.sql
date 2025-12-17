WITH location_columns AS (
    SELECT DISTINCT
        boro AS Borough,
        zip AS Incident_Zip,
        streetname AS Street_Name,
        apartment AS Apartment,
        communityboard AS Community_Board,
        councildistrict AS Council_District,
        censustract AS Census_Tract
    FROM {{ ref('raw_housing_code_violations') }}
)

SELECT
    row_number() OVER() AS Location_Dim_ID,
    *,
    current_timestamp() AS loaded_at
FROM location_columns
