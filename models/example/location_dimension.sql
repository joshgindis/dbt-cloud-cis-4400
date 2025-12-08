WITH location_columns AS (
    SELECT DISTINCT
        borough AS Borough,
        city AS City,
        incident_zip AS Incident_Zip,
        address_type AS Address_Type,
        location_type AS Location_Type,
        street_name AS Street_Name,
        cross_street_1 AS Cross_Street_1,
        cross_street_2 AS Cross_Street_2,
        community_board AS Community_Board,
        council_district AS Council_District,
        census_tract AS Census_Tract
    FROM {{ ref('raw_housing_code_violations') }}
)

SELECT
    row_number() OVER() AS Location_Dim_ID,
    *,
    current_timestamp() AS loaded_at
FROM location_columns
