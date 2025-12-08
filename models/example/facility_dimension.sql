WITH facility_columns AS (
    SELECT DISTINCT
        facilitytype AS Facility_Type,
        parkfacilityname AS Park_Facility_Name,
        parkborough AS Park_Borough,
        landmark AS Landmark
    FROM {{ ref('raw_311_complaints') }}
)

SELECT
    row_number() OVER() AS Facility_Dim_ID,
    *,
    current_timestamp() AS loaded_at
FROM facility_columns
