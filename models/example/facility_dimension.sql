WITH facility_columns AS (
    SELECT DISTINCT
        facility_type AS Facility_Type,
        park_facility_name AS park_facility_name
        park_borough AS Park_Borough_Name
        landmark AS Landmark
    FROM {{ ref('raw_311_complaints') }}
)

SELECT
    row_number() OVER() AS Facility_Dim_ID,
    *,
    current_timestamp() AS loaded_at
FROM facility_columns
