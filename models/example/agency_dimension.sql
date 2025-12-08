WITH agency_columns AS (
    SELECT DISTINCT
        agency AS Agency,
        agencyname AS Agency_Name
    FROM {{ ref('raw_311_complaints') }}
)

SELECT
    row_number() OVER() AS Agency_Dim_ID,
    *,
    current_timestamp() AS loaded_at
FROM agency_columns
