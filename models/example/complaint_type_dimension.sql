WITH complaint_type_columns AS (
    SELECT DISTINCT
        complaint_type AS Complaint_Type,
        descriptor AS Complaint_Description
    FROM {{ ref('raw_311_complaints') }}
)

SELECT
    row_number() OVER() AS Complaint_Type_Dim_ID,
    *,
    current_timestamp() AS loaded_at
FROM complaint_type_columns
