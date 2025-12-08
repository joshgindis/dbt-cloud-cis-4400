WITH notice_of_violation_columns AS (
    SELECT DISTINCT
        notice_of_violation_type AS Notice_of_Violation_Type,
        notice_of_violation_description AS Notice_of_Violation_Description
    FROM {{ ref('raw_housing_code_violations') }}
)

SELECT
    row_number() OVER() AS Notice_of_Violation_Dim_ID,
    *,
    current_timestamp() AS loaded_at
FROM notice_of_violation_columns
