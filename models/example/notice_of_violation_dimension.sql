WITH notice_of_violation_columns AS (
    SELECT DISTINCT
        novtype AS Notice_of_Violation_Type,
        novdescription AS Notice_of_Violation_Description
        novissuedate AS Notice_of_Violation_Issue_Date
    FROM {{ ref('raw_housing_code_violations') }}
)

SELECT
    row_number() OVER() AS Notice_of_Violation_Dim_ID,
    *,
    current_timestamp() AS loaded_at
FROM notice_of_violation_columns
