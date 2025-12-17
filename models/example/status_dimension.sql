WITH status_columns AS (
    SELECT DISTINCT
        currentstatus AS Current_Status,
        currentstatusdate AS Current_Status_Date
    FROM {{ ref('raw_housing_code_violations') }}
)

SELECT
    row_number() OVER() AS Status_Dim_ID,
    *,
    current_timestamp() AS loaded_at
FROM status_columns
