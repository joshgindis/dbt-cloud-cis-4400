-- raw_housing_code_violations.sql

{{ config(materialized='view') }}

SELECT *, current_timestamp() as loaded_at
FROM `NYC_311.Housing_Code_Violations_2025`
