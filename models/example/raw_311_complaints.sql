-- raw_311_complaints.sql

{{ config(materialized='view') }}

SELECT *, current_timestamp() as loaded_at
FROM `NYC_311.311_Complaints_2025`
