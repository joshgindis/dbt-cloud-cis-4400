{{
    config(
        materialized='table'
    )
}}

WITH building_dimension AS (
    SELECT * FROM {{ ref('building_dimension') }}
),
location_dimension AS (
    SELECT * FROM {{ ref('location_dimension') }}
),
status_dimension AS (
    SELECT * FROM {{ ref('status_dimension') }}
),
nov_dimension AS (
    SELECT * FROM {{ ref('notice_of_violation_dimension') }}
),
date_dimension AS (
    SELECT * FROM {{ ref('date_dimension') }}
),
all_violations AS (
    SELECT * FROM {{ ref('raw_housing_code_violations') }}
)

SELECT
    bd.building_dim_id,
    ld.location_dim_id,
    sd.status_dim_id,
    nd.notice_of_violation_dim_id,
    dd.date_dim_id,
    1 AS violation_count
FROM all_violations v

INNER JOIN building_dimension bd
    ON v.buildingid = bd.Building_ID
   AND v.registrationid = bd.RegistrationID
   AND v.boroid = bd.Boro_ID
   AND v.boro = bd.Borough
   AND v.housenumber = bd.House_Number
   AND v.lowhousenumber = bd.Low_House_Number
   AND v.highhousenumber = bd.High_House_Number
   AND v.streetname = bd.Street_Name
   AND v.streetcode = bd.Steet_Code
   AND v.zipcode = bd.Zipcode
   AND v.apartment = bd.Apartment
   AND v.story = bd.Story
   AND v.block = bd.Block
   AND v.lot = bd.Lot
   AND v.class = bd.Class
   AND v.bin = bd.Bin
   AND v.bbl = bd.BBL
   AND v.nta = bd.NTA

INNER JOIN location_dimension ld
    ON v.borough = ld.Borough
   AND v.city = ld.City
   AND v.zipcode = ld.Incident_Zip
   AND v.streetname = ld.Street_Name

INNER JOIN status_dimension sd
    ON v.current_status_id = sd.Current_Status_ID
   AND v.current_status = sd.Current_Status
   AND v.current_status_date = sd.Current_Status_Date

INNER JOIN nov_dimension nd
    ON v.notice_of_violation_description = nd.Notice_of_Violation_Description
   AND v.notice_of_violation_type = nd.Notice_of_Violation_Type

INNER JOIN date_dimension dd
    ON DATE(v.inspection_date) = dd.Full_Date
