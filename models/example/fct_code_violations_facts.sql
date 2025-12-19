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
   AND v.streetcode = bd.Street_Code
   AND v.zip = bd.Zipcode
   AND v.apartment = bd.Apartment
   AND v.story = bd.Story
   AND v.block = bd.Block
   AND v.lot = bd.Lot
   AND v.class = bd.Class
   AND v.bin = bd.Bin
   AND v.bbl = bd.BBL
   AND v.nta = bd.NTA

INNER JOIN location_dimension ld
    ON v.boro = ld.Borough
    AND v.streetname = ld.Street_Name
    AND v.apartment = ld.Apartment
    AND v.cross_street_2 = ld.Cross_Street_2
    AND v.councildistrict = ld.Council_District
    AND v.censustract = ld.Census_Tract

INNER JOIN status_dimension sd
   ON v.currentstatus = sd.Current_Status
   AND v.currentstatusdate = sd.Current_Status_Date

INNER JOIN nov_dimension nd
    ON v.novtype = nd.Notice_of_Violation_Type
   AND v.novdescription = nd.Notice_of_Violation_Description
   AND v.novissueddate = nd.Notice_of_Violation_Issue_Date
        
INNER JOIN date_dimension dd
    ON DATE(v.inspectiondate) = dd.Full_Date
