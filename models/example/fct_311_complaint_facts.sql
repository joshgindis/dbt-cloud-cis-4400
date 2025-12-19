{{
    config(
        materialized='table'
    )
}}
 
WITH agency_dimension AS
(
  SELECT * FROM {{ ref('agency_dimension') }}
),
complaint_type_dimension AS
(
  SELECT * FROM {{ ref('complaint_type_dimension') }}
),
location_dimension AS
(
  SELECT * FROM {{ ref('location_dimension') }}
),
facility_dimension AS
(
  SELECT * FROM {{ ref('facility_dimension') }}
),
date_dimension AS
(
  SELECT * FROM {{ ref('date_dimension') }}
),
all_complaints AS
(
   SELECT * FROM  {{ ref('raw_311_complaints') }}
)
SELECT
    ad.agency_dim_id,
    ctd.complaint_type_dim_id,
    ld.location_dim_id,
    fd.facility_dim_id,
    dd.date_dim_id,
    1 AS complaint_count
FROM all_complaints ac

INNER JOIN agency_dimension ad
    ON ac.agency = ad.Agency
   AND ac.agency_name = ad.Agency_Name

INNER JOIN complaint_type_dimension ctd
    ON ac.complaint_type = ctd.Complaint_Type
   AND ac.descriptor = ctd.Complaint_Description

INNER JOIN location_dimension ld
    ON ac.borough = ld.Borough
   AND ac.incident_zip = ld.Incident_Zip
   AND ac.street_name = ld.Street_Name
   AND ac.community_board = ld.Community_Board

INNER JOIN facility_dimension fd
    ON ac.facility_type = fd.Facility_Type
   AND ac.park_facility_name = fd.Park_Facility_Name
   AND ac.park_borough = fd.Park_Borough
   AND ac.landmark = fd.Landmark

INNER JOIN date_dimension dd
    ON DATE(ac.created_date) = dd.Full_Date
