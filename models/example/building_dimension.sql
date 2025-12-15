WITH building_columns AS (
    SELECT DISTINCT
        buildingid AS Building_ID,
        registrationid AS RegistrationID,
        boroid AS Boro_ID,
        boro AS Borough,
        city AS City,
        housenumber AS House_Number,
        lowhousenumber AS Low_House_Number,
        highhousenumber AS High_House_Number,
        streetname AS Street_Name,
        streetcode AS Street_Code,
        zipcode AS Zipcode,
        apartment AS Apartment,
        story AS Story,
        block AS Block,
        lot AS Lot,
        class AS Class,
        bin AS Bin,
        bbl AS BBL,
        nta AS NTA
    FROM {{ ref('raw_housing_code_violations') }}
)

SELECT
    row_number() OVER() AS Building_Dim_ID,
    *,
    current_timestamp() AS loaded_at
FROM building_columns


