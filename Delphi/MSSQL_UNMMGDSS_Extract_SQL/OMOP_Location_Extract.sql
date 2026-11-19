SELECT 
    'UNMMGDSS_DSS.PT_DIM_V'              AS IDENTITY_CONTEXT,
    CHECKSUM(pt_corrected_mrn, pt_addr1) AS LOCATION_ID,
    pt_addr1                             AS ADDRESS_1,
    pt_addr2                             AS ADDRESS_2,
    pt_city                              AS CITY,
    pt_state                             AS STATE,
    LEFT(pt_zip, 5)                      AS ZIP,
    pt_county                            AS COUNTY,
    CHECKSUM(pt_corrected_mrn, pt_addr1) AS LOCATION_SOURCE_VALUE,
    NULL                                 AS COUNTRY_CONCEPT_ID,
    NULL                                 AS COUNTRY_SOURCE_VALUE,
    0.0                                  AS LATITUDE,
    0.0                                  AS LONGITUDE,
    pt_edit_date                         AS UPDT_DT_TM,
    pt_corrected_mrn
FROM [unmmgdss].[dss].[pt_dim_v] 
WHERE pt_addr1 <> '?'