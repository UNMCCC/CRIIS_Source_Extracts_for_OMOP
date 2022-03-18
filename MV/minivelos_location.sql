-- Debbie's Notes 3/10/22
-- PATIENT ADDRESSES
-- NEED DISTINCT Because Multiple records exist per patient in DM_Patient because it contains study and status data for patient
-- What is  LOCATION_SOURCE_VALUE? DD says "Verbatim value for location" -- what? Concatenated address?  I put the PK for the PERSON
-- Don't know where address is stored for treatment/enrollment sites - ASK RICK
-- Location_PK and Source_PK are the same 1:1
SELECT 'IDENTITY_CONTEXT','SOURCE_PK','LOCATION_ID','ADDRESS_1','ADDRESS_2','CITY','STATE','ZIP','COUNTY','COUNTRY','LOCATION_SOURCE_VALUE','LATITUDE','LONGITUDE','modified_DtTm' 
UNION ALL
SELECT DISTINCT
    'MINIVELOS PATIENT(OMOP_LOCATION)' AS IDENTITY_CONTEXT  
    ,src.PkPERSON_P AS SOURCE_PK
    ,src.PkPERSON_P  AS LOCATION_ID
    ,if(src.patAddress1_p is null, '', src.patAddress1_p) as ADDRESS_q
    ,if(src.patAddress2_p is null, '', src.patAddress2_p) AS ADDRESS_2
    ,if(src.PATCITY_P is null, '', src.PATCITY_P) AS CITY
    ,if(src.PATSTATE_P is null, '', src.PATSTATE_P) AS STATE
    ,if(src.PATZIP_P is null, '', src.PATZIP_P) AS ZIP
    ,if(src.PATCOUNTY_P is null, '', src.PATCOUNTY_P) AS COUNTY
    ,'' AS COUNTRY
    ,src.PkPERSON_P AS LOCATION_SOURCE_VALUE
    ,'' AS LATITUDE
    ,'' AS LONGITUDE
    ,DATE_FORMAT(curDate(),'%Y-%m-%d %H:%i:%s') As modified_DtTm  
FROM
    MINIVELOS.DM_PATIENT src
WHERE src.PkStudy_St is not null 
and src.PkPERSON_P is not null 
and src.PersonCode_P is not null
	and src.Enroll_dt_pp >= '2010-01-01'
and  (		
		src.treatmentOrg_pp_lu ='UNM - CRTC'   
    or (src.treatmentOrg_pp_lu is null and src.enrollOrg_pp_lu = 'UNM - CRTC')
    )   
  and src.PERSONCODE_P <> '00001234 TestPatient'


INTO OUTFILE 'D:\\KRIIS_ETLs\\Sources\\mv\\minivelos_location.dat'
FIELDS TERMINATED BY '|'
ESCAPED BY "" 
LINES TERMINATED BY '\n'