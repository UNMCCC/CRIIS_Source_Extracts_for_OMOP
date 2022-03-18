SELECT 'IDENTITY_CONTEXT','SOURCE_PK','CARE_SITE_ID','CARE_SITE_NAME','PLACE_OF_SERVICE_CONCEPT_ID','LOCATION_ID','CARE_SITE_SOURCE_VALUE','PLACE_OF_SERVICE_SOURCE_VALUE','MODIFIED_DtTM'
UNION ALL
SELECT 
'MINIVELOS ER_SITE(OMOP_CARE_SITE)' AS IDENTITY_CONTEXT
,src.PK_SITE AS SOURCE_PK
,src.PK_SITE AS CARE_SITE_ID		
,src.SITE_NAME AS CARE_SITE_NAME
,'' AS PLACE_OF_SERVICE_CONCEPT_ID 
,src.PK_SITE AS LOCATION_ID   -- Care Site = Location to indicate patients are receiving treatment in UNMHSC but treatment info not maintained in Velos
,src.SITE_ID AS CARE_SITE_SOURCE_VALUE -- 'NM004'
,'' AS PLACE_OF_SERVICE_SOURCE_VALUE
,DATE_FORMAT(curDate(),'%Y-%m-%d %H:%i:%s')  AS MODIFIED_DtTM	 -- using today's date; this table shouldn't change unless we expand to include more sites outside of UNMHSC
FROM minivelos.ER_SITE src
WHERE src.PK_site = 50 -- 'UNM - CRTC'
--            FROM minivelos.ER_PATFACILITY EP_SOURCE;  -- I think that PatFacility refers to the Registering Facility and not the treatment (or enrollment facility)
INTO OUTFILE 'D:\\KRIIS_ETLs\\Sources\\mv\\minivelos_care_site.dat'
FIELDS TERMINATED BY '|'
 ESCAPED BY "" 
LINES TERMINATED BY '\n'
;

 