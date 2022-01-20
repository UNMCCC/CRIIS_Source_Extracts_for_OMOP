/* 
Notes:
1)  First 2 attributes are included so lineage tracing and source primary key 
    identification is clearly contained in the prospective extract file. 

2)  The output IDENTITY_CONTEXT clearly indicates the primary source of the
    extract file.

3)  The output SOURCE_PK will always contain the column(s) which identify
    the primary key for the source system of record.  In cases where source 
	keys include more than one element they should be sequentially numbered,
	i.e. SOURCE_PK_01, SOURCE_PK_02,etc

4)  In instances where there is no equivalent concept in the source table(s),
    simply include a "NULL as MISSING_COLUMN_NAME" in the SQL so it it clear
	that the omission is not an oversight.

5)  The structure will largely resemble the structure of the target table and 
    wherever practical contain all source elements needed to populate the 
	target, excluding OMOP metadata keys which will be looked up during the 
	Staging to Persistent Store ETL operation.
	
6)  Single value sub-selects are recommended for external lookups vs. lots of
    joins.  Makes everything easier to read.  Additionally, all single element 
	sub selects must include TOP 1 for SQLServer or ROWNUM < 2 for Oracle instances.
	This simply protects against SQLCODE -1427 in cases where the source data 
	may not be properly constrained.

7)  Always include a WHERE clause even if it only validates the source
    PK is not null.


8)  Include as much commenting as needed to clearly express the work being 
    performed.

*/
/*
-- EXECUTION CHECK SUCCESSFUL -- DAH 01/12/2022

-- 12/14/21 -- Should we distinguish between RO and MO? -- ASK INIGO

-- 12/20/21 -- Added Facility Values to Care-Site 
   when scheduling-department = 'CRTC' and scheduling-activity = 'RadGK' then fac_id = 89 ('UNMMG Lovelace Medical Center OP')
   when scheduling-department = 'CRTC' and scheduling-activity NOT 'RadGK' then fac_id = 102  ('UNM CRTC II Radiation Oncology')
   when scheduling-department = 'UNMMO' and scheduling-location = 'UNM Santa Fe' then fac_id = 51 ('UNMCC 715')
   When scheduling-department = 'UNMMO' and scheduling-location NOT 'UNM Santa Fe' then fac_id = 5 ('UNMCC 1201')
   otherwise fac_id = 5 ('UNMCC 1201') -- originally reporting all activity at fac_id = 5
			--
Addressed NULLS 01/12/2022
*/
SET NOCOUNT ON;
SELECT "IDENTITY_CONTEXT|SOURCE_PK|CARE_SITE_ID|CARE_SITE_NAME|PLACE_OF_SERVICE_CONCEPT_ID|LOCATION_ID|CARE_SITE_SOURCE_VALUE|PLACE_OF_SERVICE_SOURCE_VALUE|modified_dtTm";
SELECT 'MOSAIQ FACILITY(OMOP_CARESITE)' AS IDENTITY_CONTEXT,
       fac.FAC_ID						AS SOURCE_PK,
       fac.FAC_ID						AS CARE_SITE_ID,
       fac.Name							AS CARE_SITE_NAME,
       fac.Facility_Type				AS PLACE_OF_SERVICE_CONCEPT_ID,
       fac.FAC_ID						AS LOCATION_ID,
       fac.FAC_ID						AS CARE_SITE_SOURCE_VALUE,
       fac.Name				  		    AS PLACE_OF_SERVICE_SOURCE_VALUE,
	   FORMAT(fac.edit_dtTm,'yyyy-MM-dd HH:mm:ss')  AS modified_dtTm
 FROM Mosaiq.dbo.Facility AS fac
 WHERE fac.FAC_ID IS NOT NULL
 and   fac.FAC_ID in (5, 51, 77, 89, 102  ) -- 5=UNMCC 1201, 51='UNMCC 715', 77='UNMCC SF', 89='UNMMG Lovelace Medical Center OP',102='UNM CRTC II Radiation Oncology'
;

