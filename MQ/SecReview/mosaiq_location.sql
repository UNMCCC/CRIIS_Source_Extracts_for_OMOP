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
	
9)  This script is to be run and is written to be run in MS SQL Server and will generate errors if run in other DB platforms

10) Use date format 'YYYY-MM-DD HH24:MI:SS' per Kevin 11/9/2021 

11) Include record modified Date-Time at end of field list
*/
/*Changes to Script by Debbie Healy 11/8/2021
1) NOTE:	FOR PATIENT ADDRESSES and UNMCCC FACILITY ONLY
12/20/21
1)  Added additional facilities to be mapped using scheduling activity and location
2)  How to capture Radiation-Oncology Machine Locations?  (Machine Name, Machine Type) 
	case 
		when loc.machine_type = 1 then 'Accelerator'
		when loc.machine_type = 2 then 'Simulator'
		when loc.machine_type = 3 then 'Kilovoltage'
		when loc.machine_type = 4 then 'HDR'
		when loc.machine_type = 5 then 'Cobalt'
		else 'other'
	end machine_type_desc

Note that Mosaiq.dbo.Facility is free-text entry -- there are multiple entries for MO (fac=5) and RO (fac= 102) so I picked the "best" one

CONFIDENCE LEVEL:  MEDIUM HIGH

EXECUTION CHECK SUCCESSFUL DAH 01/12/2022

ARE ALL THESE FACILITIES NEEDED FOR OMOP_LOCATION?:  5=UNMCC 1201, 51='UNMCC 715', 77='UNMCC SF', 89='UNMMG Lovelace Medical Center OP',102='UNM CRTC II Radiation Oncology'
 
CHECKED NULLS  DAH 01/12/2022
ADDED MISSING FIELD Location_ID  DAH 01/12/2022
1/20/22 -- OMOP_LOCATION: changed Source_PK/Location_ID from ADM_ID (original code) to Pat_ID1 so that patient addresses could be linked to other patient data.

*/

SET NOCOUNT ON;
SELECT "IDENTIY_CONTEXT|SOURCE_PK|LOCATION_ID|ADDRESS_1|ADDRESS_2|CITY|STATE|ZIP|COUNTY|COUNTRY|LOCATION_SOURCE_VALUE|LATITUDE|LONGITUDE|Modified_dTm";

SELECT  --  Get Address for Valid Patients
    'MOSAIQ ADMIN(OMOP_LOCATION)'	AS IDENTITY_CONTEXT,
    adm.Pat_ID1						AS SOURCE_PK,    -- Using Pat_ID1 instead of Adm.Adm_ID 
	adm.Pat_ID1						AS LOCATION_ID, -- Field #1 in DD
    adm.Pat_Adr1					AS ADDRESS_1,
    adm.Pat_Adr2					AS ADDRESS_2,
    adm.Pat_City					AS CITY,
    adm.Pat_State					AS STATE,
    adm.Pat_Postal					AS ZIP,
    adm.Pat_County					AS COUNTY,
    adm.Pat_Country					AS COUNTRY,
    adm.ADM_ID						AS LOCATION_SOURCE_VALUE,
    NULL AS LATITUDE,
    NULL AS LONGITUDE,
	FORMAT(adm.edit_DtTm, 'yyyy-MM-dd HH:mm:ss')	AS Modified_dTm
FROM
  Mosaiq.dbo.Admin as adm
  INNER JOIN MosaiqAdmin.dbo.RS21_Patient_List_for_Security_Review Subset on adm.pat_id1 = Subset.pat_id1  -- Subset of patients for Security Review
  INNER join mosaiqAdmin.dbo.Ref_Patients on adm.pat_id1 = Ref_Patients.pat_id1 and Ref_Patients.is_valid <> 'N' -- eliminate sample patients 
WHERE
    adm.Pat_Adr1 IS NOT NULL
    AND adm.Pat_Adr1 <> ''
UNION ALL
SELECT
    'MOSAIQ FACILITY(OMOP_LOCATION)' AS IDENTITY_CONTEXT,
    Fac.FAC_ID					AS SOURCE_PK,
	Fac.FAC_ID					AS LOCATION_ID, -- Field #1 in DD
    Fac.Adr1					AS ADDRESS_1,
    Fac.Adr2					AS ADDRESS_2,
    Fac.City					AS CITY,
    Fac.State_Province			AS STATE,
    Fac.postal					AS ZIP,
    NULL						AS COUNTY,
    Fac.Country					AS COUNTRY,
    Fac.FAC_ID					AS LOCATION_SOURCE_VALUE,
    NULL						AS LATITUDE,
    NULL						AS LONGITUDE,
	FORMAT(Fac.edit_DtTm, 'yyyy-MM-dd HH:mm:ss') AS Modified_dTm
FROM
    Mosaiq.dbo.Facility as Fac
WHERE
    Fac.FAC_ID IS NOT NULL
    AND Fac.Adr1 IS NOT NULL
    AND Fac.Adr1 <> ''
	and Fac.FAC_ID in (5, 51, 77, 89, 102  ) -- 5=UNMCC 1201, 51='UNMCC 715', 77='UNMCC SF', 89='UNMMG Lovelace Medical Center OP',102='UNM CRTC II Radiation Oncology'
  -- Note that this table is free-text entry -- there are multiple entries for MO (fac=5) and RO (fac= 102) so I picked the "best" one

;


