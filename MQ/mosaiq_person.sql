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

Updates by Debbie Healy 11/12/2021
Revised: adm.adm_id	AS LOCATION_ID	Is this correct?  See mosaiq_location.sql
Removed: ad.Pri_Md_ID AS PROVIDER_ID; Reason:  This field is not a reliable indicator of the provider(s) in charge of the patient's care.  
To Do:	May need to revisit need for primary provider per Mark
Changed: WHERE Conditions:
		Removed condition to select only patients with captured appointments.  Decided to send all VALID patients
		Added condition in join to exclude pat_id1s that don't have an MRN
Added: 	ident.ida as MRN		-- No corresponding OMOP field.  Kevin will handle this
Added:	Valid patient check. Test patients are added to production in Mosaiq and used to test processes and upgrades.
		Created table mosaiqAdmin.dbo.pat_id1_check to flag pat_id1 as valid or not.  Job runs daily before start of business
		Added Join to this table to extract only valid paitents
To Do:	Found potential home in Mosaiq.Admin table to identify "Testing" patients -- Admin.user_defined_info1 (2,3,4) or Admin.user_defined_pro_id_1 (2,3,4)
		None are being used.  the _pro_ fields have the advantage that a value ("testing") can be set in the prompt table and then a drop-down set in the Admin UI.
		Discuss with Alicia.    This would eliminate the need to rely on MosaiqAdmin DB.
*/


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

12) Include Cerner MRN in field list

Updates by Debbie Healy 11/12/2021
Revised: adm.adm_id	AS LOCATION_ID	Is this correct?  See mosaiq_location.sql
Removed: ad.Pri_Md_ID AS PROVIDER_ID; Reason:  This field is not a reliable indicator of the provider(s) in charge of the patient's care.  
To Do:	May need to revisit need for primary provider per Mark
Changed: WHERE Conditions:
		Removed condition to select only patients with captured appointments.  Decided to send all VALID patients
		Added condition in join to exclude pat_id1s that don't have an MRN
Added: 	ident.ida as MRN		-- No corresponding OMOP field.  Kevin will handle this
Added:	Valid patient check. Test patients are added to production in Mosaiq and used to test processes and upgrades.
		Created table mosaiqAdmin.dbo.pat_id1_check to flag pat_id1 as valid or not.  Job runs daily before start of business
		Added Join to this table to extract only valid paitents
To Do:	Found potential home in Mosaiq.Admin table to identify "Testing" patients -- Admin.user_defined_info1 (2,3,4) or Admin.user_defined_pro_id_1 (2,3,4)
		None are being used.  the _pro_ fields have the advantage that a value ("testing") can be set in the prompt table and then a drop-down set in the Admin UI.
		Discuss with Alicia.    This would eliminate the need to rely on MosaiqAdmin DB.

CONFIDENCE LEVEL:  MEDIUM HIGH

EXECUTION CHECK SUCCESSFUL -- DAH 01/12/2022
2/3/2020 -- Wrapped Ethnicity_Source_Value and Race_Source value in these Replace statements to remove CRLF
			REPLACE(REPLACE(column, CHAR(13), ''), CHAR(10), '')
		 -- Added Adm.Adm_ID as Location_ID
*/
SET NOCOUNT ON;

SELECT 'IDENTITY_CONTEXT|SOURCE_PK|PERSON_ID|GENDER_CONCEPT_ID|YEAR_OF_BIRTH|MONTH_OF_BIRTH|DAY_OF_BIRTH|BIRTH_DATETIME|DEATH_DATETIME|RACE_CONCEPT_ID|ETHNICITY_CONCEPT_ID|LOCATION_ID|PROVIDER_ID|CARE_SITE_ID|PERSON_SOURCE_VALUE|GENDER_SOURCE_VALUE|GENDER_SOURCE_CONCEPT_ID|RACE_SOURCE_VALUE|RACE_SOURCE_CONCEPT_ID|ETHNICITY_SOURCE_VALUE|ETHNICITY_SOURCE_CONCEPT_ID|MRN|Modified_DtTm';

SELECT 'MOSAIQ PATIENT(OMOP_PERSON)'								AS IDENTITY_CONTEXT
       ,pat.Pat_ID1													AS SOURCE_PK
       ,pat.Pat_ID1													AS PERSON_ID
	   ,isNULL(adm.Gender,'')				  						AS GENDER_CONCEPT_ID
	   ,isNULL(YEAR(pat.Birth_DtTm),'')								AS YEAR_OF_BIRTH
	   ,isNULL(MONTH(pat.Birth_DtTm),'')							AS MONTH_OF_BIRTH
	   ,isNULL(DAY  (pat.Birth_DtTm),'')							AS DAY_OF_BIRTH
	   ,isNULL(FORMAT(pat.Birth_DtTm,  'yyyy-MM-dd HH:mm:ss'),'')	AS BIRTH_DATETIME
       ,isNULL(FORMAT(adm.Expired_DtTm,'yyyy-MM-dd HH:mm:ss'),'')	AS DEATH_DATETIME
	   ,isNULL(REPLACE(REPLACE(Mosaiq.dbo.fn_GetPatientRaces(pat.Pat_ID1,0,0), CHAR(13), ''), CHAR(10), ''),'')	AS RACE_CONCEPT_ID
	   ,isNULL(REPLACE(REPLACE(proEth.Description, CHAR(13), ''), CHAR(10), '') ,'')	AS ETHNICITY_CONCEPT_ID
	   ,isNULL(adm.adm_id,'')										AS LOCATION_ID  -- this points to admin with physical address. 
	   ,''															AS PROVIDER_ID  -- may need to revisit per Mark
	   ,5															AS CARE_SITE_ID  -- DEFAULT VALUE FOR UNMCCC
       ,pat.Pat_ID1													AS PERSON_SOURCE_VALUE
	   ,isNULL(adm.Gender,'')										AS GENDER_SOURCE_VALUE
	   ,''															AS GENDER_SOURCE_CONCEPT_ID
	   ,isNULL(REPLACE(REPLACE(Mosaiq.dbo.fn_GetPatientRaces(pat.Pat_ID1,0,0), CHAR(13), ''), CHAR(10), ''),'')	AS RACE_SOURCE_VALUE
	   ,''															AS RACE_SOURCE_CONCEPT_ID
	   ,isNULL(REPLACE(REPLACE(proEth.Description, CHAR(13), ''), CHAR(10), '') ,'')	AS ETHNICITY_SOURCE_VALUE
	   ,''															AS ETHNICITY_SOURCE_CONCEPT_ID
	   ,isNULL(Ref_Patients.ida,'')									AS MRN			-- Kevin is building MRN mapping
	   ,isNULL(FORMAT(pat.Edit_DtTm,'yyyy-MM-dd HH:mm:ss'),'')		AS Modified_DtTm
  FROM Mosaiq.dbo.Patient pat
  INNER JOIN mosaiqAdmin.dbo.Ref_Patients on pat.pat_id1 = Ref_Patients.pat_id1 and Ref_Patients.is_valid <> 'N'  -- eliminate sample patients 
  --INNER JOIN MosaiqAdmin.dbo.RS21_Patient_List_for_Security_Review Subset on pat.pat_id1 = Subset.pat_id1			-- Subset of patients for Security Review
  LEFT JOIN Mosaiq.dbo.admin adm on pat.pat_id1 = adm.pat_id1
  LEFT JOIN Mosaiq.dbo.Prompt proEth on adm.Ethnicity_PRO_ID = proEth.Pro_ID
  WHERE pat.Pat_ID1 IS NOT NULL
  ;
 

 



  