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

Changes to Script by Debbie Healy 11/4/2021
	 Removed: (SELECT TOP 1 MDName_FML_SFX FROM Mosaiq.dbo.vw_Rpt_Pnp_Info rsTarget WHERE rsTarget.Staff_ID=rsSource.Staff_ID) AS PROVIDER_NAME, 	Reason: derives name from either Staff or External Tables -- these will ordering/visit providers be from Staff Table
	 Changed: 1306 AS CARE_SITE_ID,					Reason: Wrong Zip; Picked Facility.Fac_id = 5 because it has the correct address for the location table
	 Removed: AND rsSource.NonPhysician <> 2		Reason: Redundant because NonPhysician == 2 when Staff.Type = 'Location'
	 Removed: ISNULL(Mosaiq.dbo.fn_GetPnpInfo(rsSource.Staff_ID, 5), '') AS SPECIALTY_SOURCE_VALUE,  Reason: Unreliable Specialties (eg. Has our CMO as a Pediatrician)  
	 Changed above to: rsSource.Other_Credentials AS SPECIALTY_SOURCE_VALUE, -- same value as for SPECIALTY_CONCEPT_ID (OK?)
	 Changed WHERE CONDITIONS:
		Issue:	Staff Table constains all Mosaiq user, so the plan was to extract only health-care providers from staff table.  
				But even selecting scheduling providers will include non-medical providers. 
				Examples, RO Treatment staff include roles of Director, Administrator, Clerical, Dosimetrist;  Financial Assistance & Medical Records Appts staff are scheduled to clerical or admistrative staff.  
				Clerical staff may change jobs and become Therapists/Technicians without their Mosaiq Staff_roll being revised.
		Issue:	Would have had to expand to include Providers who place orders since not all ordering staff actually see patients.
		Issue:  Was concerned obout messing up joins later down the line by inadvertantly excluding staff
		Removed:	LEFT OUTER JOIN dbo.Schedule SCH WITH(NOLOCK) on SCH.Staff_ID = rsSource.Staff_ID  WHERE SCH.Version = 0  
		Removed:	AND rsSource.Staff_Role <> 0     --Redunant.  Staff.Type <> "Location" does the job
		Added:	Restrictions to remove sample staff, scheduling templates, security accounts etc.
		Note:	Staff with Last_Name = 'Infusion' and First_Name '(0-1)', '(1.5-2)', '(2.5-3)', '(3.5-5)', '5.5+', 'Add On' because these are used as scheduling staf

-- ALL UNMCCC STAFF not just PROVIDERS
EXECUTION CHECK SUCCESSFUL -- DAH 01/10/2022
*/
SET NOCOUNT ON;
SELECT "IDENTITY_CONTEXT|SOURCE_PK|PROVIDER_ID|PROVIDER_NAME|NPI|DEA|SPECIALTY_CONCEPT_ID|CARE_SITE_ID|YEAR_OF_BIRTH|GENDER_CONCEPT_ID|PROVIDER_SOURCE_VALUE|SPECIALTY_SOURCE_VALUE|SPECIALTY_SOURCE_CONCEPT_ID|GENDER_SOURCE_VALUE|GENDER_SOURCE_CONCEPT_ID|modified_dtTm";
SELECT DISTINCT  -- added DH
	  'MOSAIQ STAFF(OMOP_PROVIDER)' AS IDENTITY_CONTEXT,
       rsSource.Staff_ID	AS SOURCE_PK,
       rsSource.Staff_ID	AS PROVIDER_ID,
	   isNULL(ltrim(rtrim(rsSource.Last_name)) + ', ' + ltrim(rtrim(rsSource.First_name)),'' ) AS PROVIDER_NAME, -- DAH 11/4/21
        (SELECT TOP 1 ID_Code FROM Mosaiq.dbo.EXT_ID rsTarget WHERE rsSource.Staff_ID = rsTarget.Staff_id AND rsTarget.Ext_Type = 'NPI') AS NPI,
        (SELECT TOP 1 ID_Code FROM Mosaiq.dbo.EXT_ID rsTarget WHERE rsSource.Staff_ID = rsTarget.Staff_id AND rsTarget.Ext_Type = 'DEA')  AS DEA,
       TRIM(rsSource.Other_Credentials) 			AS SPECIALTY_CONCEPT_ID,  -- Manually Maintained field in Mosaiq since 2015
	   5									AS CARE_SITE_ID,	 ---UNMCCC Default Facility	 
	   YEAR(rsSource.Birth_DtTm) 			AS YEAR_OF_BIRTH,
       rsSource.Gender 						AS GENDER_CONCEPT_ID,
       rsSource.Staff_ID					AS PROVIDER_SOURCE_VALUE,
	   TRIM(rsSource.Other_Credentials)			AS SPECIALTY_SOURCE_VALUE,     
       NULL									AS SPECIALTY_SOURCE_CONCEPT_ID,
       rsSource.Gender						AS GENDER_SOURCE_VALUE,
       NULL									AS GENDER_SOURCE_CONCEPT_ID,
	   Format(rsSource.edit_DtTm,'yyyy-MM-dd HH:mm:ss') as Modified_DtTm	
FROM Mosaiq.dbo.Staff rsSource
WHERE	rsSource.Type <> 'Location'				-- To select people and exclude places and machines
    and rsSource.Type <> '*Do Not Delete*'		 
	and rsSource.First_Name not in ('Template', 'Sample')	
	and rsSource.Last_Name not like '%ZZ%'	
	and rsSource.Last_Name not in  ('Auditor', 'Unknown', 'Unknown Doctor', 'Unlisted', 'zBilling', 'Himaudit')
;

