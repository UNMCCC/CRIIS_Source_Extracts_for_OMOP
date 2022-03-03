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

10) Comments reflect Item # as referrd to in the NAACCR layout V21-Chapter-IX-

LTV - 2/7/2022 - handled NULL values with the ISNULL function. Replaced NULL selections with empty ticks. Added conditions to predicate to prevent non-provider data from being returned.

*/
SET NOCOUNT ON;
SELECT 'IDENTITY_CONTEXT|SOURCE_PK|PROVIDER_ID|PROVIDER_NAME|NPI|DEA|SPECIALTY_CONCEPT_ID|CARE_SITE_ID|YEAR_OF_BIRTH|GENDER_CONCEPT_ID|PROVIDER_SOURCE_VALUE|SPECIALTY_SOURCE_VALUE|SPECIALTY_SOURCE_CONCEPT_ID|GENDER_SOURCE_VALUE|GENDER_SOURCE_CONCEPT_ID|Modified_DtTm';
SELECT 'CNEXT PATIENT (OMOP_PROVIDER)' AS IDENTITY_CONTEXT
       ,ISNULL(LBL_CODE, '') AS SOURCE_PK
       ,ISNULL(LBL_CODE, '') AS  PROVIDER_ID
       ,CONCAT (ISNULL(MD_LAST, '') ,', ', ISNULL(MD_FIRST, ''))AS PROVIDER_NAME
       ,ISNULL(NPI_ID, '') AS NPI
       ,'' AS DEA
       ,'' AS SPECIALTY_CONCEPT_ID
       ,'' AS CARE_SITE_ID
       ,'' AS YEAR_OF_BIRTH
       ,'' AS GENDER_CONCEPT_ID
       ,'' AS PROVIDER_SOURCE_VALUE
       ,ISNULL(MD_SPEC, '') AS SPECIALTY_SOURCE_VALUE
       ,'' AS SPECIALTY_SOURCE_CONCEPT_ID
       ,'' AS GENDER_SOURCE_VALUE
       ,'' AS GENDER_SOURCE_CONCEPT_ID
      ,format(SYSDATETIME(),'yyyy-mm-dd HH:mm:ss') as Modified_DtTm
       /*MD_INST AS CARE_SITE_DESC*/
  FROM UNMPHYSICIANS.DBO.DOCTORS
  where LBL_CODE not like 'Z999%'
    and LBL_CODE <> '99999999'