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

LTV - 1/31/2022 - adding patient's MRN at the end of the query per Mark. MRN is also listed as the PERSON_ID in this query.

LTV - 2/4/2022 - handled NULL values with the ISNULL function. I am removing the added MRN field because the PERSON_ID field is the
                 MRN in this case.

*/
SET NOCOUNT ON;
SELECT 'IDENTITY_CONTEXT|SOURCE_PK|PERSON_ID|OBSERVATION_PERIOD_START_DATE|OBSERVATION_PERIOD_END_DATE|PERIOD_TYPE_CONCEPT_ID';
SELECT  'CNEXT HOSPITAL(OMOP_OBSERVATION_PERIOD)' AS IDENTITY_CONTEXT
      ,rsSource.UK  AS SOURCE_PK
      ,ISNULL(RTRIM(rsSource.F00006), '') AS PERSON_ID                                                                                                                       /*10*/
	  ,(SELECT TOP 1 ISNULL(FORMAT(TRY_CAST(F00427 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '') FROM UNM_CNExTCases.dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS OBSERVATION_PERIOD_START_DATE     /*590*/
      ,(SELECT TOP 1 ISNULL(FORMAT(TRY_CAST(F00128 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '') FROM UNM_CNExTCases.dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS OBSERVATION_PERIOD_END_DATE       /*600*/
	  ,'1791@32' AS PERIOD_TYPE_CONCEPT_ID
  FROM UNM_CNExTCases.dbo.Hospital rsSource