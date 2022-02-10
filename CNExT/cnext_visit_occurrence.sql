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

LTV - 1/31/2022 - adding patient's MRN at the end of the query per Mark.

LTV - 2/7/2022 - handled NULL values with the ISNULL function. Replaced NULL selections with empty ticks.

*/
SET NOCOUNT ON;
SELECT 'IDENTITY_CONTEXT|SOURCE_PK|VISIT_OCCURRENCE_ID|PERSON_ID|VISIT_CONCEPT_ID|VISIT_START_DATE|VISIT_START_DATETIME|VISIT_END_DATE|VISIT_END_DATETIME|VISIT_TYPE_CONCEPT_ID|PROVIDER_ID|CARE_SITE_ID|VISIT_SOURCE_VALUE|VISIT_SOURCE_CONCEPT_ID|ADMITTED_FROM_CONCEPT_ID|ADMITTED_FROM_SOURCE_VALUE|REFERRED_TO_CONCEPT_ID|DISCHARGE_TO_SOURCE_VALUE|PRECEDING_VISIT_OCCURRENCE_ID|MRN';
SELECT  'CNEXT PATIENT(OMOP_VISIT_OCCURRENCE)' AS IDENTITY_CONTEXT
    ,HSP.UK AS SOURCE_PK
    ,HSP.UK AS VISIT_OCCURRENCE_ID
	,ISNULL(HSP.F00016, '') AS PERSON_ID                                                                                                                                         /*190*/
	,ISNULL(HSG.F05522, '') AS VISIT_CONCEPT_ID                                                                                                                                  /*605*/
	,(SELECT TOP 1 ISNULL(FORMAT(TRY_CAST(F00427 AS DATE), 'yyyy-MM-dd'), '') FROM UNM_CNExTCases.dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS VISIT_START_DATE                     /*590*/ 
	,(SELECT TOP 1 ISNULL(FORMAT(TRY_CAST(F00427 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '') FROM UNM_CNExTCases.dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS VISIT_START_DATETIME     /*590*/
    ,(SELECT TOP 1 ISNULL(FORMAT(TRY_CAST(F00128 AS DATE), 'yyyy-MM-dd'), '') FROM UNM_CNExTCases.dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS VISIT_END_DATE                       /*600*/
    ,(SELECT TOP 1 ISNULL(FORMAT(TRY_CAST(F00128 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '') FROM UNM_CNExTCases.dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS VISIT_END_DATETIME       /*600*/
	,'1791@32' AS VISIT_TYPE_CONCEPT_ID
	,(SELECT TOP 1 ISNULL(rsTarget.F00675, '')  FROM UNM_CNExTCases.dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS PROVIDER_ID                          /*2460*/
    ,(SELECT TOP 1 ISNULL(rsTarget.F00003, '') FROM UNM_CNExTCases.dbo.PatExtended rsTarget WHERE rsTarget.uk = rsSource.uk Order By rsTarget.UK ASC) AS CARE_SITE_ID                           /*21*/
	,ISNULL(HSG.F05522, '') AS VISIT_SOURCE_VALUE                                                                                                                                /*605*/
	,CASE                        --handles nulls and empty field values
		WHEN HSG.F05522 <> ''
		THEN '605@' + HSG.F05522
		ELSE ''
     END AS VISIT_SOURCE_CONCEPT_ID
	,(SELECT TOP 1 ISNULL(rsTarget.F01684, '')  FROM UNM_CNExTCases.dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS ADMITTED_FROM_CONCEPT_ID              /*2410*/
	,(SELECT TOP 1 ISNULL(rsTarget.F03715, '')  FROM UNM_CNExTCases.dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS ADMITTED_FROM_SOURCE_VALUE            /*2415*/
	,(SELECT TOP 1 ISNULL(rsTarget.F01685, '')  FROM UNM_CNExTCases.dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS REFERRED_TO_CONCEPT_ID                /*2420*/
	,(SELECT TOP 1 ISNULL(rsTarget.F03716, '')  FROM UNM_CNExTCases.dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS DISCHARGE_TO_SOURCE_VALUE             /*2425*/
	,ISNULL(HSP.fk2, '') AS PRECEDING_VISIT_OCCURRENCE_ID
	,ISNULL(HSP.F00006, '') AS MRN
FROM UNM_CNExTCases.dbo.Patient rsSource
JOIN UNM_CNExTCases.dbo.PatExtended PEX ON PEX.uk = rsSource.UK
JOIN UNM_CNExTCases.dbo.Tumor TUM ON rsSource.uk = TUM.fk1
JOIN UNM_CNExTCases.dbo.Hospital HSP ON TUM.uk = HSP.fk2
JOIN UNM_CNExTCases.dbo.HospSupp HSG ON HSG.UK = HSP.UK
WHERE PEX.F00004 IS NOT NULL