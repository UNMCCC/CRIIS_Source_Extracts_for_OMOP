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
LTV - 2/25/2022 - renamed REFERRED_TO_CONCEPT_ID to DISCHARGE_TO_CONCEPT_ID per Mark.

*/
SET NOCOUNT ON;
DECLARE @IncDate VARCHAR(8);
SET @IncDate = CONVERT(VARCHAR(8),DateAdd(week, -5, GETDATE()),112);
DECLARE @AllDates VARCHAR(8);
SET @AllDates = '20100101';
DECLARE @fromDate VARCHAR(8);
SET @fromDate = 
   CASE $(isInc)
     WHEN 'Y' THEN  @IncDate
     WHEN 'N' THEN  @AllDates
   END
   
SELECT 'IDENTITY_CONTEXT|SOURCE_PK|VISIT_OCCURRENCE_ID|PERSON_ID|VISIT_CONCEPT_ID|VISIT_START_DATE|VISIT_START_DATETIME|VISIT_END_DATE|VISIT_END_DATETIME|VISIT_TYPE_CONCEPT_ID|PROVIDER_ID|CARE_SITE_ID|VISIT_SOURCE_VALUE|VISIT_SOURCE_CONCEPT_ID|ADMITTED_FROM_CONCEPT_ID|ADMITTED_FROM_SOURCE_VALUE|REFERRED_TO_CONCEPT_ID|DISCHARGE_TO_SOURCE_VALUE|PRECEDING_VISIT_OCCURRENCE_ID|MRN';
SELECT  'CNEXT PATIENT(OMOP_VISIT_OCCURRENCE)' AS IDENTITY_CONTEXT
        ,rsSource.UK AS SOURCE_PK
        ,rsSource.UK AS VISIT_OCCURRENCE_ID
	    ,PAT.uk AS PERSON_ID                                                                                   /*190*/
	    ,'' AS VISIT_CONCEPT_ID
		,case 
		   when HSP.F00024 = '99999999'
		   then ''
		   when right(HSP.F00024, 4) = '9999'
		   then ISNULL(FORMAT(TRY_CAST(left(HSP.F00024,4) + '0101' AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
		   when right(HSP.F00024, 2) = '99'
		   then ISNULL(FORMAT(TRY_CAST(left(HSP.F00024,6) + '01' AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
		   else ISNULL(FORMAT(TRY_CAST(HSP.F00024 AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
	     end AS VISIT_START_DATE
	    ,case 
		   when HSP.F00024 = '99999999'
		   then ''
		   when right(HSP.F00024, 4) = '9999'
		   then ISNULL(FORMAT(TRY_CAST(left(HSP.F00024,4) + '0101' AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
	       when right(HSP.F00024, 2) = '99'
		   then ISNULL(FORMAT(TRY_CAST(left(HSP.F00024,6) + '01' AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
		   else ISNULL(FORMAT(TRY_CAST(HSP.F00024 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
	     end AS VISIT_START_DATETIME
        ,'' AS VISIT_END_DATE                       /*600*/
        ,'' AS VISIT_END_DATETIME                   /*600*/
	    ,'1791@32' AS VISIT_TYPE_CONCEPT_ID
	    ,'' AS PROVIDER_ID                          /*2460*/
        ,'' AS CARE_SITE_ID                           /*21*/
	    ,'' AS VISIT_SOURCE_VALUE                                                                              /*605*/
	    ,'' AS VISIT_SOURCE_CONCEPT_ID
	    ,'' AS ADMITTED_FROM_CONCEPT_ID
	    ,'' AS ADMITTED_FROM_SOURCE_VALUE            /*2415*/
	    ,'' AS DISCHARGE_TO_CONCEPT_ID
	    ,'' AS DISCHARGE_TO_SOURCE_VALUE             /*2425*/
	    ,rsSource.UK AS PRECEDING_VISIT_OCCURRENCE_ID
	    ,ISNULL(HSP.F00006, '') AS MRN
        , CASE WHEN format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') is NULL
         then FORmat(GETDATE(), 'yyyy-MM-dd HH:mm:ss')  
	 else format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') end
	  AS modified_dtTm
  FROM UNM_CNExTCases.dbo.Tumor rsSource
  JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.uk = rsSource.fk1  
  JOIN UNM_CNExTCases.dbo.Hospital HSP ON HSP.fk2 = rsSource.uk 
 INNER JOIN UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK = HExt.UK 
   and HSP.F00006 not in (999999998, 9999998, 999999, 9999)
   and HSP.F00006 >= 1000
   and HExt.F00084 >= @fromDate