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

LTV - 2/4/2022 - handled NULL values with the ISNULL function. Replaced NULL selections with empty ticks.

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
   
SELECT 'IDENTITY_CONTEXT|SOURCE_PK|CONDITION_OCCURRENCE_ID|PERSON_ID|CONDITION_CONCEPT_ID_SITE|CONDITION_CONCEPT_ID_MORPH|CONDITION_START_DATE|CONDITION_START_DATETIME|CONDITION_END_DATE|CONDITION_END_DATETIME|CONDITION_TYPE_CONCEPT_ID|CONDITION_STATUS_CONCEPT_ID|STOP_REASON|PROVIDER_ID|VISIT_OCCURRENCE_ID|VISIT_DETAIL_ID|CONDITION_SOURCE_VALUE|CONDITION_SOURCE_CONCEPT_ID|CONDITION_STATUS_SOURCE_VALUE|MRN|Modified_DtTm';
SELECT  'CNEXT TUMOR(OMOP_CONDITION_OCCURRENCE)' AS IDENTITY_CONTEXT
        ,rsSource.uk AS SOURCE_PK
        ,rsSource.uk AS CONDITION_OCCURRENCE_ID
    	,PAT.UK AS PERSON_ID  /*545*/ 
	    ,ISNULL(STUFF(rsSource.F00152,4,0,'.'), '') AS CONDITION_CONCEPT_ID_SITE                                                  /*400*/ 
        ,ISNULL(STUFF(rsSource.F02503,5,0,'/'), '') AS CONDITION_CONCEPT_ID_MORPH
		,case
		   when rsSource.F00029 = '00000000'
		   then ''
		   when right(rsSource.F00029, 4) = '9999'
		   then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		   when right(rsSource.F00029,2) = '99'
           then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		   else ISNULL(FORMAT(TRY_CAST(rsSource.F00029 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		 END AS CONDITION_START_DATE
        ,case
		   when rsSource.F00029 = '00000000'
		   then ''
		   when right(rsSource.F00029, 4) = '9999'
		   then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,4) + '0101' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		   when right(rsSource.F00029,2) = '99'
           then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,6) + '01' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		   else ISNULL(FORMAT(TRY_CAST(rsSource.F00029 AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		 END AS CONDITION_START_DATETIME
        ,'' AS CONDITION_END_DATE
	    ,'' AS CONDITION_END_DATETIME
    	,'1791@32' AS CONDITION_TYPE_CONCEPT_ID
        ,CASE WHEN rsSource.F00129 <> 9 THEN 'Confirmed diagnosis'
              ELSE '' 
          END AS CONDITION_STATUS_CONCEPT_ID                                                                          /*490*/ 
	    ,'' AS STOP_REASON
	    ,(SELECT TOP 1 ISNULL(rsTarget.F05162, '') FROM UNM_CNExTCases.dbo.DxStg rsTarget WHERE rsSource.uk = rsTarget.fk2 Order By rsTarget.UK ASC) AS PROVIDER_ID     /*2460*/
	    ,rsSource.uk AS VISIT_OCCURRENCE_ID
	    ,'' AS VISIT_DETAIL_ID
	    ,ISNULL(STUFF(rsSource.F02503,5,0,'/') + '-'+ STUFF(rsSource.F00152,4,0,'.'), '') AS CONDITION_SOURCE_VALUE               /*764*/
        ,CASE WHEN rsSource.F00129 <> 9 THEN '490@'  + rsSource.F00129
		      ELSE '' END AS CONDITION_SOURCE_CONCEPT_ID                                                                          /*490*/ 
        ,ISNULL(rsSource.F00129, '') AS CONDITION_STATUS_SOURCE_VALUE                                                             /*490*/ 
	    ,ISNULL(HSP.F00006, '') AS MRN
	    ,CASE WHEN format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') is NULL
             then FORmat(GETDATE(), 'yyyy-MM-dd HH:mm:ss')  
	         else format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') 
		  end AS modified_dtTm
 FROM UNM_CNExTCases.dbo.Tumor rsSource
 JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.uk = rsSource.fk1
 JOIN UNM_CNExTCases.dbo.Stage rsTarget ON rsTarget.uk = rsSource.uk
 JOIN UNM_CNExTCases.dbo.Hospital HSP ON rsSource.uk = HSP.fk2
 INNER JOIN  UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK = HExt.UK
 WHERE HSP.F00006 not in(999999998, 9999998, 999999, 9999)
   and HSP.F00006 >= 1000
   and HExt.F00084 >= @fromDate
  ORDER BY rsSource.uk DESC