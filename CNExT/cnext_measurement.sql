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

*/

SELECT  'CNEXT TUMOR(OMOP_MEASUREMENT)' AS IDENTITY_CONTEXT
        ,rsSource.uk AS SOURCE_PK
        ,rsSource.uk AS MEASUREMENT_ID
    	,(SELECT TOP 1 rsTarget.F00004 FROM UNM_CNExTCases.dbo.PatExtended rsTarget WHERE rsTarget.uk =  rsSource.fk1 Order By rsTarget.UK ASC) AS PERSON_ID  /*20*/ 
		,STUFF(rsSource.F00152,4,0,'.') AS MEASUREMENT_CONCEPT_ID_SITE                                                                                                      /*400*/ 
        ,STUFF(rsSource.F02503,5,0,'/') AS MEASUREMENT_CONCEPT_ID_MORPH                                                                                                     /*521*/ 
        ,rsTarget.F07625 AS MEASUREMENT_CONCEPT_ID_GRADE_PATHOLOGICAL                                                                                                    /*3844*/ 
        ,TRY_CAST(rsSource.F00029 AS DATE) AS MEASUREMENT_DATE                                                                                         /*390*/ 
        ,TRY_CAST(rsSource.F00029 AS DATETIME) AS MEASUREMENT_DATETIME                                                                                 /*390*/
        ,TRY_CAST(rsSource.F00029 AS TIME) AS MEASUREMENT_TIME                                                                                         /*390*/  
	    ,'1791@32' AS MEASUREMENT_TYPE_CONCEPT_ID
	    ,NULL AS OPERATOR_CONCEPT_ID
		,rsSource.F02503 AS VALUE_AS_NUMBER                                                                                                                         /*521*/ 
        ,STUFF(rsSource.F02503,5,0,'/') AS VALUE_AS_CONCEPT_ID                                                                                            /*521*/ 
		,NULL AS UNIT_CONCEPT_ID
		,NULL AS RANGE_LOW
		,NULL AS RANGE_HIGH
		,(SELECT TOP 1 rsTarget.F05162 FROM UNM_CNExTCases.dbo.DxStg rsTarget WHERE rsSource.uk = rsTarget.fk2 Order By rsTarget.UK ASC) AS PROVIDER_ID     /*2460*/
		,rsSource.fk1 AS VISIT_OCCURRENCE_ID
        ,rsSource.uk AS VISIT_DETAIL_ID 
        ,rsTarget.F07625 AS MEASUREMENT_SOURCE_VALUE                                                                                                    /*3844*/ 
		,'3844@' + rsTarget.F07625 AS MEASUREMENT_SOURCE_CONCEPT_ID
        ,0 AS UNIT_SOURCE_VALUE
        ,STUFF(rsSource.F02503,5,0,'/') AS VALUE_SOURCE_VALUE                                                                                           /*521*/
		,HSP.F00006 AS MRN
 FROM UNM_CNExTCases.dbo.Tumor rsSource
JOIN UNM_CNExTCases.dbo.Stage rsTarget ON rsTarget.uk = rsSource.uk
JOIN UNM_CNExTCases.dbo.Hospital HSP ON HSP.fk2 = rsSource.uk
order by rsSource.uk desc