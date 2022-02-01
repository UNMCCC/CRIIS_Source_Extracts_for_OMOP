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

SELECT  'CNEXT TREATMENT(OMOP_PROCEDURE_OCCURRENCE)' AS IDENTITY_CONTEXT                                                                              /*'UNMTR DX/STG RECORD'*/
         ,rsSource.uk  AS SOURCE_PK
         ,rsSource.uk AS PROCEDURE_OCCURRENCE_ID
         ,(SELECT TOP 1 rsTarget.F00004 FROM UNM_CNExTCases.dbo.PatExtended rsTarget WHERE rsTarget.uk =  rsSource.fk1 Order By rsTarget.UK ASC) AS PERSON_ID  /*20*/
         ,'740@'  + TRT.F00420 AS PROCEDURE_CONCEPT_ID                                                                                                                   /*740*/
         ,TRY_CAST(F00422 AS DATE) AS PROCEDURE_DATE                                                                                                           /*1280*/
         ,TRY_CAST(F00422 AS DATETIME) AS PROCEDURE_DATETIME
    	 ,'1791@32' AS PROCEDURE_TYPE_CONCEPT_ID
         ,0 AS MODIFIER_CONCEPT_ID
         ,1 AS QUANTITY
         ,(SELECT TOP 1 rsTarget.F05162 FROM UNM_CNExTCases.dbo.DxStg rsTarget WHERE rsSource.uk = rsTarget.fk2 Order By rsTarget.UK ASC) AS PROVIDER_ID           /*2460*/
         ,(SELECT TOP 1 rsTarget.UK FROM UNM_CNExTCases.dbo.Patient rsTarget WHERE rsTarget.uk =  rsSource.fk1 Order By rsTarget.UK ASC) AS VISIT_OCCURRENCE_ID
         ,rsSource.uk AS VISIT_DETAIL_ID
         ,TRT.F00420 AS PROCEDURE_SOURCE_VALUE                                                                                                                                                                                                /*740*/
         ,0 AS PROCEDURE_SOURCE_CONCEPT_ID
         ,0 AS MODIFIER_SOURCE_VALUE
		 ,HSP.F00006 AS MRN
  FROM UNM_CNExTCases.dbo.Tumor rsSource
  JOIN UNM_CNExTCases.dbo.Treatment TRT on TRT.fk1 = rsSource.UK
  JOIN UNM_CNExTCases.dbo.Hospital HSP ON HSP.fk2 = rsSource.uk
  where TRT.F00420 NOT IN ('00','09')
UNION ALL
SELECT TOP 1000 'CNEXT TREATMENT(OMOP_PROCEDURE_OCCURRENCE)' AS IDENTITY_CONTEXT                                                                              /*'UNMTR SURGICAL RECORD'*/
        ,rsSource.uk AS SOURCE_PK
        ,rsSource.UK AS PROCEDURE_OCCURRENCE_ID
        ,(SELECT TOP 1 rsTarget.F00004 FROM UNM_CNExTCases.dbo.PatExtended rsTarget WHERE rsTarget.uk =  rsSource.fk1 Order By rsTarget.UK ASC) AS PERSON_ID  /*20*/
        ,'670@'  + SRG.F03488 AS PROCEDURE_CONCEPT_ID                                                                                                         /*670*/
        ,TRY_CAST(SRG.F00434 AS DATE)  AS PROCEDURE_DATE             /*1200*/ 
    	,TRY_CAST(SRG.F00434 AS DATETIME) AS PROCEDURE_DATETIME     /*1200*/
    	,'1791@32' AS PROCEDURE_TYPE_CONCEPT_ID
	    ,0 AS MODIFIER_CONCEPT_ID
		,1 AS QUANTITY
    	,SRG.F05161 AS PROVIDER_ID                          /*2480*/
        ,(SELECT TOP 1 rsTarget.UK FROM UNM_CNExTCases.dbo.Patient rsTarget WHERE rsTarget.uk =  rsSource.fk1 Order By rsTarget.UK ASC) AS VISIT_OCCURRENCE_ID
        ,rsSource.uk AS VISIT_DETAIL_ID
        ,SRG.F03488 AS PROCEDURE_SOURCE_VALUE                                                                                                                                                                                                /*740*/
	    ,0 AS PROCEDURE_SOURCE_CONCEPT_ID
	    ,0 AS MODIFIER_SOURCE_VALUE
		,HSP.F00006 AS MRN
   FROM UNM_CNExTCases.dbo.Tumor rsSource
   JOIN UNM_CNExTCases.dbo.Treatment TRT on TRT.fk1 = rsSource.UK
   JOIN UNM_CNExTCases.dbo.Surg SRG ON SRG.fk2 = rsSource.uk
   JOIN UNM_CNExTCases.dbo.Hospital HSP ON HSP.fk2 = rsSource.uk
WHERE SRG.F03488 != '00' 
    AND SRG.F03488 < '98'
UNION ALL
SELECT TOP 1000 'CNEXT TREATMENT(OMOP_PROCEDURE_OCCURRENCE)' AS IDENTITY_CONTEXT                                                                              /*'UNMTR RADIATION RECORD'*/
        ,rsSource.uk AS SOURCE_PK
        ,rsSource.UK AS PROCEDURE_OCCURRENCE_ID
        ,(SELECT TOP 1 rsTarget.F00004 FROM UNM_CNExTCases.dbo.PatExtended rsTarget WHERE rsTarget.uk =  rsSource.fk1 Order By rsTarget.UK ASC) AS PERSON_ID  /*20*/
        ,'1506@'  + RAD.F07799 AS PROCEDURE_CONCEPT_ID                                                                                                        /*1506*/
        ,TRY_CAST(RAD.F05187 AS DATE)  AS PROCEDURE_DATE             /*1210*/ 
    	,TRY_CAST(RAD.F05187 AS DATETIME) AS PROCEDURE_DATETIME     /*1210*/
    	,'1791@32' AS PROCEDURE_TYPE_CONCEPT_ID
	    ,0 AS MODIFIER_CONCEPT_ID
		,1 AS QUANTITY
    	,RAD.F05156 AS PROVIDER_ID                          /*2480*/
        ,(SELECT TOP 1 rsTarget.UK FROM UNM_CNExTCases.dbo.Patient rsTarget WHERE rsTarget.uk =  rsSource.fk1 Order By rsTarget.UK ASC) AS VISIT_OCCURRENCE_ID
        ,rsSource.uk AS VISIT_DETAIL_ID
        ,RAD.F07799 AS PROCEDURE_SOURCE_VALUE                                                                                                                                                                                                /*740*/
	    ,0 AS PROCEDURE_SOURCE_CONCEPT_ID
	    ,0 AS MODIFIER_SOURCE_VALUE
		,HSP.F00006 AS MRN
   FROM UNM_CNExTCases.dbo.Tumor rsSource
  JOIN UNM_CNExTCases.dbo.Treatment TRT on TRT.fk1 = rsSource.UK
  INNER JOIN UNM_CNExTCases.dbo.Radiation RAD ON RAD.fk2 = rsSource.uk
  JOIN UNM_CNExTCases.dbo.Hospital HSP ON HSP.fk2 = rsSource.uk
  WHERE F07799 > '00'
UNION ALL
SELECT TOP 1000 'CNEXT TREATMENT(OMOP_PROCEDURE_OCCURRENCE)' AS IDENTITY_CONTEXT                                                                               /*'UNMTR OTHER RECORD'*/
        ,rsSource.uk SOURCE_PK
        ,rsSource.UK AS PROCEDURE_OCCURRENCE_ID
        ,(SELECT TOP 1 rsTarget.F00004 FROM UNM_CNExTCases.dbo.PatExtended rsTarget WHERE rsTarget.uk =  rsSource.fk1 Order By rsTarget.UK ASC) AS PERSON_ID  /*20*/
        ,'730@'  + OTH.F05069 AS PROCEDURE_CONCEPT_ID                                                                                                         /*730*/
        ,TRY_CAST(OTH.F05195 AS DATE)  AS PROCEDURE_DATE             /*1250*/ 
    	,TRY_CAST(OTH.F05195 AS DATETIME) AS PROCEDURE_DATETIME     /*1250*/
    	,'1791@32' AS PROCEDURE_TYPE_CONCEPT_ID
	    ,0 AS MODIFIER_CONCEPT_ID
		,1 AS QUANTITY
    	,OTH.F05160 AS PROVIDER_ID                          /*2460*/
        ,(SELECT TOP 1 rsTarget.UK FROM UNM_CNExTCases.dbo.Patient rsTarget WHERE rsTarget.uk =  rsSource.fk1 Order By rsTarget.UK ASC) AS VISIT_OCCURRENCE_ID
        ,rsSource.uk AS VISIT_DETAIL_ID
        ,OTH.F05069 AS PROCEDURE_SOURCE_VALUE                                                                                                                                                                                                /*740*/
	    ,0 AS PROCEDURE_SOURCE_CONCEPT_ID
	    ,0 AS MODIFIER_SOURCE_VALUE
		,HSP.F00006 AS MRN
   FROM UNM_CNExTCases.dbo.Tumor rsSource
   JOIN UNM_CNExTCases.dbo.Treatment TRT on TRT.fk1 = rsSource.UK
   JOIN UNM_CNExTCases.dbo.Other OTH ON OTH.fk2 = rsSource.uk
   JOIN UNM_CNExTCases.dbo.Hospital HSP ON HSP.fk2 = rsSource.uk
WHERE OTH.F05069 in ( '1','2','3','6')