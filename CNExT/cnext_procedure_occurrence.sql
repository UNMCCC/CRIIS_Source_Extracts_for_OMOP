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
LTV - 2/7/2022 - handled NULL values with the ISNULL function. Join from Treatment table to Tumor corrected in 1st select statement and removed
                 from all others. 
LTV - 2/8/2022 - handled empty column values where a satic value is added to them so that nothing would be returned. Change predicate for 3rd unioned select statement to 
	             exclude rows where RAD.F07799 would be NULL, empty, '00', or '99'.
LTV - 2/22/2022 - Changed condition on the SURG table from SRG.F03488 != '00' to SRG.F03488 > '00' per Mark to avoid NULL and empty space values.
LTV - 2/22/2022 - Changed condition on the Radiation table. Added WHERE F05257 > '000'.

*/
SET NOCOUNT ON;
SELECT 'IDENTITY_CONTEXT|SOURCE_PK|PROCEDURE_OCCURRENCE_ID|PERSON_ID|PROCEDURE_CONCEPT_ID|PROCEDURE_DATE|PROCEDURE_DATETIME|PROCEDURE_TYPE_CONCEPT_ID|MODIFIER_CONCEPT_ID|QUANTITY|PROVIDER_ID|VISIT_OCCURRENCE_ID|VISIT_DETAIL_ID|PROCEDURE_SOURCE_VALUE|PROCEDURE_SOURCE_CONCEPT_ID|MODIFIER_SOURCE_VALUE|MRN|Modified_DtTm';
SELECT  'CNEXT TREATMENT(OMOP_PROCEDURE_OCCURRENCE)' AS IDENTITY_CONTEXT                                                                           /*'UNMTR DX/STG RECORD'*/
         ,rsSource.uk  AS SOURCE_PK
         ,rsSource.uk AS PROCEDURE_OCCURRENCE_ID
         ,(SELECT TOP 1 ISNULL(rsTarget.F00004, '') FROM UNM_CNExTCases.dbo.PatExtended rsTarget WHERE rsTarget.uk =  rsSource.fk1 Order By rsTarget.UK ASC) AS PERSON_ID  /*20*/
         ,'740@'  + TRT.F00420 AS PROCEDURE_CONCEPT_ID   --nulls handled by predicate; no empty space values  /*740*/
         ,ISNULL(FORMAT(TRY_CAST(F00422 AS DATE), 'yyyy-MM-dd'), '') AS PROCEDURE_DATE                        /*1280*/
         ,ISNULL(FORMAT(TRY_CAST(F00422 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '') AS PROCEDURE_DATETIME
    	 ,'1791@32' AS PROCEDURE_TYPE_CONCEPT_ID
         ,0 AS MODIFIER_CONCEPT_ID
         ,1 AS QUANTITY
         ,(SELECT TOP 1 ISNULL(rsTarget.F05162, '') FROM UNM_CNExTCases.dbo.DxStg rsTarget WHERE rsSource.uk = rsTarget.fk2 Order By rsTarget.UK ASC) AS PROVIDER_ID     /*2460*/
         ,(SELECT TOP 1 rsTarget.UK FROM UNM_CNExTCases.dbo.Patient rsTarget WHERE rsTarget.uk =  rsSource.fk1 Order By rsTarget.UK ASC) AS VISIT_OCCURRENCE_ID
         ,rsSource.uk AS VISIT_DETAIL_ID
         ,TRT.F00420 AS PROCEDURE_SOURCE_VALUE   --nulls handled by predicate; no empty space values      /*740*/
         ,0 AS PROCEDURE_SOURCE_CONCEPT_ID
         ,0 AS MODIFIER_SOURCE_VALUE
		 ,ISNULL(HSP.F00006, '') AS MRN
		 ,isNULL(format(TRY_CAST(HExt.F00084 as datetime),'yyyy-mm-dd HH:mm:ss'),'')  AS modified_dtTm
  FROM UNM_CNExTCases.dbo.Tumor rsSource
  JOIN UNM_CNExTCases.dbo.Treatment TRT on TRT.UK = rsSource.UK--TRT.fk1 = rsSource.UK
  JOIN UNM_CNExTCases.dbo.Hospital HSP on HSP.FK2=rsSource.UK
  JOIN  UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK=HExt.UK

  where TRT.F00420 NOT IN ('00','09')
UNION ALL
SELECT 'CNEXT SURG(OMOP_PROCEDURE_OCCURRENCE)' AS IDENTITY_CONTEXT                                                                   /*'UNMTR SURGICAL RECORD'*/
        ,rsSource.uk AS SOURCE_PK
        ,rsSource.UK AS PROCEDURE_OCCURRENCE_ID
        ,(SELECT TOP 1 ISNULL(rsTarget.F00004, '') FROM UNM_CNExTCases.dbo.PatExtended rsTarget WHERE rsTarget.uk =  rsSource.fk1 Order By rsTarget.UK ASC) AS PERSON_ID  /*20*/
        ,CASE                                  --no nulls, but empty space values to handle
			WHEN SRG.F03488 <> ''
			THEN '670@'  + SRG.F03488
			ELSE ''
		 END AS PROCEDURE_CONCEPT_ID                                                                          /*670*/
        ,ISNULL(FORMAT(TRY_CAST(SRG.F00434 AS DATE), 'yyyy-MM-dd'), '')  AS PROCEDURE_DATE                    /*1200*/ 
    	,ISNULL(FORMAT(TRY_CAST(SRG.F00434 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '') AS PROCEDURE_DATETIME     /*1200*/
    	,'1791@32' AS PROCEDURE_TYPE_CONCEPT_ID
	    ,0 AS MODIFIER_CONCEPT_ID
		,1 AS QUANTITY
    	,ISNULL(SRG.F05161, '') AS PROVIDER_ID                                            /*2480*/
        ,(SELECT TOP 1 rsTarget.UK FROM UNM_CNExTCases.dbo.Patient rsTarget WHERE rsTarget.uk =  rsSource.fk1 Order By rsTarget.UK ASC) AS VISIT_OCCURRENCE_ID
        ,rsSource.uk AS VISIT_DETAIL_ID
        ,SRG.F03488 AS PROCEDURE_SOURCE_VALUE    --nulls handled by predicate             /*740*/
	    ,0 AS PROCEDURE_SOURCE_CONCEPT_ID
	    ,0 AS MODIFIER_SOURCE_VALUE
		,ISNULL(HSP.F00006, '') AS MRN
		,isNULL(format(TRY_CAST(HExt.F00084 as datetime),'yyyy-mm-dd HH:mm:ss'),'')  AS modified_dtTm
   FROM UNM_CNExTCases.dbo.Tumor rsSource
   JOIN UNM_CNExTCases.dbo.Surg SRG ON SRG.fk2 = rsSource.uk
   JOIN UNM_CNExTCases.dbo.Hospital HSP ON HSP.fk2 = rsSource.uk
   JOIN  UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK=HExt.UK

WHERE SRG.F03488 > '00' 
    AND SRG.F03488 < '98'
UNION ALL
SELECT 'CNEXT RADIATION(OMOP_PROCEDURE_OCCURRENCE)' AS IDENTITY_CONTEXT                                                                  /*'UNMTR RADIATION RECORD'*/
        ,rsSource.uk AS SOURCE_PK
        ,rsSource.UK AS PROCEDURE_OCCURRENCE_ID
        ,(SELECT TOP 1 ISNULL(rsTarget.F00004, '') FROM UNM_CNExTCases.dbo.PatExtended rsTarget WHERE rsTarget.uk =  rsSource.fk1 Order By rsTarget.UK ASC) AS PERSON_ID  /*20*/
       	,'1506@'  + RAD.F07799 AS PROCEDURE_CONCEPT_ID   --handled nulls, empty spaces, and values '00' and '99' in predicate             /*1506*/
		,ISNULL(FORMAT(TRY_CAST(RAD.F05187 AS DATE),'yyyy-MM-dd'), '') AS PROCEDURE_DATE                                                  /*1210*/ 
    	,ISNULL(FORMAT(TRY_CAST(RAD.F05187 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '') AS PROCEDURE_DATETIME                                 /*1210*/
    	,'1791@32' AS PROCEDURE_TYPE_CONCEPT_ID
	    ,0 AS MODIFIER_CONCEPT_ID
		,1 AS QUANTITY
    	,ISNULL(RAD.F05156, '') AS PROVIDER_ID                                             /*2480*/
        ,(SELECT TOP 1 rsTarget.UK FROM UNM_CNExTCases.dbo.Patient rsTarget WHERE rsTarget.uk =  rsSource.fk1 Order By rsTarget.UK ASC) AS VISIT_OCCURRENCE_ID
        ,rsSource.uk AS VISIT_DETAIL_ID
        ,RAD.F07799 AS PROCEDURE_SOURCE_VALUE    --nulls handled by predicate              /*740*/
	    ,0 AS PROCEDURE_SOURCE_CONCEPT_ID
	    ,0 AS MODIFIER_SOURCE_VALUE
		,ISNULL(HSP.F00006, '') AS MRN
	 ,isNULL(format(TRY_CAST(HExt.F00084 as datetime),'yyyy-mm-dd HH:mm:ss'),'')  AS modified_dtTm
   FROM UNM_CNExTCases.dbo.Tumor rsSource
   INNER JOIN UNM_CNExTCases.dbo.Radiation RAD ON RAD.fk2 = rsSource.uk
   JOIN UNM_CNExTCases.dbo.Hospital HSP ON HSP.fk2 = rsSource.uk
   JOIN  UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK=HExt.UK

  WHERE F05257 > '000' 
    AND F07799 > '00'
    AND F07799 < '99'
UNION ALL
SELECT 'CNEXT OTHER(OMOP_PROCEDURE_OCCURRENCE)' AS IDENTITY_CONTEXT                                                                 /*'UNMTR OTHER RECORD'*/
        ,rsSource.uk SOURCE_PK
        ,rsSource.UK AS PROCEDURE_OCCURRENCE_ID
        ,(SELECT TOP 1 ISNULL(rsTarget.F00004, '') FROM UNM_CNExTCases.dbo.PatExtended rsTarget WHERE rsTarget.uk =  rsSource.fk1 Order By rsTarget.UK ASC) AS PERSON_ID  /*20*/
        ,'730@'  + OTH.F05069 AS PROCEDURE_CONCEPT_ID    --empty spaces and null values handled by predicate          /*730*/
        ,ISNULL(FORMAT(TRY_CAST(OTH.F05195 AS DATE), 'yyyy-MM-dd'), '')  AS PROCEDURE_DATE                            /*1250*/ 
    	,ISNULL(FORMAT(TRY_CAST(OTH.F05195 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '') AS PROCEDURE_DATETIME             /*1250*/
    	,'1791@32' AS PROCEDURE_TYPE_CONCEPT_ID
	    ,0 AS MODIFIER_CONCEPT_ID
		,1 AS QUANTITY
    	,ISNULL(OTH.F05160, '') AS PROVIDER_ID                                      /*2460*/
        ,(SELECT TOP 1 rsTarget.UK FROM UNM_CNExTCases.dbo.Patient rsTarget WHERE rsTarget.uk =  rsSource.fk1 Order By rsTarget.UK ASC) AS VISIT_OCCURRENCE_ID
        ,rsSource.uk AS VISIT_DETAIL_ID
        ,OTH.F05069 AS PROCEDURE_SOURCE_VALUE   --nulls handled by predicate        /*740*/
	    ,0 AS PROCEDURE_SOURCE_CONCEPT_ID
	    ,0 AS MODIFIER_SOURCE_VALUE
		,ISNULL(HSP.F00006, '') AS MRN
        ,isNULL(format(TRY_CAST(HExt.F00084 as datetime),'yyyy-mm-dd HH:mm:ss'),'')  AS modified_dtTm
   FROM UNM_CNExTCases.dbo.Tumor rsSource
   JOIN UNM_CNExTCases.dbo.Other OTH ON OTH.fk2 = rsSource.uk
   JOIN UNM_CNExTCases.dbo.Hospital HSP ON HSP.fk2 = rsSource.uk
   JOIN  UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK=HExt.UK

WHERE OTH.F05069 in ( '1','2','3','6')