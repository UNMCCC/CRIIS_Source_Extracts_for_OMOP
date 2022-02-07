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

LTV - 2/7/2022 - handled NULL values with the ISNULL function. Replaced NULL selections with empty ticks.

*/

SELECT  'CNEXT TUMOR(OMOP_NOTES)' AS IDENTITY_CONTEXT
        ,rsSource.uk AS SOURCE_PK
        ,rsSource.uk AS NOTE_ID
        ,(SELECT TOP 1 ISNULL(rsTarget.F00004, '') FROM UNM_CNExTCases.dbo.PatExtended rsTarget WHERE rsTarget.uk =  rsSource.fk1 Order By rsTarget.UK ASC) AS PERSON_ID  /*20*/ 
        ,rsSource.uk AS NOTE_EVENT_ID
		,'' AS NOTE_EVENT_FIELD_CONCEPT_ID
        ,ISNULL(FORMAT(TRY_CAST(F00029 AS DATE), 'yyyy-MM-dd'), '') AS NOTE_DATE                                                                                               /*443*/ 
        ,ISNULL(FORMAT(TRY_CAST(F00029 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '') AS NOTE_DATETIME                                                                                /*443*/ 
		,'1791@32' AS NOTE_TYPE_CONCEPT_ID
		,'' AS NOTE_CLASS_CONCEPT_ID
	    ,CASE WHEN F01220 IS NOT NULL THEN 'Text_Follow_Up_Notes' 
		          ELSE '' END AS NOTE_TITLE_1
        ,CASE WHEN F01220 IS NOT NULL THEN F01220 
		          ELSE '' END AS  NOTE_TEXT_1                                                                                                /*2680*/ 
		,CASE WHEN F01506 IS NOT NULL THEN 'Text_Follow_Up_Remarks' 
		          ELSE '' END AS NOTE_TITLE_2
        ,CASE WHEN F01506 IS NOT NULL THEN F01506 
		          ELSE '' END AS  NOTE_TEXT_2                                                                                                /*2580*/
		,'' AS NOTE_TITLE_3
        ,''  NOTE_TEXT_3
		,'' AS NOTE_TITLE_4
        ,''  NOTE_TEXT_4
		,'' AS NOTE_TITLE_5
        ,''  NOTE_TEXT_5
		,'' AS NOTE_TITLE_6
        ,''  NOTE_TEXT_6
		,'' AS NOTE_TITLE_7
        ,''  NOTE_TEXT_7
		,'' AS NOTE_TITLE_8
        ,''  NOTE_TEXT_8
		,'' AS NOTE_TITLE_9
        ,''  NOTE_TEXT_9
		,'UTF-8 (32678)' AS ENCODING_CONCEPT_ID
	    ,'4182347' AS LANGUAGE_CONCEPT_ID
        ,(SELECT TOP 1 ISNULL(HEX.F00675, '') FROM UNM_CNExTCases.dbo.Hospital HSP
                                  JOIN UNM_CNExTCases.dbo.HospExtended HEX ON HEX.UK = HSP.UK 
								 WHERE HSP.FK2 = rsSource.UK ORDER BY HEX.UK ASC) AS PROVIDER_ID                                              /*2460*/
		,ISNULL(rsSource.fk1, '') AS VISIT_OCCURRENCE_ID
        ,rsSource.uk AS VISIT_DETAIL_ID 
        ,'UNM_CNExTCases.dbo.Tumor.uk' AS NOTE_SOURCE_VALUE
        ,(SELECT TOP 1 ISNULL(rsTarget.F00016, '') FROM UNM_CNExTCases.dbo.Hospital rsTarget WHERE rsTarget.fk2 = rsSource.UK  Order By  rsTarget.fk2 ASC) AS ACCESSION_NUMBER  /*550*/
        ,(SELECT TOP 1 ISNULL(rsTarget.F00006, '') FROM UNM_CNExTCases.dbo.Hospital rsTarget WHERE rsTarget.fk2 = rsSource.UK  Order By  rsTarget.fk2 ASC) AS MRN
 FROM UNM_CNExTCases.dbo.Tumor rsSource
  JOIN UNM_CNExTCases.dbo.FollowUp rsTarget ON rsTarget.uk = rsSource.uk
  union all
  SELECT TOP 1000 'CNEXT TUMOR(OMOP_NOTES)' AS IDENTITY_CONTEXT
        ,rsSource.uk AS SOURCE_PK
        ,rsSource.uk AS NOTE_ID
        ,(SELECT TOP 1 ISNULL(rsTarget.F00004, '') FROM UNM_CNExTCases.dbo.PatExtended rsTarget WHERE rsTarget.uk =  rsSource.fk1 Order By rsTarget.UK ASC) AS PERSON_ID  /*20*/ 
        ,rsSource.uk AS NOTE_EVENT_ID
		,'' AS NOTE_EVENT_FIELD_CONCEPT_ID
        ,ISNULL(FORMAT(TRY_CAST(F00029 AS DATE), 'yyyy-MM-dd'), '') AS NOTE_DATE                                                                                               /*443*/ 
        ,ISNULL(FORMAT(TRY_CAST(F00029 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '') AS NOTE_DATETIME                                                                                       /*443*/ 
		,'1791@32' AS NOTE_TYPE_CONCEPT_ID
		,'' AS NOTE_CLASS_CONCEPT_ID
	    ,CASE WHEN F00030 IS NOT NULL THEN 'Text_Final_Dx' 
		          ELSE '' END AS NOTE_TITLE_1
        ,CASE WHEN F00030 IS NOT NULL THEN F00030 
		          ELSE '' END AS  NOTE_TEXT_1                                                                                                /*2680*/ 
		,CASE WHEN F00089 IS NOT NULL THEN 'Text_Primary_Site' 
		          ELSE '' END AS NOTE_TITLE_2
        ,CASE WHEN F00089 IS NOT NULL THEN F00089 
		          ELSE '' END AS  NOTE_TEXT_2                                                                                                /*2580*/
		,CASE WHEN F00090 IS NOT NULL THEN 'Text_Histology' 
		          ELSE '' END AS NOTE_TITLE_3
        ,CASE WHEN F00090 IS NOT NULL THEN F00090 
		          ELSE '' END AS  NOTE_TEXT_3                                                                                                /*2590*/
		,CASE WHEN F01209 IS NOT NULL THEN 'Text_Scopes' 
		          ELSE '' END AS NOTE_TITLE_4
        ,CASE WHEN F01209 IS NOT NULL THEN F01209 
		          ELSE '' END AS  NOTE_TEXT_4                                                                                                /*2540*/
		,CASE WHEN F01210 IS NOT NULL THEN 'Text_Labs' 
		          ELSE '' END AS NOTE_TITLE_5
        ,CASE WHEN F01210 IS NOT NULL THEN F01210 
		          ELSE '' END AS  NOTE_TEXT_5                                                                                                /*2550*/
		,CASE WHEN F01211 IS NOT NULL THEN 'Text_Physical_Exam' 
		          ELSE '' END AS NOTE_TITLE_6
        ,CASE WHEN F01211 IS NOT NULL THEN F01211 
		          ELSE '' END AS  NOTE_TEXT_6                                                                                                /*2520*/
		,CASE WHEN F01212 IS NOT NULL THEN 'Text_Xrays_Scans' 
		          ELSE '' END AS NOTE_TITLE_7
        ,CASE WHEN F01212 IS NOT NULL THEN F01212 
		          ELSE '' END AS  NOTE_TEXT_7                                                                                                /*2530*/
		,CASE WHEN F01213 IS NOT NULL THEN 'Text_Pathology' 
		          ELSE '' END AS NOTE_TITLE_8
        ,CASE WHEN F01213 IS NOT NULL THEN F01213 
		          ELSE '' END AS  NOTE_TEXT_8                                                                                                /*2570*/
		,CASE WHEN F01214 IS NOT NULL THEN 'Text_Operative_Findings' 
		          ELSE '' END AS NOTE_TITLE_9
        ,CASE WHEN F01214 IS NOT NULL THEN F01214 
		          ELSE '' END AS  NOTE_TEXT_9                                                                                                /*2560*/
	    ,'UTF-8 (32678)' AS ENCODING_CONCEPT_ID
	    ,'4182347' AS LANGUAGE_CONCEPT_ID
        ,(SELECT TOP 1 ISNULL(HEX.F00675, '') FROM UNM_CNExTCases.dbo.Hospital HSP
                                  JOIN UNM_CNExTCases.dbo.HospExtended HEX ON HEX.UK = HSP.UK 
								 WHERE HSP.FK2 = rsSource.UK ORDER BY HEX.UK ASC) AS PROVIDER_ID                                              /*2460*/
		,ISNULL(rsSource.fk1, '') AS VISIT_OCCURRENCE_ID
        ,rsSource.uk AS VISIT_DETAIL_ID 
        ,'UNM_CNExTCases.dbo.Tumor.uk' AS NOTE_SOURCE_VALUE
        ,(SELECT TOP 1 ISNULL(rsTarget.F00016, '') FROM UNM_CNExTCases.dbo.Hospital rsTarget WHERE rsTarget.fk2 = rsSource.UK  Order By  rsTarget.fk2 ASC) AS ACCESSION_NUMBER  /*550*/
        ,(SELECT TOP 1 ISNULL(rsTarget.F00006, '') FROM UNM_CNExTCases.dbo.Hospital rsTarget WHERE rsTarget.fk2 = rsSource.UK  Order By  rsTarget.fk2 ASC) AS MRN
 FROM UNM_CNExTCases.dbo.Tumor rsSource
 UNION ALL
SELECT TOP 1000 'CNEXT TUMOR(OMOP_NOTES)' AS IDENTITY_CONTEXT
        ,rsSource.uk AS SOURCE_PK
        ,rsSource.uk AS NOTE_ID
        ,(SELECT TOP 1 ISNULL(rsTarget.F00004, '') FROM UNM_CNExTCases.dbo.PatExtended rsTarget WHERE rsTarget.uk =  rsSource.fk1 Order By rsTarget.UK ASC) AS PERSON_ID  /*20*/ 
        ,rsSource.uk AS NOTE_EVENT_ID
		,'' AS NOTE_EVENT_FIELD_CONCEPT_ID
        ,ISNULL(FORMAT(TRY_CAST(F00029 AS DATE), 'yyyy-MM-dd'), '') AS NOTE_DATE                                                                                                                       /*443*/ 
        ,ISNULL(FORMAT(TRY_CAST(F00029 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '') AS NOTE_DATETIME                                                                                                               /*443*/ 
		,'1791@32' AS NOTE_TYPE_CONCEPT_ID
		,'' AS NOTE_CLASS_CONCEPT_ID
        ,CASE WHEN CAST(F01215 AS VARCHAR(100)) IS NOT NULL AND CAST(F01215 AS VARCHAR(100)) !=  ' ' THEN 'Text_Radiation_Beam_Summary' 
		          ELSE '' END AS NOTE_TITLE_1
        ,CASE WHEN F01215 IS NOT NULL THEN F01215 
		          ELSE '' END AS  NOTE_TEXT_1
        ,CASE WHEN F01216 IS NOT NULL AND CAST(F01216 AS VARCHAR(100)) !=  ' ' THEN 'Text_Chemotherapy_Summary' 
		          ELSE '' END AS NOTE_TITLE_2
        ,CASE WHEN F01216 IS NOT NULL THEN F01216 
		          ELSE '' END AS  NOTE_TEXT_2
        ,CASE WHEN F01217 IS NOT NULL  AND CAST(F01217 AS VARCHAR(100)) !=  ' ' THEN 'Text_Hormone_Summary' 
		          ELSE '' END AS NOTE_TITLE_3
        ,CASE WHEN F01217 IS NOT NULL THEN F01217 
		          ELSE '' END AS  NOTE_TEXT_3
        ,CASE WHEN F01218 IS NOT NULL  AND CAST(F01218 AS VARCHAR(100)) !=  ' ' THEN 'Text_Immunotherapy_Summary' 
		          ELSE '' END AS NOTE_TITLE_4
        ,CASE WHEN F01218 IS NOT NULL THEN F01218 
		          ELSE '' END AS  NOTE_TEXT_4
        ,CASE WHEN F01219 IS NOT NULL  AND CAST(F01219 AS VARCHAR(100)) !=  ' ' THEN 'Text_Other_Therapy_Summary' 
		          ELSE '' END AS NOTE_TITLE_5
        ,CASE WHEN F01219 IS NOT NULL THEN F01219 
		          ELSE '' END AS  NOTE_TEXT_5
        ,CASE WHEN F01223 IS NOT NULL  AND CAST(F01223 AS VARCHAR(100)) !=  ' ' THEN 'Text_Staging' 
		          ELSE '' END AS NOTE_TITLE_6
        ,CASE WHEN F01223 IS NOT NULL THEN F01223 
		          ELSE '' END AS  NOTE_TEXT_6
        ,CASE WHEN F01351 IS NOT NULL  AND CAST(F01351 AS VARCHAR(100)) !=  ' ' THEN 'Text_Surgery_Summary' 
		          ELSE '' END AS NOTE_TITLE_7
        ,CASE WHEN F01351 IS NOT NULL THEN F01351 
		          ELSE '' END AS  NOTE_TEXT_7
        ,CASE WHEN F01413 IS NOT NULL  AND CAST(F01413 AS VARCHAR(100)) !=  ' ' THEN 'Text_Before_1998' 
		          ELSE '' END AS NOTE_TITLE_8
        ,CASE WHEN F01413 IS NOT NULL THEN F01413 
		          ELSE '' END AS  NOTE_TEXT_8
        ,CASE WHEN F05952 IS NOT NULL  AND CAST(F05952 AS VARCHAR(100)) !=  ' ' THEN 'Text_Radiation_Other_Summary' 
		          ELSE '' END AS NOTE_TITLE_9
        ,CASE WHEN F05952 IS NOT NULL THEN F05952 
		          ELSE '' END AS  NOTE_TEXT_9
	    ,'UTF-8 (32678)' AS ENCODING_CONCEPT_ID
	    ,'4182347' AS LANGUAGE_CONCEPT_ID
        ,(SELECT TOP 1 ISNULL(HEX.F00675, '') FROM UNM_CNExTCases.dbo.Hospital HSP
                               JOIN UNM_CNExTCases.dbo.HospExtended HEX ON HEX.UK = HSP.UK WHERE HSP.FK2 = rsSource.UK ORDER BY HEX.UK ASC) AS PROVIDER_ID    /*2460*/
		,ISNULL(rsSource.fk1, '') AS VISIT_OCCURRENCE_ID
        ,rsSource.uk AS VISIT_DETAIL_ID 
        ,'UNM_CNExTCases.dbo.Tumor.uk' AS NOTE_SOURCE_VALUE
        ,(SELECT TOP 1 ISNULL(rsTarget.F00016, '') FROM UNM_CNExTCases.dbo.Hospital rsTarget WHERE rsTarget.fk2 = rsSource.UK  Order By  rsTarget.fk2 ASC) AS ACCESSION_NUMBER  /*550*/
        ,(SELECT TOP 1 ISNULL(rsTarget.F00006, '') FROM UNM_CNExTCases.dbo.Hospital rsTarget WHERE rsTarget.fk2 = rsSource.UK  Order By  rsTarget.fk2 ASC) AS MRN
 FROM UNM_CNExTCases.dbo.Tumor rsSource
  JOIN UNM_CNExTCases.dbo.Treatment TRT on TRT.UK = rsSource.UK--TRT.fk1 = rsSource.UK
WHERE TRT.F00420 NOT IN ('00','09')
   ORDER BY NOTE_TYPE_CONCEPT_ID
           ,rsSource.UK DESC