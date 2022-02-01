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

SELECT  'CNEXT TUMOR(OMOP_OBSERVATIONS)' AS IDENTITY_CONTEXT                                                                                         /*Family History*/
        ,rsSource.uk AS SOURCE_PK
        ,rsSource.uk AS OBSERVATION_ID
        ,(SELECT TOP 1 rsTarget.F00004 FROM UNM_CNExTCases.dbo.PatExtended rsTarget WHERE rsTarget.uk =  rsSource.fk1 Order By rsTarget.UK ASC) AS PERSON_ID  /*20*/ 
        ,F06433 AS OBSERVATION_CONCEPT_ID_1
	    ,' ' AS OBSERVATION_CONCEPT_ID_2
        ,' ' AS OBSERVATION_CONCEPT_ID_3
        ,' ' AS OBSERVATION_CONCEPT_ID_4
        ,' ' AS OBSERVATION_CONCEPT_ID_5 
        ,' ' AS OBSERVATION_CONCEPT_ID_6 
        ,' ' AS OBSERVATION_CONCEPT_ID_7
        ,' ' AS OBSERVATION_CONCEPT_ID_8
        ,' ' AS OBSERVATION_CONCEPT_ID_9
        ,' ' AS OBSERVATION_CONCEPT_ID_10
        ,TRY_CAST(rsSource.F00029 AS DATE) AS OBSERVATION_DATE
        ,TRY_CAST(rsSource.F00029 AS DATETIME) AS OBSERVATION_DATETIME
		,'1791@32' AS OBSERVATION_TYPE_CONCEPT_ID
	    ,NULL AS VALUE_AS_NUMBER
	    ,NULL AS VALUE_AS_STRING
        ,ISNULL(F06434,'') AS VALUE_AS_CONCEPT_ID_1
        ,ISNULL(F06435,'') AS QUALIFIER_CONCEPT_ID_1
        ,ISNULL(F06436,'') AS VALUE_AS_CONCEPT_ID_2
        ,ISNULL(F06437,'') AS QUALIFIER_CONCEPT_ID_2
        ,ISNULL(F06438,'') AS VALUE_AS_CONCEPT_ID_3
        ,ISNULL(F06439,'') AS QUALIFIER_CONCEPT_ID_3
	    ,NULL AS UNIT_CONCEPT_ID
		,(SELECT TOP 1 rsTarget.F05162 FROM UNM_CNExTCases.dbo.DxStg rsTarget WHERE rsSource.uk = rsTarget.fk2 Order By rsTarget.UK ASC) AS PROVIDER_ID     /*2460*/
		,rsSource.fk1 AS VISIT_OCCURRENCE_ID
        ,rsSource.uk AS VISIT_DETAIL_ID 
        ,STUFF(rsSource.F00152,4,0,'.') AS OBSERVATION_SOURCE_VALUE                                                                                                    /*400*/
		,0 AS OBSERVATION_SOURCE_CONCEPT_ID
        ,0 AS UNIT_SOURCE_VALUE	   
	    ,F06433 AS QUALIFIER_SOURCE_VALUE
	    ,rsSource.FK1 AS OBSERVATION_EVENT_ID
	    ,'UNM_CNExTCases.dbo.Tumor rsSource' AS OBS_EVENT_FIELD_CONCEPT_ID
	    ,NULL AS VALUE_AS_DATETIME
		,HSP.F00006 AS MRN
   FROM UNM_CNExTCases.dbo.Tumor rsSource
   JOIN UNM_CNExTCases.dbo.Hospital HSP ON HSP.fk2 = rsSource.uk
  WHERE F06433 IS NOT NULL
UNION ALL
SELECT TOP 1000 'CNEXT TUMOR(OMOP_OBSERVATIONS)' AS IDENTITY_CONTEXT                                                                                         /*'TUMOR REGISTRY COMORBIDITY RECORD'*/
           ,rsSource.uk AS SOURCE_PK
           ,rsSource.uk AS OBSERVATION_ID
           ,(SELECT TOP 1 rsTarget.F00004 FROM UNM_CNExTCases.dbo.PatExtended rsTarget WHERE rsTarget.uk =  rsSource.fk1 Order By rsTarget.UK ASC) AS PERSON_ID  /*20*/ 
           ,ISNULL(F03442,'') AS OBSERVATION_CONCEPT_ID_1
           ,ISNULL(F03443,'') AS OBSERVATION_CONCEPT_ID_2                                                                                                                                                                                             /*3120*/ 
           ,ISNULL(F03444,'') AS OBSERVATION_CONCEPT_ID_3                                                                                                                                                                                             /*3130*/ 
           ,ISNULL(F03445,'') AS OBSERVATION_CONCEPT_ID_4                                                                                                                                                                                             /*3140*/ 
           ,ISNULL(F03446,'') AS OBSERVATION_CONCEPT_ID_5                                                                                                                                                                                             /*3150*/ 
           ,ISNULL(F03447,'') AS OBSERVATION_CONCEPT_ID_6                                                                                                                                                                                             /*3160*/ 
           ,ISNULL(F04261,'') AS OBSERVATION_CONCEPT_ID_7                                                                                                                                                                                             /*3161*/ 
           ,ISNULL(F04262,'') AS OBSERVATION_CONCEPT_ID_8                                                                                                                                                                                             /*3162*/ 
           ,ISNULL(F04263,'') AS OBSERVATION_CONCEPT_ID_9                                                                                                                                                                                             /*3163*/ 
           ,ISNULL(F04264,'') AS OBSERVATION_CONCEPT_ID_10                                                                                                                                                                                           /*3164*/ 
           ,TRY_CAST(rsSource.F00029 AS DATE) AS OBSERVATION_DATE
           ,TRY_CAST(rsSource.F00029 AS DATETIME) AS OBSERVATION_DATETIME
		   ,'1791@32' AS OBSERVATION_TYPE_CONCEPT_ID
	       ,F03442 AS VALUE_AS_NUMBER
	       ,F03442 AS VALUE_AS_STRING
           ,ISNULL(F03442,'') AS VALUE_AS_CONCEPT_ID_1
           ,ISNULL(F03442,'') AS QUALIFIER_CONCEPT_ID_1
		   ,'' AS VALUE_AS_CONCEPT_ID_2
           ,'' AS QUALIFIER_CONCEPT_ID_2
           ,'' AS VALUE_AS_CONCEPT_ID_3
           ,'' AS QUALIFIER_CONCEPT_ID_3
	       ,NULL AS UNIT_CONCEPT_ID
		   ,(SELECT TOP 1 rsTarget.F05162 FROM UNM_CNExTCases.dbo.DxStg rsTarget WHERE rsSource.uk = rsTarget.fk2 Order By rsTarget.UK ASC) AS PROVIDER_ID     /*2460*/
		   ,rsSource.fk1 AS VISIT_OCCURRENCE_ID
           ,rsSource.uk AS VISIT_DETAIL_ID 
           ,STUFF(rsSource.F00152,4,0,'.') AS OBSERVATION_SOURCE_VALUE                                                                                                    /*400*/
		   ,0 AS OBSERVATION_SOURCE_CONCEPT_ID
           ,0 AS UNIT_SOURCE_VALUE	   
	       ,F03442 AS QUALIFIER_SOURCE_VALUE
	       ,rsSource.FK1 AS OBSERVATION_EVENT_ID
	       ,'UNM_CNExTCases.dbo.Tumor rsSource' AS OBS_EVENT_FIELD_CONCEPT_ID
	       ,NULL AS VALUE_AS_DATETIME
           ,HSP.F00006 AS MRN		   
FROM UNM_CNExTCases.dbo.Tumor rsSource
JOIN UNM_CNExTCases.dbo.Hospital HSP ON HSP.fk2 = rsSource.uk
 WHERE F03442 IS NOT NULL
    AND F03442 <> '00000'
    AND F03442 <> ''
UNION ALL
SELECT TOP 1000 'CNEXT TUMOR(OMOP_OBSERVATIONS)' AS IDENTITY_CONTEXT                                                                                         /*'TUMOR REGISTRY FOLLOWUP RECURRENCE'*/
        ,rsSource.uk AS SOURCE_PK
        ,rsSource.uk AS OBSERVATION_ID                                                                                                                                  /*1772*/
        ,(SELECT TOP 1 rsTarget.F00004 FROM UNM_CNExTCases.dbo.PatExtended rsTarget WHERE rsTarget.uk =  rsSource.fk1 Order By rsTarget.UK ASC) AS PERSON_ID  /*20*/ 
        ,F00070 AS OBSERVATION_CONCEPT_ID_1   
        ,ISNULL(F03443,'') AS OBSERVATION_CONCEPT_ID_2                                                                                                                                                                                             /*3120*/ 
        ,ISNULL(F03444,'') AS OBSERVATION_CONCEPT_ID_3                                                                                                                                                                                             /*3130*/ 
        ,ISNULL(F03445,'') AS OBSERVATION_CONCEPT_ID_4                                                                                                                                                                                             /*3140*/ 
        ,ISNULL(F03446,'') AS OBSERVATION_CONCEPT_ID_5                                                                                                                                                                                             /*3150*/ 
        ,ISNULL(F03447,'') AS OBSERVATION_CONCEPT_ID_6                                                                                                                                                                                             /*3160*/ 
        ,ISNULL(F04261,'') AS OBSERVATION_CONCEPT_ID_7                                                                                                                                                                                             /*3161*/ 
        ,ISNULL(F04262,'') AS OBSERVATION_CONCEPT_ID_8                                                                                                                                                                                             /*3162*/ 
        ,ISNULL(F04263,'') AS OBSERVATION_CONCEPT_ID_9                                                                                                                                                                                             /*3163*/ 
        ,ISNULL(F04264,'') AS OBSERVATION_CONCEPT_ID_10                     
        ,TRY_CAST(rsTarget.F00157 AS DATE) AS OBSERVATION_DATE                                                                                                                                                /*1772*/
        ,TRY_CAST(rsTarget.F00157 AS DATETIME) AS OBSERVATION_DATETIME
		,'1791@32' AS OBSERVATION_TYPE_CONCEPT_ID
        ,F05275 AS VALUE_AS_NUMBER
        ,F05275 AS VALUE_AS_STRING
        ,F05275 AS VALUE_AS_CONCEPT_ID_1
        ,ISNULL(F03442,'') AS QUALIFIER_CONCEPT_ID_1
		,'' AS VALUE_AS_CONCEPT_ID_2
        ,'' AS QUALIFIER_CONCEPT_ID_2
        ,'' AS VALUE_AS_CONCEPT_ID_3
        ,'' AS QUALIFIER_CONCEPT_ID_3
	    ,NULL AS UNIT_CONCEPT_ID                                                                                                                                                                                          /*2460*/
        ,F00075 AS PROVIDER_ID                                                                                                                                                                                                           /*2470*/ 
		,rsSource.fk1 AS VISIT_OCCURRENCE_ID
        ,rsSource.uk AS VISIT_DETAIL_ID 
        ,STUFF(rsSource.F00152,4,0,'.') AS OBSERVATION_SOURCE_VALUE                                                                                                                                             /*400*/
		,0 AS OBSERVATION_SOURCE_CONCEPT_ID
        ,0 AS UNIT_SOURCE_VALUE	   
	    ,F00072 AS QUALIFIER_SOURCE_VALUE                                                                                                                                                                                     /*1791*/
	    ,rsSource.FK1 AS OBSERVATION_EVENT_ID
	    ,'UNM_CNExTCases.dbo.Tumor rsSource' AS OBS_EVENT_FIELD_CONCEPT_ID
	    ,NULL AS VALUE_AS_DATETIME
		,HSP.F00006 AS MRN
  FROM UNM_CNExTCases.dbo.Tumor rsSource
  JOIN UNM_CNExTCases.dbo.FollowUp rsTarget ON rsTarget.uk = rsSource.uk
  JOIN UNM_CNExTCases.dbo.Hospital HSP ON HSP.fk2 = rsSource.uk
  WHERE F00070 IS NOT NULL
    AND F00070 != ''
UNION ALL
SELECT TOP 1000 'CNEXT TUMOR(OMOP_OBSERVATIONS)' AS IDENTITY_CONTEXT                                                                                         /*'TUMOR REGISTRY SECONDARY DX RECORD'*/
           ,rsSource.uk AS SOURCE_PK
           ,rsSource.uk AS OBSERVATION_ID
           ,(SELECT TOP 1 rsTarget.F00004 FROM UNM_CNExTCases.dbo.PatExtended rsTarget WHERE rsTarget.uk =  rsSource.fk1 Order By rsTarget.UK ASC) AS PERSON_ID  /*20*/ 
           ,ISNULL(F06117,'') AS OBSERVATION_CONCEPT_ID_1
           ,ISNULL(F06118,'') AS OBSERVATION_CONCEPT_ID_2                                                                                                                                                                                             /*3120*/ 
           ,ISNULL(F06119,'') AS OBSERVATION_CONCEPT_ID_3                                                                                                                                                                                             /*3130*/ 
           ,ISNULL(F06120,'') AS OBSERVATION_CONCEPT_ID_4                                                                                                                                                                                             /*3140*/ 
           ,ISNULL(F06121,'') AS OBSERVATION_CONCEPT_ID_5                                                                                                                                                                                             /*3150*/ 
           ,ISNULL(F06122,'') AS OBSERVATION_CONCEPT_ID_6                                                                                                                                                                                             /*3160*/ 
           ,ISNULL(F06123,'') AS OBSERVATION_CONCEPT_ID_7                                                                                                                                                                                             /*3161*/ 
           ,ISNULL(F06124,'') AS OBSERVATION_CONCEPT_ID_8                                                                                                                                                                                             /*3162*/ 
           ,ISNULL(F06125,'') AS OBSERVATION_CONCEPT_ID_9                                                                                                                                                                                             /*3163*/ 
           ,ISNULL(F06126,'') AS OBSERVATION_CONCEPT_ID_10                                                                                                                                                                                           /*3164*/ 
           ,TRY_CAST(rsSource.F00029 AS DATE) AS OBSERVATION_DATE
           ,TRY_CAST(rsSource.F00029 AS DATETIME) AS OBSERVATION_DATETIME
		   ,'1791@32' AS OBSERVATION_TYPE_CONCEPT_ID
	       ,F06117 AS VALUE_AS_NUMBER
	       ,F06117 AS VALUE_AS_STRING
           ,ISNULL(F06117,'') AS VALUE_AS_CONCEPT_ID_1
           ,ISNULL(F06117,'') AS QUALIFIER_CONCEPT_ID_1
		   ,'' AS VALUE_AS_CONCEPT_ID_2
           ,'' AS QUALIFIER_CONCEPT_ID_2
           ,'' AS VALUE_AS_CONCEPT_ID_3
           ,'' AS QUALIFIER_CONCEPT_ID_3
	       ,NULL AS UNIT_CONCEPT_ID
		   ,(SELECT TOP 1 rsTarget.F05162 FROM UNM_CNExTCases.dbo.DxStg rsTarget WHERE rsSource.uk = rsTarget.fk2 Order By rsTarget.UK ASC) AS PROVIDER_ID     /*2460*/
		   ,rsSource.fk1 AS VISIT_OCCURRENCE_ID
           ,rsSource.uk AS VISIT_DETAIL_ID 
           ,STUFF(rsSource.F00152,4,0,'.') AS OBSERVATION_SOURCE_VALUE                                                                                                    /*400*/
		   ,0 AS OBSERVATION_SOURCE_CONCEPT_ID
           ,0 AS UNIT_SOURCE_VALUE	   
	       ,F06117 AS QUALIFIER_SOURCE_VALUE
	       ,rsSource.FK1 AS OBSERVATION_EVENT_ID
	       ,'UNM_CNExTCases.dbo.Tumor rsSource' AS OBS_EVENT_FIELD_CONCEPT_ID
	       ,NULL AS VALUE_AS_DATETIME
		   ,HSP.F00006 AS MRN
 FROM UNM_CNExTCases.dbo.Tumor rsSource
  JOIN UNM_CNExTCases.dbo.FollowUp rsTarget ON rsTarget.uk = rsSource.uk
  JOIN UNM_CNExTCases.dbo.Hospital HSP ON HSP.fk2 = rsSource.uk
 WHERE F06117 IS NOT NULL
    AND F06117 <> '0000000'
    AND F06117 <> ''
ORDER BY OBSERVATION_TYPE_CONCEPT_ID
         ,rsSource.UK DESC