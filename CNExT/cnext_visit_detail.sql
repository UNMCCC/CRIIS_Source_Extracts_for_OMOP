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

*/

SELECT DISTINCT  'CNEXT PATIENT(OMOP_VISIT_DETAIL)' AS IDENTITY_CONTEXT
        ,rsSource.uk AS SOURCE_PK
        ,rsSource.uk AS VISIT_DETAIL_ID
    	,HSP.F00016 AS PERSON_ID              /*545*/                                                                                                                                         /*190*/
    	,HSG.F05522 AS VISIT_DETAIL_CONCEPT_ID                                                                                                                                  /*605*/
        ,TRY_CAST(SRG.F00434 AS DATE)  AS VISIT_DETAIL_START_DATE             /*1200*/ 
    	,TRY_CAST(SRG.F00434 AS DATETIME) AS VISIT_DETAIL_START_DATETIME     /*1200*/
        ,TRY_CAST(SRG.F05169 AS DATE) AS VISIT_DETAIL_END_DATE               /*3180*/
        ,TRY_CAST(SRG.F05169 AS DATETIME) AS VISIT_DETAIL_END_DATETIME       /*3180*/
    	,'1791@32' AS VISIT_DETAIL_TYPE_CONCEPT_ID
    	,SRG.F05161 AS PROVIDER_ID                          /*2480*/
        ,SRG.F01689 AS CARE_SITE_ID                           /*540*/
    	,SRG.F03488 AS VISIT_DETAIL_SOURCE_VALUE                          /*670*/
	    ,'605@' + SRG.F03488 AS VISIT_DETAIL_SOURCE_CONCEPT_ID
	    ,(SELECT TOP 1 rsTarget.F01684  FROM dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS ADMITTING_SOURCE_VALUE              /*2410*/
	    ,(SELECT TOP 1 rsTarget.F03715  FROM dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS ADMITTING_SOURCE_CONCEPT_ID            /*2415*/
	    ,(SELECT TOP 1 rsTarget.F01685  FROM dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS DISCHARGE_TO_SOURCE_VALUE                /*2420*/
	    ,(SELECT TOP 1 rsTarget.F03716  FROM dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS DISCHARGE_TO_CONCEPT_ID             /*2425*/
	    ,HSP.fk2 AS PRECEDING_VISIT_DETAIL_ID
	    ,NULL AS VISIT_DETAIL_PARENT_ID
	    ,HSP.UK AS VISIT_OCCURRENCE_ID                                                                      /*10*/
   FROM UNM_CNExTCases.dbo.Tumor rsSource
   JOIN UNM_CNExTCases.dbo.Hospital HSP ON rsSource.uk = HSP.fk2
   JOIN UNM_CNExTCases.dbo.HospSupp HSG ON HSG.UK = HSP.UK
   JOIN UNM_CNExTCases.dbo.Surg SRG ON SRG.fk2 = rsSource.uk
WHERE SRG.F03488 != '00' 
    AND SRG.F03488 < '98'
UNION ALL
SELECT DISTINCT TOP 1000 'CNEXT PATIENT(OMOP_VISIT_DETAIL)' AS IDENTITY_CONTEXT
        ,rsSource.uk AS SOURCE_PK
        ,rsSource.uk AS VISIT_DETAIL_ID
    	,HSP.F00016 AS PERSON_ID                                                                                                                                         /*190*/
    	,HSG.F05522 AS VISIT_DETAIL_CONCEPT_ID                                                                                                                                  /*605*/
	    ,TRY_CAST(RAD.F05187 AS DATE)  AS VISIT_DETAIL_START_DATE             /*1210*/ 
    	,TRY_CAST(RAD.F05187 AS DATETIME) AS VISIT_DETAIL_START_DATETIME     /*1210*/
        ,TRY_CAST(RAD.F05212 AS DATE) AS VISIT_DETAIL_END_DATE               /*3220*/
        ,TRY_CAST(RAD.F05212 AS DATETIME) AS VISIT_DETAIL_END_DATETIME       /*3220*/
    	,'1791@32' AS VISIT_DETAIL_TYPE_CONCEPT_ID
    	,RAD.F05156 AS PROVIDER_ID                          /*2480*/
        ,RAD.F03478 AS CARE_SITE_ID                           /*1550*/
    	,RAD.F05257 AS VISIT_DETAIL_SOURCE_VALUE                                                                                                                                /*1360*/
	    ,'1360@' + RAD.F05257 AS VISIT_DETAIL_SOURCE_CONCEPT_ID
	    ,(SELECT TOP 1 rsTarget.F01684  FROM dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS ADMITTING_SOURCE_VALUE              /*2410*/
	    ,(SELECT TOP 1 rsTarget.F03715  FROM dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS ADMITTING_SOURCE_CONCEPT_ID            /*2415*/
	    ,(SELECT TOP 1 rsTarget.F01685  FROM dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS DISCHARGE_TO_SOURCE_VALUE                /*2420*/
	    ,(SELECT TOP 1 rsTarget.F03716  FROM dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS DISCHARGE_TO_CONCEPT_ID             /*2425*/
	    ,HSP.fk2 AS PRECEDING_VISIT_DETAIL_ID
	    ,NULL AS VISIT_DETAIL_PARENT_ID
	    ,HSP.UK AS VISIT_OCCURRENCE_ID                                                                      /*10*/
   FROM UNM_CNExTCases.dbo.Tumor rsSource
  INNER JOIN UNM_CNExTCases.dbo.Hospital HSP ON rsSource.uk = HSP.fk2
  INNER JOIN UNM_CNExTCases.dbo.HospSupp HSG ON HSG.UK = HSP.UK
  INNER JOIN UNM_CNExTCases.dbo.Radiation RAD ON RAD.fk2 = rsSource.uk
  WHERE F05257 > '000'
UNION ALL
SELECT DISTINCT TOP 1000 'CNEXT PATIENT(OMOP_VISIT_DETAIL)' AS IDENTITY_CONTEXT
        ,rsSource.uk AS SOURCE_PK
        ,rsSource.uk AS VISIT_DETAIL_ID
    	,HSP.F00016 AS PERSON_ID                                                                                                                                         /*190*/
    	,HSG.F05522 AS VISIT_DETAIL_CONCEPT_ID                                                                                                                                  /*605*/
	    ,TRY_CAST(CHM.F05189 AS DATE)  AS VISIT_DETAIL_START_DATE             /*1220*/ 
    	,TRY_CAST(CHM.F05189 AS DATETIME) AS VISIT_DETAIL_START_DATETIME     /*1220*/
        ,TRY_CAST(CHM.F05214 AS DATE) AS VISIT_DETAIL_END_DATE               /*3180*/
        ,TRY_CAST(CHM.F05214 AS DATETIME) AS VISIT_DETAIL_END_DATETIME       /*3180*/
    	,'1791@32' AS VISIT_DETAIL_TYPE_CONCEPT_ID
    	,CHM.F05157 AS PROVIDER_ID                          /*2460*/
        ,CHM.F03479 AS CARE_SITE_ID                           /*540*/
    	,CHM.F05037 AS VISIT_DETAIL_SOURCE_VALUE                                                                                                                                /*700*/
	    ,'700@' + CHM.F05037 AS VISIT_DETAIL_SOURCE_CONCEPT_ID
	    ,(SELECT TOP 1 rsTarget.F01684  FROM dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS ADMITTING_SOURCE_VALUE              /*2410*/
	    ,(SELECT TOP 1 rsTarget.F03715  FROM dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS ADMITTING_SOURCE_CONCEPT_ID            /*2415*/
	    ,(SELECT TOP 1 rsTarget.F01685  FROM dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS DISCHARGE_TO_SOURCE_VALUE                /*2420*/
	    ,(SELECT TOP 1 rsTarget.F03716  FROM dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS DISCHARGE_TO_CONCEPT_ID             /*2425*/
	    ,HSP.fk2 AS PRECEDING_VISIT_DETAIL_ID
	    ,NULL AS VISIT_DETAIL_PARENT_ID
	    ,HSP.UK AS VISIT_OCCURRENCE_ID                                                                      /*10*/
   FROM UNM_CNExTCases.dbo.Tumor rsSource
   JOIN UNM_CNExTCases.dbo.Hospital HSP ON rsSource.uk = HSP.fk2
   JOIN UNM_CNExTCases.dbo.HospSupp HSG ON HSG.UK = HSP.UK
   JOIN UNM_CNExTCases.dbo.Chemo CHM ON CHM.fk2 = rsSource.uk
WHERE CHM.F05037 IN ('01', '02', '03')
    AND CHM.F05669 > '00'
UNION ALL
SELECT DISTINCT TOP 1000 'CNEXT PATIENT(OMOP_VISIT_DETAIL)' AS IDENTITY_CONTEXT
        ,rsSource.uk AS SOURCE_PK
        ,rsSource.uk AS VISIT_DETAIL_ID
    	,HSP.F00016 AS PERSON_ID                                                                                                                                         /*190*/
    	,HSG.F05522 AS VISIT_DETAIL_CONCEPT_ID                                                                                                                                  /*605*/
	    ,TRY_CAST(HOR.F05191 AS DATE)  AS VISIT_DETAIL_START_DATE             /*1230*/ 
    	,TRY_CAST(HOR.F05191 AS DATETIME) AS VISIT_DETAIL_START_DATETIME     /*1230*/
        ,TRY_CAST(HOR.F05216 AS DATE) AS VISIT_DETAIL_END_DATE               /*3180*/
        ,TRY_CAST(HOR.F05216 AS DATETIME) AS VISIT_DETAIL_END_DATETIME       /*3180*/
    	,'1791@32' AS VISIT_DETAIL_TYPE_CONCEPT_ID
    	,HOR.F05158 AS PROVIDER_ID                          /*2460*/
        ,HOR.F03480 AS CARE_SITE_ID                           /*540*/
    	,HOR.F05063 AS VISIT_DETAIL_SOURCE_VALUE                                                                                                                                /*710*/
	    ,'710@' + HOR.F05063 AS VISIT_DETAIL_SOURCE_CONCEPT_ID
	    ,(SELECT TOP 1 rsTarget.F01684  FROM dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS ADMITTING_SOURCE_VALUE              /*2410*/
	    ,(SELECT TOP 1 rsTarget.F03715  FROM dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS ADMITTING_SOURCE_CONCEPT_ID            /*2415*/
	    ,(SELECT TOP 1 rsTarget.F01685  FROM dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS DISCHARGE_TO_SOURCE_VALUE                /*2420*/
	    ,(SELECT TOP 1 rsTarget.F03716  FROM dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS DISCHARGE_TO_CONCEPT_ID             /*2425*/
	    ,HSP.fk2 AS PRECEDING_VISIT_DETAIL_ID
	    ,NULL AS VISIT_DETAIL_PARENT_ID
	    ,HSP.UK AS VISIT_OCCURRENCE_ID                                                                      /*10*/
   FROM UNM_CNExTCases.dbo.Tumor rsSource
   JOIN UNM_CNExTCases.dbo.Hospital HSP ON rsSource.uk = HSP.fk2
   JOIN UNM_CNExTCases.dbo.HospSupp HSG ON HSG.UK = HSP.UK
   JOIN UNM_CNExTCases.dbo.Hormone HOR ON HOR.fk2 = rsSource.uk
WHERE HOR.F05063 IN ('01')
UNION ALL
SELECT DISTINCT TOP 1000 'CNEXT PATIENT(OMOP_VISIT_DETAIL)' AS IDENTITY_CONTEXT
        ,rsSource.uk AS SOURCE_PK
        ,rsSource.uk AS VISIT_DETAIL_ID
    	,HSP.F00016 AS PERSON_ID                                                                                                                                         /*190*/
    	,HSG.F05522 AS VISIT_DETAIL_CONCEPT_ID                                                                                                                                  /*605*/
	    ,TRY_CAST(BRM.F05193 AS DATE)  AS VISIT_DETAIL_START_DATE             /*1240*/ 
    	,TRY_CAST(BRM.F05193 AS DATETIME) AS VISIT_DETAIL_START_DATETIME     /*1240*/
        ,TRY_CAST(BRM.F05218 AS DATE) AS VISIT_DETAIL_END_DATE               /*3180*/
        ,TRY_CAST(BRM.F05218 AS DATETIME) AS VISIT_DETAIL_END_DATETIME       /*3180*/
    	,'1791@32' AS VISIT_DETAIL_TYPE_CONCEPT_ID
    	,BRM.F05159 AS PROVIDER_ID                          /*2460*/
        ,BRM.F03481 AS CARE_SITE_ID                           /*540*/
    	,BRM.F05066 AS VISIT_DETAIL_SOURCE_VALUE                                                                                                                                /*720*/
	    ,'720@' + BRM.F05066 AS VISIT_DETAIL_SOURCE_CONCEPT_ID
	    ,(SELECT TOP 1 rsTarget.F01684  FROM dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS ADMITTING_SOURCE_VALUE              /*2410*/
	    ,(SELECT TOP 1 rsTarget.F03715  FROM dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS ADMITTING_SOURCE_CONCEPT_ID            /*2415*/
	    ,(SELECT TOP 1 rsTarget.F01685  FROM dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS DISCHARGE_TO_SOURCE_VALUE                /*2420*/
	    ,(SELECT TOP 1 rsTarget.F03716  FROM dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS DISCHARGE_TO_CONCEPT_ID             /*2425*/
	    ,HSP.fk2 AS PRECEDING_VISIT_DETAIL_ID
	    ,NULL AS VISIT_DETAIL_PARENT_ID
	    ,HSP.UK AS VISIT_OCCURRENCE_ID                                                                      /*10*/
   FROM UNM_CNExTCases.dbo.Tumor rsSource
   JOIN UNM_CNExTCases.dbo.Hospital HSP ON rsSource.uk = HSP.fk2
   JOIN UNM_CNExTCases.dbo.HospSupp HSG ON HSG.UK = HSP.UK
   JOIN UNM_CNExTCases.dbo.Immuno BRM ON BRM.fk2 = rsSource.uk
WHERE BRM.F05066 IN ('01')
UNION ALL
SELECT DISTINCT TOP 1000 'CNEXT PATIENT(OMOP_VISIT_DETAIL)' AS IDENTITY_CONTEXT
        ,rsSource.uk AS SOURCE_PK
        ,rsSource.uk AS VISIT_DETAIL_ID
    	,HSP.F00016 AS PERSON_ID                                                                                                                                         /*190*/
    	,HSG.F05522 AS VISIT_DETAIL_CONCEPT_ID                                                                                                                                  /*605*/
	    ,TRY_CAST(OTH.F05195 AS DATE)  AS VISIT_DETAIL_START_DATE             /*1250*/ 
    	,TRY_CAST(OTH.F05195 AS DATETIME) AS VISIT_DETAIL_START_DATETIME     /*1250*/
        ,NULL AS VISIT_DETAIL_END_DATE
        ,NULL AS VISIT_DETAIL_END_DATETIME 
    	,'1791@32' AS VISIT_DETAIL_TYPE_CONCEPT_ID
    	,OTH.F05160 AS PROVIDER_ID                          /*2460*/
        ,OTH.F05067 AS CARE_SITE_ID                           /*540*/
    	,OTH.F05069 AS VISIT_DETAIL_SOURCE_VALUE                                                                                                                                /*730*/
	    ,'730@' + OTH.F05069 AS VISIT_DETAIL_SOURCE_CONCEPT_ID
	    ,(SELECT TOP 1 rsTarget.F01684  FROM dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS ADMITTING_SOURCE_VALUE              /*2410*/
	    ,(SELECT TOP 1 rsTarget.F03715  FROM dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS ADMITTING_SOURCE_CONCEPT_ID            /*2415*/
	    ,(SELECT TOP 1 rsTarget.F01685  FROM dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS DISCHARGE_TO_SOURCE_VALUE                /*2420*/
	    ,(SELECT TOP 1 rsTarget.F03716  FROM dbo.HospExtended rsTarget WHERE rsTarget.UK = rsSource.UK Order By rsTarget.UK ASC) AS DISCHARGE_TO_CONCEPT_ID             /*2425*/
	    ,HSP.fk2 AS PRECEDING_VISIT_DETAIL_ID
	    ,NULL AS VISIT_DETAIL_PARENT_ID
	    ,HSP.UK AS VISIT_OCCURRENCE_ID                                                                      /*10*/
   FROM UNM_CNExTCases.dbo.Tumor rsSource
   JOIN UNM_CNExTCases.dbo.Hospital HSP ON rsSource.uk = HSP.fk2
   JOIN UNM_CNExTCases.dbo.HospSupp HSG ON HSG.UK = HSP.UK
   JOIN UNM_CNExTCases.dbo.Other OTH ON OTH.fk2 = rsSource.uk
WHERE OTH.F05069 in ( '1','2','3','6')