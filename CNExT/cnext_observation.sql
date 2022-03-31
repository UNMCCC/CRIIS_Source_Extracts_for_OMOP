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

LTV - 2/4/2022 - handled NULL values with the ISNULL function. Removed the ISNULL function from columns that would never be null per the predicate. Removed spaces between
                 tick marks and replaced the word NULL with tick marks.

*/
SET NOCOUNT ON;
SELECT 'IDENTITY_CONTEXT|SOURCE_PK|OBSERVATION_ID|PERSON_ID|OBSERVATION_CONCEPT_ID_1|OBSERVATION_CONCEPT_ID_2|OBSERVATION_CONCEPT_ID_3|OBSERVATION_CONCEPT_ID_4|OBSERVATION_CONCEPT_ID_5|OBSERVATION_CONCEPT_ID_6|OBSERVATION_CONCEPT_ID_7|OBSERVATION_CONCEPT_ID_8|OBSERVATION_CONCEPT_ID_9|OBSERVATION_CONCEPT_ID_10|OBSERVATION_DATE|OBSERVATION_DATETIME|OBSERVATION_TYPE_CONCEPT_ID|VALUE_AS_NUMBER|VALUE_AS_STRING|VALUE_AS_CONCEPT_ID_1|QUALIFIER_CONCEPT_ID_1|VALUE_AS_CONCEPT_ID_2|QUALIFIER_CONCEPT_ID_2|VALUE_AS_CONCEPT_ID_3|QUALIFIER_CONCEPT_ID_3|UNIT_CONCEPT_ID|PROVIDER_ID|VISIT_OCCURRENCE_ID|VISIT_DETAIL_ID|OBSERVATION_SOURCE_VALUE|OBSERVATION_SOURCE_CONCEPT_ID|UNIT_SOURCE_VALUE|QUALIFIER_SOURCE_VALUE|OBSERVATION_EVENT_ID|OBS_EVENT_FIELD_CONCEPT_ID|VALUE_AS_DATETIME|MRN|Modified_DtTm';
SELECT 'CNEXT TUMOR(OMOP_OBSERVATIONS)' AS IDENTITY_CONTEXT                                                                                         /*Family History*/
        ,rsSource.uk AS SOURCE_PK
        ,rsSource.uk AS OBSERVATION_ID
        ,PAT.UK AS PERSON_ID  /*20*/ 
        ,F06433 AS OBSERVATION_CONCEPT_ID_1    --this field is not null per WHERE condition -- tumor.F06433::Family History (nullable)
	    ,'' AS OBSERVATION_CONCEPT_ID_2
        ,'' AS OBSERVATION_CONCEPT_ID_3
        ,'' AS OBSERVATION_CONCEPT_ID_4
        ,'' AS OBSERVATION_CONCEPT_ID_5 
        ,'' AS OBSERVATION_CONCEPT_ID_6 
        ,'' AS OBSERVATION_CONCEPT_ID_7
        ,'' AS OBSERVATION_CONCEPT_ID_8
        ,'' AS OBSERVATION_CONCEPT_ID_9
        ,'' AS OBSERVATION_CONCEPT_ID_10
        ,case
		   when rsSource.F00029 = '00000000'
		   then ''
		   when right(rsSource.F00029, 4) = '9999'
		   then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		   when right(rsSource.F00029,2) = '99'
           then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		   else ISNULL(FORMAT(TRY_CAST(rsSource.F00029 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		 END AS OBSERVATION_DATE 
        ,case
		   when rsSource.F00029 = '00000000'
		   then ''
		   when right(rsSource.F00029, 4) = '9999'
		   then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,4) + '0101' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		   when right(rsSource.F00029,2) = '99'
           then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,6) + '01' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		   else ISNULL(FORMAT(TRY_CAST(rsSource.F00029 AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		 END AS OBSERVATION_DATETIME
		,'1791@32' AS OBSERVATION_TYPE_CONCEPT_ID
		,'' AS VALUE_AS_NUMBER
		,'' AS VALUE_AS_STRING
        ,ISNULL(F06434,'') AS VALUE_AS_CONCEPT_ID_1
        ,ISNULL(F06435,'') AS QUALIFIER_CONCEPT_ID_1
        ,ISNULL(F06436,'') AS VALUE_AS_CONCEPT_ID_2
        ,ISNULL(F06437,'') AS QUALIFIER_CONCEPT_ID_2
        ,ISNULL(F06438,'') AS VALUE_AS_CONCEPT_ID_3
        ,ISNULL(F06439,'') AS QUALIFIER_CONCEPT_ID_3
		,'' AS UNIT_CONCEPT_ID
		,(SELECT TOP 1 ISNULL(rsTarget.F05162,'') 
	        FROM UNM_CNExTCases.dbo.DxStg rsTarget 
		   WHERE rsSource.uk = rsTarget.fk2 Order By rsTarget.UK ASC) AS PROVIDER_ID     /*2460  DD: Dx/Stg Proc, Physician */
		,rsSource.uk AS VISIT_OCCURRENCE_ID  -- Really?  FK1 is the connecting ID for patient. tumor.UK connects to Hosp.FK2
        ,rsSource.uk AS VISIT_DETAIL_ID -- better, but still, that hust joins to the Hosp.PK.
        ,ISNULL(STUFF(rsSource.F00152,4,0,'.'),'') AS OBSERVATION_SOURCE_VALUE       --Site - Primary (ICD-O-3)                 /*400*/
	,'' AS OBSERVATION_SOURCE_CONCEPT_ID
        ,'' AS UNIT_SOURCE_VALUE	   
		,F06433 AS QUALIFIER_SOURCE_VALUE    --this field is not be null per WHERE condition  --Family History
	    ,ISNULL(rsSource.FK1,'') AS OBSERVATION_EVENT_ID                          
	    ,'UNM_CNExTCases.dbo.Tumor rsSource' AS OBS_EVENT_FIELD_CONCEPT_ID
	    ,'' AS VALUE_AS_DATETIME
	    ,ISNULL(HSP.F00006,'') AS MRN
	    ,'Family_History' AS OBSERVATION_TYPE
		,CASE WHEN format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') is NULL
           then FORmat(GETDATE(), 'yyyy-MM-dd HH:mm:ss')  
	       else format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') end
	     AS modified_dtTm
   FROM UNM_CNExTCases.dbo.Tumor rsSource
   JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.uk = rsSource.fk1
   JOIN UNM_CNExTCases.dbo.Hospital HSP ON HSP.fk2 = rsSource.uk
   JOIN UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK = HExt.UK
  WHERE F06433 IS NOT NULL
    and HSP.F00006 not in (999999998, 9999998, 999999, 9999)
    and HSP.F00006 >= 1000
UNION
SELECT 'CNEXT TUMOR(OMOP_OBSERVATIONS)' AS IDENTITY_CONTEXT                                                     /*'TUMOR REGISTRY COMORBIDITY RECORD'*/
            ,rsSource.uk AS SOURCE_PK
            ,rsSource.uk AS OBSERVATION_ID
            ,PAT.UK AS PERSON_ID  /*20*/ 
            ,F03442 AS OBSERVATION_CONCEPT_ID_1    --this field cannot be null per predicate
            ,ISNULL(F03443,'') AS OBSERVATION_CONCEPT_ID_2                                                       /*3120*/ 
            ,ISNULL(F03444,'') AS OBSERVATION_CONCEPT_ID_3                                                       /*3130*/ 
            ,ISNULL(F03445,'') AS OBSERVATION_CONCEPT_ID_4                                                       /*3140*/ 
            ,ISNULL(F03446,'') AS OBSERVATION_CONCEPT_ID_5                                                       /*3150*/ 
            ,ISNULL(F03447,'') AS OBSERVATION_CONCEPT_ID_6                                                       /*3160*/ 
            ,ISNULL(F04261,'') AS OBSERVATION_CONCEPT_ID_7                                                       /*3161*/ 
            ,ISNULL(F04262,'') AS OBSERVATION_CONCEPT_ID_8                                                       /*3162*/ 
            ,ISNULL(F04263,'') AS OBSERVATION_CONCEPT_ID_9                                                       /*3163*/ 
            ,ISNULL(F04264,'') AS OBSERVATION_CONCEPT_ID_10                                                      /*3164*/ 
            ,case
		      when rsSource.F00029 = '00000000'
		      then ''
		      when right(rsSource.F00029, 4) = '9999'
		      then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		      when right(rsSource.F00029,2) = '99'
              then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		      else ISNULL(FORMAT(TRY_CAST(rsSource.F00029 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		    END AS OBSERVATION_DATE 
			,case
		      when rsSource.F00029 = '00000000'
		      then ''
		      when right(rsSource.F00029, 4) = '9999'
		      then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,4) + '0101' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		      when right(rsSource.F00029,2) = '99'
              then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,6) + '01' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		      else ISNULL(FORMAT(TRY_CAST(rsSource.F00029 AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		    END AS OBSERVATION_DATETIME
		   ,'1791@32' AS OBSERVATION_TYPE_CONCEPT_ID
	       ,F03442 AS VALUE_AS_NUMBER          --this field cannot be null per predicate
	       ,F03442 AS VALUE_AS_STRING          --this field cannot be null per predicate
           ,F03442 AS VALUE_AS_CONCEPT_ID_1    --this field cannot be null per predicate
           ,F03442 AS QUALIFIER_CONCEPT_ID_1   --this field cannot be null per predicate
		   ,'' AS VALUE_AS_CONCEPT_ID_2
           ,'' AS QUALIFIER_CONCEPT_ID_2
           ,'' AS VALUE_AS_CONCEPT_ID_3
           ,'' AS QUALIFIER_CONCEPT_ID_3
	       ,'' AS UNIT_CONCEPT_ID
		   ,(SELECT TOP 1 ISNULL(rsTarget.F05162,'') FROM UNM_CNExTCases.dbo.DxStg rsTarget WHERE rsSource.uk = rsTarget.fk2 Order By rsTarget.UK ASC) AS PROVIDER_ID     /*2460*/
		   ,rsSource.uk AS VISIT_OCCURRENCE_ID
           ,rsSource.uk AS VISIT_DETAIL_ID 
           ,ISNULL(STUFF(rsSource.F00152,4,0,'.'),'') AS OBSERVATION_SOURCE_VALUE                               /*400*/
		   ,'' AS OBSERVATION_SOURCE_CONCEPT_ID
           ,'' AS UNIT_SOURCE_VALUE	   
	       ,F03442 AS QUALIFIER_SOURCE_VALUE    --this field cannot be null per predicate
	       ,ISNULL(rsSource.FK1,'') AS OBSERVATION_EVENT_ID
	       ,'UNM_CNExTCases.dbo.Tumor rsSource' AS OBS_EVENT_FIELD_CONCEPT_ID
	       ,'' AS VALUE_AS_DATETIME
           ,ISNULL(HSP.F00006,'') AS MRN
	       ,'Comorbidity' AS OBSERVATION_TYPE		   
		   ,isNULL(format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss'),'')  AS modified_dtTm
  FROM UNM_CNExTCases.dbo.Tumor rsSource
  JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.uk = rsSource.fk1
  JOIN UNM_CNExTCases.dbo.Hospital HSP ON HSP.fk2 = rsSource.uk
  JOIN UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK = HExt.UK
 WHERE F03442 IS NOT NULL
    AND F03442 <> '00000'
    AND F03442 <> ''
	and HSP.F00006 not in (999999998, 9999998, 999999, 9999)
    and HSP.F00006 >= 1000
UNION
SELECT 'CNEXT FOLLOWUP(OMOP_OBSERVATIONS)' AS IDENTITY_CONTEXT                                                                                         /*'TUMOR REGISTRY FOLLOWUP RECURRENCE'*/
        ,rsSource.uk AS SOURCE_PK
        ,rsSource.uk AS OBSERVATION_ID                                                                                                                                  /*1772*/
        ,PAT.UK AS PERSON_ID  /*20*/ 
        ,F00070 AS OBSERVATION_CONCEPT_ID_1   --this field cannot be null per predicate   
        ,ISNULL(F03443,'') AS OBSERVATION_CONCEPT_ID_2                                               /*3120*/ 
        ,ISNULL(F03444,'') AS OBSERVATION_CONCEPT_ID_3                                               /*3130*/ 
        ,ISNULL(F03445,'') AS OBSERVATION_CONCEPT_ID_4                                               /*3140*/ 
        ,ISNULL(F03446,'') AS OBSERVATION_CONCEPT_ID_5                                               /*3150*/ 
        ,ISNULL(F03447,'') AS OBSERVATION_CONCEPT_ID_6                                               /*3160*/ 
        ,ISNULL(F04261,'') AS OBSERVATION_CONCEPT_ID_7                                               /*3161*/ 
        ,ISNULL(F04262,'') AS OBSERVATION_CONCEPT_ID_8                                               /*3162*/ 
        ,ISNULL(F04263,'') AS OBSERVATION_CONCEPT_ID_9                                               /*3163*/ 
        ,ISNULL(F04264,'') AS OBSERVATION_CONCEPT_ID_10                     
        ,case
		   when rsSource.F00029 = '00000000'
		   then ''
		   when right(rsSource.F00029, 4) = '9999'
		   then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		   when right(rsSource.F00029,2) = '99'
           then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		   else ISNULL(FORMAT(TRY_CAST(rsSource.F00029 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		 END AS OBSERVATION_DATE 
		,case
		   when rsSource.F00029 = '00000000'
		   then ''
		   when right(rsSource.F00029, 4) = '9999'
		   then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,4) + '0101' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		   when right(rsSource.F00029,2) = '99'
           then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,6) + '01' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		   else ISNULL(FORMAT(TRY_CAST(rsSource.F00029 AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		 END AS OBSERVATION_DATETIME
		,'1791@32' AS OBSERVATION_TYPE_CONCEPT_ID
        ,ISNULL(F05275,'') AS VALUE_AS_NUMBER
        ,ISNULL(F05275,'') AS VALUE_AS_STRING
        ,ISNULL(F05275,'') AS VALUE_AS_CONCEPT_ID_1
        ,ISNULL(F03442,'') AS QUALIFIER_CONCEPT_ID_1
		,'' AS VALUE_AS_CONCEPT_ID_2
        ,'' AS QUALIFIER_CONCEPT_ID_2
        ,'' AS VALUE_AS_CONCEPT_ID_3
        ,'' AS QUALIFIER_CONCEPT_ID_3
	    ,'' AS UNIT_CONCEPT_ID                                /*2460*/
        ,ISNULL(F00075,'') AS PROVIDER_ID                     /*2470*/ 
		,rsSource.uk AS VISIT_OCCURRENCE_ID
        ,rsSource.uk AS VISIT_DETAIL_ID 
        ,ISNULL(STUFF(rsSource.F00152,4,0,'.'),'') AS OBSERVATION_SOURCE_VALUE                        /*400*/
		,'' AS OBSERVATION_SOURCE_CONCEPT_ID
        ,'' AS UNIT_SOURCE_VALUE	   
	    ,ISNULL(F00072,'') AS QUALIFIER_SOURCE_VALUE                                                  /*1791*/
	    ,ISNULL(rsSource.FK1,'') AS OBSERVATION_EVENT_ID
	    ,'UNM_CNExTCases.dbo.Tumor rsSource' AS OBS_EVENT_FIELD_CONCEPT_ID
	    ,'' AS VALUE_AS_DATETIME
		,ISNULL(HSP.F00006,'') AS MRN
	    ,'Follow_Up' AS OBSERVATION_TYPE
        ,isNULL(format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss'),'')  AS modified_dtTm
  FROM UNM_CNExTCases.dbo.Tumor rsSource
  JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.uk = rsSource.fk1
  JOIN UNM_CNExTCases.dbo.FollowUp rsTarget ON rsTarget.uk = rsSource.uk
  JOIN UNM_CNExTCases.dbo.Hospital HSP ON HSP.fk2 = rsSource.UK
  JOIN UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK = HExt.UK
 WHERE F00070 IS NOT NULL
   AND F00070 != ''
   and HSP.F00006 not in (999999998, 9999998, 999999, 9999)
   and HSP.F00006 >= 1000
UNION
SELECT 'CNEXT FOLLOWUP(OMOP_OBSERVATIONS)' AS IDENTITY_CONTEXT                                                                                         /*'TUMOR REGISTRY SECONDARY DX RECORD'*/
           ,rsSource.uk AS SOURCE_PK
           ,rsSource.uk AS OBSERVATION_ID
           ,PAT.UK AS PERSON_ID  /*20*/ 
           ,F06117 AS OBSERVATION_CONCEPT_ID_1   --this field cannot be null per predicate
           ,ISNULL(F06118,'') AS OBSERVATION_CONCEPT_ID_2                    /*3120*/ 
           ,ISNULL(F06119,'') AS OBSERVATION_CONCEPT_ID_3                    /*3130*/ 
           ,ISNULL(F06120,'') AS OBSERVATION_CONCEPT_ID_4                    /*3140*/ 
           ,ISNULL(F06121,'') AS OBSERVATION_CONCEPT_ID_5                    /*3150*/ 
           ,ISNULL(F06122,'') AS OBSERVATION_CONCEPT_ID_6                    /*3160*/ 
           ,ISNULL(F06123,'') AS OBSERVATION_CONCEPT_ID_7                    /*3161*/ 
           ,ISNULL(F06124,'') AS OBSERVATION_CONCEPT_ID_8                    /*3162*/ 
           ,ISNULL(F06125,'') AS OBSERVATION_CONCEPT_ID_9                    /*3163*/ 
           ,ISNULL(F06126,'') AS OBSERVATION_CONCEPT_ID_10                   /*3164*/ 
           ,case
		     when rsSource.F00029 = '00000000'
		     then ''
		     when right(rsSource.F00029, 4) = '9999'
		     then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		     when right(rsSource.F00029,2) = '99'
             then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		     else ISNULL(FORMAT(TRY_CAST(rsSource.F00029 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		    END AS OBSERVATION_DATE 
		   ,case
		     when rsSource.F00029 = '00000000'
		     then ''
		     when right(rsSource.F00029, 4) = '9999'
		     then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,4) + '0101' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		     when right(rsSource.F00029,2) = '99'
             then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,6) + '01' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		     else ISNULL(FORMAT(TRY_CAST(rsSource.F00029 AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		    END AS OBSERVATION_DATETIME
		   ,'1791@32' AS OBSERVATION_TYPE_CONCEPT_ID
	       ,F06117 AS VALUE_AS_NUMBER   --this field cannot be null per predicate
	       ,F06117 AS VALUE_AS_STRING   --this field cannot be null per predicate
           ,F06117 AS VALUE_AS_CONCEPT_ID_1   --this field cannot be null per predicate
           ,F06117 AS QUALIFIER_CONCEPT_ID_1  --this field cannot be null per predicate
		   ,'' AS VALUE_AS_CONCEPT_ID_2
           ,'' AS QUALIFIER_CONCEPT_ID_2
           ,'' AS VALUE_AS_CONCEPT_ID_3
           ,'' AS QUALIFIER_CONCEPT_ID_3
	       ,'' AS UNIT_CONCEPT_ID
		   ,(SELECT TOP 1 ISNULL(rsTarget.F05162,'') FROM UNM_CNExTCases.dbo.DxStg rsTarget WHERE rsSource.uk = rsTarget.fk2 Order By rsTarget.UK ASC) AS PROVIDER_ID     /*2460*/
		   ,rsSource.uk AS VISIT_OCCURRENCE_ID
           ,rsSource.uk AS VISIT_DETAIL_ID 
           ,ISNULL(STUFF(rsSource.F00152,4,0,'.'),'') AS OBSERVATION_SOURCE_VALUE    /*400*/
		   ,'' AS OBSERVATION_SOURCE_CONCEPT_ID
           ,'' AS UNIT_SOURCE_VALUE	   
	       ,F06117 AS QUALIFIER_SOURCE_VALUE   --this field cannot be null per predicate
	       ,ISNULL(rsSource.FK1,'') AS OBSERVATION_EVENT_ID
	       ,'UNM_CNExTCases.dbo.Tumor rsSource' AS OBS_EVENT_FIELD_CONCEPT_ID
	       ,'' AS VALUE_AS_DATETIME
		   ,ISNULL(HSP.F00006,'') AS MRN
	      ,'Secondary_Dx' AS OBSERVATION_TYPE
          ,isNULL(format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss'),'')  AS modified_dtTm
  FROM UNM_CNExTCases.dbo.Tumor rsSource
  JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.uk = rsSource.fk1
  JOIN UNM_CNExTCases.dbo.FollowUp rsTarget ON rsTarget.uk = rsSource.uk
  JOIN UNM_CNExTCases.dbo.Hospital HSP ON HSP.fk2 = rsSource.UK
  JOIN UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK = HExt.UK
 WHERE F06117 IS NOT NULL
   AND F06117 <> '0000000'
   AND F06117 <> ''
   and HSP.F00006 not in (999999998, 9999998, 999999, 9999)
   and HSP.F00006 >= 1000
ORDER BY OBSERVATION_TYPE_CONCEPT_ID
         ,rsSource.UK DESC