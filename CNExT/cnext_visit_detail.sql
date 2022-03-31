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
LTV - 2/22/2022 - Changed condition on the SURG table from SRG.F03488 != '00' to SRG.F03488 > '00' per Mark to avoid NULL and empty space values.
LTV - 2/22/2022 - Changed condition on the Radiation table. Added AND F07799 > '00' AND F07799 < '99' to the predicate per Mark.

*/
SET NOCOUNT ON;
SELECT 'IDENTITY_CONTEXT|SOURCE_PK|VISIT_DETAIL_ID|PERSON_ID|VISIT_DETAIL_CONCEPT_ID|VISIT_DETAIL_START_DATE|VISIT_DETAIL_START_DATETIME|VISIT_DETAIL_END_DATE|VISIT_DETAIL_END_DATETIME|VISIT_DETAIL_TYPE_CONCEPT_ID|PROVIDER_ID|CARE_SITE_ID|VISIT_DETAIL_SOURCE_VALUE|VISIT_DETAIL_SOURCE_CONCEPT_ID|ADMITTING_SOURCE_VALUE|ADMITTING_SOURCE_CONCEPT_ID|DISCHARGE_TO_SOURCE_VALUE|DISCHARGE_TO_CONCEPT_ID|PRECEDING_VISIT_DETAIL_ID|VISIT_DETAIL_PARENT_ID|VISIT_OCCURRENCE_ID|MRN|CONDITION_CONCEPT_ID_SITE|Modified_DtTm';
SELECT DISTINCT  'CNEXT SURG (OMOP_VISIT_DETAIL)' AS IDENTITY_CONTEXT
        ,rsSource.uk AS SOURCE_PK
        ,SRG.uk AS VISIT_DETAIL_ID
    	,PAT.UK AS PERSON_ID 
    	,ISNULL(HSG.F05522, '') AS VISIT_DETAIL_CONCEPT_ID                                                                                                                                  /*605*/
        ,case
		     when SRG.F00434 = '99999999'
			 then ''
			 when right(SRG.F00434,4) = '9999'
			 then ISNULL(FORMAT(TRY_CAST(left(SRG.F00434,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '') 
			 when right(SRG.F00434,2) = '99'
			 then ISNULL(FORMAT(TRY_CAST(left(SRG.F00434,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
			 else ISNULL(FORMAT(TRY_CAST(SRG.F00434 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		 end AS VISIT_DETAIL_START_DATE
		 ,case
		     when SRG.F00434 = '99999999'
			 then ''
			 when right(SRG.F00434,4) = '9999'
			 then ISNULL(FORMAT(TRY_CAST(left(SRG.F00434,4) + '0101' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '') 
			 when right(SRG.F00434,2) = '99'
			 then ISNULL(FORMAT(TRY_CAST(left(SRG.F00434,6) + '01' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
			 else ISNULL(FORMAT(TRY_CAST(SRG.F00434 AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		 end AS VISIT_DETAIL_START_DATETIME
		,case 
		      when SRG.F05169 = '99999999'
			  then ''
		      when right(SRG.F05169, 4) = '9999'
		      then ISNULL(FORMAT(TRY_CAST(left(SRG.F05169,4) + '0101' AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
			  when right(SRG.F05169, 2) = '99'
			  then ISNULL(FORMAT(TRY_CAST(left(SRG.F05169,6) + '01' AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
			  else ISNULL(FORMAT(TRY_CAST(SRG.F05169 AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
		 end AS VISIT_DETAIL_END_DATE
		,case 
		      when SRG.F05169 = '99999999'
			  then ''
		      when right(SRG.F05169, 4) = '9999'
		      then ISNULL(FORMAT(TRY_CAST(left(SRG.F05169,4) + '0101' AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
			  when right(SRG.F05169, 2) = '99'
			  then ISNULL(FORMAT(TRY_CAST(left(SRG.F05169,6) + '01' AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
			  else ISNULL(FORMAT(TRY_CAST(SRG.F05169 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
		 end AS VISIT_DETAIL_END_DATETIME
    	,'1791@32' AS VISIT_DETAIL_TYPE_CONCEPT_ID
    	,ISNULL(SRG.F05161, '') AS PROVIDER_ID                            /*2480*/
        ,ISNULL(SRG.F01689, '') AS CARE_SITE_ID                           /*540*/
    	,SRG.F03488 AS VISIT_DETAIL_SOURCE_VALUE
	    ,CASE
			WHEN SRG.F03488 <> ''
			THEN '1290@'  + SRG.F03488
			ELSE ''
		 END AS VISIT_DETAIL_SOURCE_CONCEPT_ID         
	    ,ISNULL(HExt.F01684, '') AS ADMITTING_SOURCE_VALUE
	    ,ISNULL(HExt.F03715, '') AS ADMITTING_SOURCE_CONCEPT_ID
	    ,ISNULL(HExt.F01685, '') AS DISCHARGE_TO_SOURCE_VALUE
	    ,ISNULL(HExt.F03716, '') AS DISCHARGE_TO_CONCEPT_ID
	    ,ISNULL(HSP.fk2, '') AS PRECEDING_VISIT_DETAIL_ID
	    ,'' AS VISIT_DETAIL_PARENT_ID
	    ,rsSource.UK AS VISIT_OCCURRENCE_ID                                   /*10*/
		,ISNULL(HSP.F00006, '') AS MRN
		,ISNULL(STUFF(rsSource.F00152,4,0,'.'), '') AS CONDITION_CONCEPT_ID_SITE
        ,CASE 
		    WHEN format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') is NULL
            then format(GETDATE(), 'yyyy-MM-dd HH:mm:ss')  
	        else format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss')
         end AS modified_dtTm
   FROM UNM_CNExTCases.dbo.Tumor rsSource
   JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.uk = rsSource.fk1
   JOIN UNM_CNExTCases.dbo.Hospital HSP ON rsSource.uk = HSP.fk2
   JOIN UNM_CNExTCases.dbo.HospSupp HSG ON HSG.UK = HSP.UK
   JOIN UNM_CNExTCases.dbo.Surg SRG ON SRG.fk2 = rsSource.uk
   JOIN  UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK = HExt.UK
  WHERE SRG.F03488 > '00' 
    AND SRG.F03488 < '98'
	and ((SRG.F00434 is not null and SRG.F00434 != '' and SRG.F00434 not in ('00000000','88888888'))
     or (SRG.F05169 is not null and SRG.F05169 != '' and SRG.F05169 not in ('00000000','88888888')))
	and HSP.F00006 not in (999999998, 9999998, 999999, 9999)
    and HSP.F00006 >= 1000 
UNION
SELECT DISTINCT 'CNEXT RADIATION(OMOP_VISIT_DETAIL)' AS IDENTITY_CONTEXT
        ,rsSource.uk AS SOURCE_PK
        ,RAD.uk AS VISIT_DETAIL_ID
    	,PAT.UK AS PERSON_ID
    	,ISNULL(HSG.F05522, '') AS VISIT_DETAIL_CONCEPT_ID
		,case
		     when RAD.F05187 = '99999999'
			 then ''
			 when right(RAD.F05187,4) = '9999'
			 then ISNULL(FORMAT(TRY_CAST(left(RAD.F05187,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '') 
			 when right(RAD.F05187,2) = '99'
			 then ISNULL(FORMAT(TRY_CAST(left(RAD.F05187,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
			 else ISNULL(FORMAT(TRY_CAST(RAD.F05187 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		 end AS VISIT_DETAIL_START_DATE
		,case
		     when RAD.F05187 = '99999999'
			 then ''
			 when right(RAD.F05187,4) = '9999'
			 then ISNULL(FORMAT(TRY_CAST(left(RAD.F05187,4) + '0101' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '') 
			 when right(RAD.F05187,2) = '99'
			 then ISNULL(FORMAT(TRY_CAST(left(RAD.F05187,6) + '01' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
			 else ISNULL(FORMAT(TRY_CAST(RAD.F05187 AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		 end AS VISIT_DETAIL_START_DATETIME
		,case 
		      when RAD.F05212 = '99999999'
			  then ''
		      when right(RAD.F05212, 4) = '9999'
		      then ISNULL(FORMAT(TRY_CAST(left(RAD.F05212,4) + '0101' AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
			  when right(RAD.F05212, 2) = '99'
			  then ISNULL(FORMAT(TRY_CAST(left(RAD.F05212,6) + '01' AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
			  else ISNULL(FORMAT(TRY_CAST(RAD.F05212 AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
		 end AS VISIT_DETAIL_END_DATE
		,case 
		      when RAD.F05212 = '99999999'
			  then ''
		      when right(RAD.F05212, 4) = '9999'
		      then ISNULL(FORMAT(TRY_CAST(left(RAD.F05212,4) + '0101' AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
			  when right(RAD.F05212, 2) = '99'
			  then ISNULL(FORMAT(TRY_CAST(left(RAD.F05212,6) + '01' AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
			  else ISNULL(FORMAT(TRY_CAST(RAD.F05212 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
		 end AS VISIT_DETAIL_END_DATETIME
    	,'1791@32' AS VISIT_DETAIL_TYPE_CONCEPT_ID
    	,ISNULL(RAD.F05156, '') AS PROVIDER_ID                            /*2480*/
        ,ISNULL(RAD.F03478, '') AS CARE_SITE_ID                           /*1550*/
    	,RAD.F07799 AS VISIT_DETAIL_SOURCE_VALUE
	    ,'1506@' + RAD.F07799 AS VISIT_DETAIL_SOURCE_CONCEPT_ID
	    ,ISNULL(HExt.F01684, '') AS ADMITTING_SOURCE_VALUE
	    ,ISNULL(HExt.F03715, '') AS ADMITTING_SOURCE_CONCEPT_ID
	    ,ISNULL(HExt.F01685, '') AS DISCHARGE_TO_SOURCE_VALUE
	    ,ISNULL(HExt.F03716, '') AS DISCHARGE_TO_CONCEPT_ID
	    ,ISNULL(HSP.fk2, '') AS PRECEDING_VISIT_DETAIL_ID
	    ,'' AS VISIT_DETAIL_PARENT_ID
	    ,rsSource.UK AS VISIT_OCCURRENCE_ID                                 /*10*/
		,ISNULL(HSP.F00006, '') AS MRN
		,ISNULL(STUFF(rsSource.F00152,4,0,'.'), '') AS CONDITION_CONCEPT_ID_SITE
		,CASE 
		    WHEN format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') is NULL
            then format(GETDATE(), 'yyyy-MM-dd HH:mm:ss')  
	        else format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss')
         end AS modified_dtTm
   FROM UNM_CNExTCases.dbo.Tumor rsSource
   JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.uk = rsSource.fk1
  INNER JOIN UNM_CNExTCases.dbo.Hospital HSP ON rsSource.uk = HSP.fk2
  INNER JOIN UNM_CNExTCases.dbo.HospSupp HSG ON HSG.UK = HSP.UK
  INNER JOIN UNM_CNExTCases.dbo.Radiation RAD ON RAD.fk2 = rsSource.uk
  INNER JOIN UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK = HExt.UK
  WHERE F05257 > '000'
    AND F07799 > '00'
    AND F07799 < '99'
	and ((RAD.F05187 is not NULL and RAD.F05187 != '' and RAD.F05187 not in ('00000000','88888888'))
	 or (RAD.F05212 is not NULL and RAD.F05212 != '' and RAD.F05212 not in ('00000000','88888888')))
	and HSP.F00006 not in (999999998, 9999998, 999999, 9999)
    and HSP.F00006 >= 1000 
UNION
SELECT DISTINCT 'CNEXT CHEMO(OMOP_VISIT_DETAIL)' AS IDENTITY_CONTEXT
        ,rsSource.uk AS SOURCE_PK
        ,CHM.uk AS VISIT_DETAIL_ID
    	,PAT.UK AS PERSON_ID
    	,ISNULL(HSG.F05522, '') AS VISIT_DETAIL_CONCEPT_ID
		,case
		     when CHM.F05189 = '99999999'
			 then ''
			 when right(CHM.F05189,4) = '9999'
			 then ISNULL(FORMAT(TRY_CAST(left(CHM.F05189,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '') 
			 when right(CHM.F05189,2) = '99'
			 then ISNULL(FORMAT(TRY_CAST(left(CHM.F05189,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
			 else ISNULL(FORMAT(TRY_CAST(CHM.F05189 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		 end AS VISIT_DETAIL_START_DATE
		,case
		     when CHM.F05189 = '99999999'
			 then ''
			 when right(CHM.F05189,4) = '9999'
			 then ISNULL(FORMAT(TRY_CAST(left(CHM.F05189,4) + '0101' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '') 
			 when right(CHM.F05189,2) = '99'
			 then ISNULL(FORMAT(TRY_CAST(left(CHM.F05189,6) + '01' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
			 else ISNULL(FORMAT(TRY_CAST(CHM.F05189 AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		 end AS VISIT_DETAIL_START_DATETIME
		,case 
		      when CHM.F05214 = '99999999'
			  then ''
		      when right(CHM.F05214, 4) = '9999'
		      then ISNULL(FORMAT(TRY_CAST(left(CHM.F05214,4) + '0101' AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
			  when right(CHM.F05214, 2) = '99'
			  then ISNULL(FORMAT(TRY_CAST(left(CHM.F05214,6) + '01' AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
			  else ISNULL(FORMAT(TRY_CAST(CHM.F05214 AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
		 end AS VISIT_DETAIL_END_DATE
		,case 
		      when CHM.F05214 = '99999999'
			  then ''
		      when right(CHM.F05214, 4) = '9999'
		      then ISNULL(FORMAT(TRY_CAST(left(CHM.F05214,4) + '0101' AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
			  when right(CHM.F05214, 2) = '99'
			  then ISNULL(FORMAT(TRY_CAST(left(CHM.F05214,6) + '01' AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
			  else ISNULL(FORMAT(TRY_CAST(CHM.F05214 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
		 end AS VISIT_DETAIL_END_DATETIME
    	,'1791@32' AS VISIT_DETAIL_TYPE_CONCEPT_ID
    	,ISNULL(CHM.F05157, '') AS PROVIDER_ID                            /*2460*/
        ,ISNULL(CHM.F03479, '') AS CARE_SITE_ID                           /*540*/
    	,CHM.F05037 AS VISIT_DETAIL_SOURCE_VALUE
	    ,'700@' + CHM.F05037 AS VISIT_DETAIL_SOURCE_CONCEPT_ID
		,ISNULL(HExt.F01684, '') AS ADMITTING_SOURCE_VALUE
	    ,ISNULL(HExt.F03715, '') AS ADMITTING_SOURCE_CONCEPT_ID
	    ,ISNULL(HExt.F01685, '') AS DISCHARGE_TO_SOURCE_VALUE
	    ,ISNULL(HExt.F03716, '') AS DISCHARGE_TO_CONCEPT_ID
	    ,ISNULL(HSP.fk2, '') AS PRECEDING_VISIT_DETAIL_ID
	    ,'' AS VISIT_DETAIL_PARENT_ID
	    ,rsSource.UK AS VISIT_OCCURRENCE_ID                                /*10*/
		,ISNULL(HSP.F00006, '') AS MRN
		,ISNULL(STUFF(rsSource.F00152,4,0,'.'), '') AS CONDITION_CONCEPT_ID_SITE
        ,CASE
     		WHEN format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') is NULL
            then format(GETDATE(), 'yyyy-MM-dd HH:mm:ss')  
	        else format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss')
		 end AS modified_dtTm
   FROM UNM_CNExTCases.dbo.Tumor rsSource
   JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.uk = rsSource.fk1
   JOIN UNM_CNExTCases.dbo.Hospital HSP ON rsSource.uk = HSP.fk2
   JOIN UNM_CNExTCases.dbo.HospSupp HSG ON HSG.UK = HSP.UK
   JOIN UNM_CNExTCases.dbo.Chemo CHM ON CHM.fk2 = rsSource.uk
  INNER JOIN  UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK = HExt.UK
  WHERE CHM.F05037 IN ('01', '02', '03')
    AND CHM.F05669 > '00'
	and ((CHM.F05189 is not null and CHM.F05189 != '' and CHM.F05189 not in ('00000000','19000000','05  0532','07  0142','07  0252','07  0352','88888888'))
	 or (CHM.F05214 is not null and CHM.F05214 != '' and CHM.F05214 not in ('00000000','88888888')))
    and HSP.F00006 not in (999999998, 9999998, 999999, 9999)
    and HSP.F00006 >= 1000
UNION
SELECT DISTINCT 'CNEXT HORMONE(OMOP_VISIT_DETAIL)' AS IDENTITY_CONTEXT
        ,rsSource.uk AS SOURCE_PK
        ,HOR.uk AS VISIT_DETAIL_ID
    	,PAT.UK AS PERSON_ID
    	,ISNULL(HSG.F05522, '') AS VISIT_DETAIL_CONCEPT_ID
		,case
		     when HOR.F05191 = '99999999'
			 then ''
			 when right(HOR.F05191,4) = '9999'
			 then ISNULL(FORMAT(TRY_CAST(left(HOR.F05191,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '') 
			 when right(HOR.F05191,2) = '99'
			 then ISNULL(FORMAT(TRY_CAST(left(HOR.F05191,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
			 else ISNULL(FORMAT(TRY_CAST(HOR.F05191 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		 end AS VISIT_DETAIL_START_DATE
		,case
		     when HOR.F05191 = '99999999'
			 then ''
			 when right(HOR.F05191,4) = '9999'
			 then ISNULL(FORMAT(TRY_CAST(left(HOR.F05191,4) + '0101' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '') 
			 when right(HOR.F05191,2) = '99'
			 then ISNULL(FORMAT(TRY_CAST(left(HOR.F05191,6) + '01' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
			 else ISNULL(FORMAT(TRY_CAST(HOR.F05191 AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		 end AS VISIT_DETAIL_START_DATETIME
		,case 
		      when HOR.F05216 = '99999999'
			  then ''
		      when right(HOR.F05216, 4) = '9999'
		      then ISNULL(FORMAT(TRY_CAST(left(HOR.F05216,4) + '0101' AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
			  when right(HOR.F05216, 2) = '99'
			  then ISNULL(FORMAT(TRY_CAST(left(HOR.F05216,6) + '01' AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
			  else ISNULL(FORMAT(TRY_CAST(HOR.F05216 AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
		 end AS VISIT_DETAIL_END_DATE
		,case 
		      when HOR.F05216 = '99999999'
			  then ''
		      when right(HOR.F05216, 4) = '9999'
		      then ISNULL(FORMAT(TRY_CAST(left(HOR.F05216,4) + '0101' AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
			  when right(HOR.F05216, 2) = '99'
			  then ISNULL(FORMAT(TRY_CAST(left(HOR.F05216,6) + '01' AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
			  else ISNULL(FORMAT(TRY_CAST(HOR.F05216 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
		 end AS VISIT_DETAIL_END_DATETIME
    	,'1791@32' AS VISIT_DETAIL_TYPE_CONCEPT_ID
    	,ISNULL(HOR.F05158, '') AS PROVIDER_ID                            /*2460*/
        ,ISNULL(HOR.F03480, '') AS CARE_SITE_ID
		,HOR.F05063 AS VISIT_DETAIL_SOURCE_VALUE
	    ,'710@' + HOR.F05063 AS VISIT_DETAIL_SOURCE_CONCEPT_ID
		,ISNULL(HExt.F01684, '') AS ADMITTING_SOURCE_VALUE
	    ,ISNULL(HExt.F03715, '') AS ADMITTING_SOURCE_CONCEPT_ID
	    ,ISNULL(HExt.F01685, '') AS DISCHARGE_TO_SOURCE_VALUE
	    ,ISNULL(HExt.F03716, '') AS DISCHARGE_TO_CONCEPT_ID
	    ,ISNULL(HSP.fk2, '') AS PRECEDING_VISIT_DETAIL_ID
	    ,'' AS VISIT_DETAIL_PARENT_ID
	    ,rsSource.UK AS VISIT_OCCURRENCE_ID                                /*10*/
		,ISNULL(HSP.F00006, '') AS MRN
		,ISNULL(STUFF(rsSource.F00152,4,0,'.'), '') AS CONDITION_CONCEPT_ID_SITE
   	    ,CASE
    		WHEN format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') is NULL
            then format(GETDATE(), 'yyyy-MM-dd HH:mm:ss')  
	        else format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss')
		 end AS modified_dtTm
   FROM UNM_CNExTCases.dbo.Tumor rsSource
   JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.uk = rsSource.fk1
   JOIN UNM_CNExTCases.dbo.Hospital HSP ON rsSource.uk = HSP.fk2
   JOIN UNM_CNExTCases.dbo.HospSupp HSG ON HSG.UK = HSP.UK
   JOIN UNM_CNExTCases.dbo.Hormone HOR ON HOR.fk2 = rsSource.uk
  INNER JOIN  UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK = HExt.UK
  WHERE HOR.F05063 = '01'
    and ((HOR.F05191 is not null and HOR.F05191 != '' and HOR.F05191 not in ('00000000','88888888'))
     or (HOR.F05216 is not null and HOR.F05216 != '' and HOR.F05216 not in('00000000','88888888')))
	and HSP.F00006 not in (999999998, 9999998, 999999, 9999)
    and HSP.F00006 >= 1000 
UNION
SELECT DISTINCT 'CNEXT IMMUNO(OMOP_VISIT_DETAIL)' AS IDENTITY_CONTEXT
        ,rsSource.uk AS SOURCE_PK
        ,BRM.uk AS VISIT_DETAIL_ID
    	,PAT.UK AS PERSON_ID
    	,ISNULL(HSG.F05522, '') AS VISIT_DETAIL_CONCEPT_ID
		,case
		     when BRM.F05193 = '99999999'
			 then ''
			 when right(BRM.F05193,4) = '9999'
			 then ISNULL(FORMAT(TRY_CAST(left(BRM.F05193,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '') 
			 when right(BRM.F05193,2) = '99'
			 then ISNULL(FORMAT(TRY_CAST(left(BRM.F05193,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
			 else ISNULL(FORMAT(TRY_CAST(BRM.F05193 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		 end AS VISIT_DETAIL_START_DATE
		,case
		     when BRM.F05193 = '99999999'
			 then ''
			 when right(BRM.F05193,4) = '9999'
			 then ISNULL(FORMAT(TRY_CAST(left(BRM.F05193,4) + '0101' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '') 
			 when right(BRM.F05193,2) = '99'
			 then ISNULL(FORMAT(TRY_CAST(left(BRM.F05193,6) + '01' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
			 else ISNULL(FORMAT(TRY_CAST(BRM.F05193 AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		 end AS VISIT_DETAIL_START_DATETIME
		,case 
		      when BRM.F05218 = '99999999'
			  then ''
		      when right(BRM.F05218, 4) = '9999'
		      then ISNULL(FORMAT(TRY_CAST(left(BRM.F05218,4) + '0101' AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
			  when right(BRM.F05218, 2) = '99'
			  then ISNULL(FORMAT(TRY_CAST(left(BRM.F05218,6) + '01' AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
			  else ISNULL(FORMAT(TRY_CAST(BRM.F05218 AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
		 end AS VISIT_DETAIL_END_DATE
		,case 
		      when BRM.F05218 = '99999999'
			  then ''
		      when right(BRM.F05218, 4) = '9999'
		      then ISNULL(FORMAT(TRY_CAST(left(BRM.F05218,4) + '0101' AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
			  when right(BRM.F05218, 2) = '99'
			  then ISNULL(FORMAT(TRY_CAST(left(BRM.F05218,6) + '01' AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
			  else ISNULL(FORMAT(TRY_CAST(BRM.F05218 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
		 end AS VISIT_DETAIL_END_DATETIME
    	,'1791@32' AS VISIT_DETAIL_TYPE_CONCEPT_ID
    	,ISNULL(BRM.F05159, '') AS PROVIDER_ID                            /*2460*/
        ,ISNULL(BRM.F03481, '') AS CARE_SITE_ID                           /*540*/
    	,BRM.F05066 AS VISIT_DETAIL_SOURCE_VALUE
	    ,'720@' + BRM.F05066 AS VISIT_DETAIL_SOURCE_CONCEPT_ID
		,ISNULL(HExt.F01684, '') AS ADMITTING_SOURCE_VALUE
	    ,ISNULL(HExt.F03715, '') AS ADMITTING_SOURCE_CONCEPT_ID
	    ,ISNULL(HExt.F01685, '') AS DISCHARGE_TO_SOURCE_VALUE
	    ,ISNULL(HExt.F03716, '') AS DISCHARGE_TO_CONCEPT_ID
	    ,ISNULL(HSP.fk2, '') AS PRECEDING_VISIT_DETAIL_ID
	    ,'' AS VISIT_DETAIL_PARENT_ID
	    ,rsSource.UK AS VISIT_OCCURRENCE_ID                               /*10*/
		,ISNULL(HSP.F00006, '') AS MRN
		,ISNULL(STUFF(rsSource.F00152,4,0,'.'), '') AS CONDITION_CONCEPT_ID_SITE
   	    ,CASE
    		WHEN format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') is NULL
            then format(GETDATE(), 'yyyy-MM-dd HH:mm:ss')  
	        else format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss')
		 end AS modified_dtTm
   FROM UNM_CNExTCases.dbo.Tumor rsSource
   JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.uk = rsSource.fk1
   JOIN UNM_CNExTCases.dbo.Hospital HSP ON rsSource.uk = HSP.fk2
   JOIN UNM_CNExTCases.dbo.HospSupp HSG ON HSG.UK = HSP.UK
   JOIN UNM_CNExTCases.dbo.Immuno BRM ON BRM.fk2 = rsSource.uk
  INNER JOIN  UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK = HExt.UK
  WHERE BRM.F05066 = '01'
    and ((BRM.F05193 is not null and BRM.F05193 != '' and BRM.F05193 not in('00000000','88888888'))
     or (BRM.F05218 is not null and BRM.F05218 != '' and BRM.F05218 not in('00000000','88888888')))
	and HSP.F00006 not in (999999998, 9999998, 999999, 9999)
    and HSP.F00006 >= 1000 
UNION
SELECT DISTINCT 'CNEXT OTHER(OMOP_VISIT_DETAIL)' AS IDENTITY_CONTEXT
        ,rsSource.uk AS SOURCE_PK
        ,OTH.uk AS VISIT_DETAIL_ID
    	,PAT.UK AS PERSON_ID
    	,ISNULL(HSG.F05522, '') AS VISIT_DETAIL_CONCEPT_ID
		,case
		     when OTH.F05195 = '99999999'
			 then ''
			 when right(OTH.F05195,4) = '9999'
			 then ISNULL(FORMAT(TRY_CAST(left(OTH.F05195,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '') 
			 when right(OTH.F05195,2) = '99'
			 then ISNULL(FORMAT(TRY_CAST(left(OTH.F05195,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
			 else ISNULL(FORMAT(TRY_CAST(OTH.F05195 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		 end AS VISIT_DETAIL_START_DATE
		,case
		     when OTH.F05195 = '99999999'
			 then ''
			 when right(OTH.F05195,4) = '9999'
			 then ISNULL(FORMAT(TRY_CAST(left(OTH.F05195,4) + '0101' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '') 
			 when right(OTH.F05195,2) = '99'
			 then ISNULL(FORMAT(TRY_CAST(left(OTH.F05195,6) + '01' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
			 else ISNULL(FORMAT(TRY_CAST(OTH.F05195 AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		 end AS VISIT_DETAIL_START_DATETIME
        ,'' AS VISIT_DETAIL_END_DATE
        ,'' AS VISIT_DETAIL_END_DATETIME 
    	,'1791@32' AS VISIT_DETAIL_TYPE_CONCEPT_ID
    	,ISNULL(OTH.F05160, '') AS PROVIDER_ID                            /*2460*/
        ,ISNULL(OTH.F05067, '') AS CARE_SITE_ID                           /*540*/
    	,OTH.F05069 AS VISIT_DETAIL_SOURCE_VALUE
	    ,'730@' + OTH.F05069 AS VISIT_DETAIL_SOURCE_CONCEPT_ID
		,ISNULL(HExt.F01684, '') AS ADMITTING_SOURCE_VALUE
	    ,ISNULL(HExt.F03715, '') AS ADMITTING_SOURCE_CONCEPT_ID
	    ,ISNULL(HExt.F01685, '') AS DISCHARGE_TO_SOURCE_VALUE
	    ,ISNULL(HExt.F03716, '') AS DISCHARGE_TO_CONCEPT_ID
	    ,ISNULL(HSP.fk2, '') AS PRECEDING_VISIT_DETAIL_ID
	    ,'' AS VISIT_DETAIL_PARENT_ID
	    ,rsSource.UK AS VISIT_OCCURRENCE_ID                             /*10*/
		,ISNULL(HSP.F00006, '') AS MRN
		,ISNULL(STUFF(rsSource.F00152,4,0,'.'), '') AS CONDITION_CONCEPT_ID_SITE
		,CASE 
		    WHEN format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') is NULL
            then format(GETDATE(), 'yyyy-MM-dd HH:mm:ss')  
	        else format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss')
		 end AS modified_dtTm
   FROM UNM_CNExTCases.dbo.Tumor rsSource
   JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.uk = rsSource.fk1
   JOIN UNM_CNExTCases.dbo.Hospital HSP ON rsSource.uk = HSP.fk2
   JOIN UNM_CNExTCases.dbo.HospSupp HSG ON HSG.UK = HSP.UK
   JOIN UNM_CNExTCases.dbo.Other OTH ON OTH.fk2 = rsSource.uk
  INNER JOIN  UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK = HExt.UK
  WHERE OTH.F05069 in ( '1','2','3','6')
    and OTH.F05195 is not null and OTH.F05195 != '' and OTH.F05195 not in ('00000000','88888888')
    and HSP.F00006 not in (999999998, 9999998, 999999, 9999)
    and HSP.F00006 >= 1000
UNION
  SELECT DISTINCT 'CNEXT HOSP(OMOP_VISIT_DETAIL)' AS IDENTITY_CONTEXT
    ,rsSource.uk AS SOURCE_PK
    ,hsp.UK AS VISIT_DETAIL_ID
	,PAT.uk AS PERSON_ID
	,ISNULL(HSG.F05522, '') AS VISIT_DETAIL_CONCEPT_ID
	,case
		 when HExt.F00427 = '99999999'
		 then ''
		 when right(HExt.F00427,4) = '9999'
		 then ISNULL(FORMAT(TRY_CAST(left(HExt.F00427,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '') 
		 when right(HExt.F00427,2) = '99'
		 then ISNULL(FORMAT(TRY_CAST(left(HExt.F00427,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		 else ISNULL(FORMAT(TRY_CAST(HExt.F00427 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
	 end AS VISIT_DETAIL_START_DATE
	,case
		 when HExt.F00427 = '99999999'
		 then ''
		 when right(HExt.F00427,4) = '9999'
		 then ISNULL(FORMAT(TRY_CAST(left(HExt.F00427,4) + '0101' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '') 
		 when right(HExt.F00427,2) = '99'
		 then ISNULL(FORMAT(TRY_CAST(left(HExt.F00427,6) + '01' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		 else ISNULL(FORMAT(TRY_CAST(HExt.F00427 AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
	 end AS VISIT_DETAIL_START_DATETIME
	,case 
		 when HExt.F00128 = '99999999'
		 then ''
		 when right(HExt.F00128, 4) = '9999'
		 then ISNULL(FORMAT(TRY_CAST(left(HExt.F00128,4) + '0101' AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
		 when right(HExt.F00128, 2) = '99'
		 then ISNULL(FORMAT(TRY_CAST(left(HExt.F00128,6) + '01' AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
		 else ISNULL(FORMAT(TRY_CAST(HExt.F00128 AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
	 end AS VISIT_DETAIL_END_DATE
	,case 
		 when HExt.F00128 = '99999999'
		 then ''
		 when right(HExt.F00128, 4) = '9999'
		 then ISNULL(FORMAT(TRY_CAST(left(HExt.F00128,4) + '0101' AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
	     when right(HExt.F00128, 2) = '99'
		 then ISNULL(FORMAT(TRY_CAST(left(HExt.F00128,6) + '01' AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
		 else ISNULL(FORMAT(TRY_CAST(HExt.F00128 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
	 end AS VISIT_DETAIL_END_DATETIME
	,'1791@32' AS VISIT_DETAIL_TYPE_CONCEPT_ID
	,ISNULL(HExt.F00675, '') AS PROVIDER_ID
    ,ISNULL(PEX.F00003, '') AS CARE_SITE_ID
	,ISNULL(HExt.F03564, '') AS VISIT_DETAIL_SOURCE_VALUE
	,CASE
		WHEN HExt.F03564 <> ''
		THEN '3250@' + HExt.F03564
		ELSE ''
	 END AS VISIT_DETAIL_SOURCE_CONCEPT_ID
	,ISNULL(HExt.F01684, '') AS ADMITTING_SOURCE_VALUE
	,ISNULL(HExt.F03715, '') AS ADMITTING_SOURCE_CONCEPT_ID
	,ISNULL(HExt.F01685, '') AS DISCHARGE_TO_SOURCE_VALUE
	,ISNULL(HExt.F03716, '') AS DISCHARGE_TO_CONCEPT_ID
	,ISNULL(HSP.fk2, '') AS PRECEDING_VISIT_DETAIL_ID
	,'' AS VISIT_DETAIL_PARENT_ID
	,rsSource.UK AS VISIT_OCCURRENCE_ID
	,ISNULL(HSP.F00006, '') AS MRN
	,ISNULL(STUFF(rsSource.F00152,4,0,'.'), '') AS CONDITION_CONCEPT_ID_SITE
	,CASE 
		WHEN format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') is NULL
        then format(GETDATE(), 'yyyy-MM-dd HH:mm:ss')  
	    else format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss')
	 end AS modified_dtTm
  FROM UNM_CNExTCases.dbo.Tumor rsSource
  JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.uk = rsSource.fk1
  JOIN UNM_CNExTCases.dbo.PatExtended PEX ON PEX.uk = PAT.UK
  JOIN UNM_CNExTCases.dbo.Hospital HSP ON rsSource.uk = HSP.fk2
  JOIN UNM_CNExTCases.dbo.HospSupp HSG ON HSG.UK = HSP.UK
 INNER JOIN  UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK = HExt.UK 
WHERE ((HExt.F00427 is not null and HExt.F00427 != '' and HExt.F00427 not in ('00000000','88888888'))
   or (HExt.F00128 is not null and HExt.F00128 != '' and HExt.F00128 not in('00000000','88888888')))
  and HSP.F00006 not in (999999998, 9999998, 999999, 9999)
  and HSP.F00006 >= 1000 