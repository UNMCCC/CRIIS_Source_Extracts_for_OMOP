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
   
SELECT 'IDENTITY_CONTEXT|SOURCE_PK|NOTE_ID|PERSON_ID|NOTE_EVENT_ID|NOTE_EVENT_FIELD_CONCEPT_ID|NOTE_DATE|NOTE_DATETIME|NOTE_TYPE_CONCEPT_ID|NOTE_CLASS_CONCEPT_ID|NOTE_TITLE_1|NOTE_TEXT_1|NOTE_TITLE_2|NOTE_TEXT_2|NOTE_TITLE_3|NOTE_TEXT_3|NOTE_TITLE_4|NOTE_TEXT_4|NOTE_TITLE_5|NOTE_TEXT_5|NOTE_TITLE_6|NOTE_TEXT_6|NOTE_TITLE_7|NOTE_TEXT_7|NOTE_TITLE_8|NOTE_TEXT_8|NOTE_TITLE_9|NOTE_TEXT_9|ENCODING_CONCEPT_ID|LANGUAGE_CONCEPT_ID|PROVIDER_ID|VISIT_OCCURRENCE_ID|VISIT_DETAIL_ID|NOTE_SOURCE_VALUE|ACCESSION_NUMBER|MRN|Modified_DtTm';
SELECT  'CNEXT FOLLOWUP(OMOP_NOTES)' AS IDENTITY_CONTEXT
        ,rsSource.uk AS SOURCE_PK
        ,rsSource.uk AS NOTE_ID
        ,PAT.UK AS PERSON_ID
        ,rsSource.uk AS NOTE_EVENT_ID
		,'' AS NOTE_EVENT_FIELD_CONCEPT_ID
        ,case
		   when rsSource.F00029 = '00000000'
		   then ''
		   when right(rsSource.F00029, 4) = '9999'
		   then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		   when right(rsSource.F00029,2) = '99'
           then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		   else ISNULL(FORMAT(TRY_CAST(rsSource.F00029 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		 END AS NOTE_DATE 
        ,case
		   when rsSource.F00029 = '00000000'
		   then ''
		   when right(rsSource.F00029, 4) = '9999'
		   then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,4) + '0101' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		   when right(rsSource.F00029,2) = '99'
           then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,6) + '01' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		   else ISNULL(FORMAT(TRY_CAST(rsSource.F00029 AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		 END AS NOTE_DATETIME
		,'1791@32' AS NOTE_TYPE_CONCEPT_ID
		,'' AS NOTE_CLASS_CONCEPT_ID
		,CASE WHEN F01220 IS NOT NULL THEN 
	       'Text_Follow_Up_Notes' 
	      ELSE '' END AS NOTE_TITLE_1
		,CASE WHEN F01220 IS NOT NULL THEN 
	        isNULL(RTRIM(REPLACE(REPLACE(REPLACE(CAST(F01220 as NVarchar(4000)), CHAR(13), ''), CHAR(10), ''), '|','-' )), '')				 
	      ELSE '' END AS  NOTE_TEXT_1                                                                                                /*2680*/ 
		,CASE WHEN F01506 IS NOT NULL THEN 
	        'Text_Follow_Up_Remarks' 
		  ELSE '' END AS NOTE_TITLE_2
		,CASE WHEN F01506 IS NOT NULL THEN 
	        isNULL(RTRIM( REPLACE(REPLACE(REPLACE(CAST(F01506 as NVarchar(4000)), CHAR(13), ''), CHAR(10), ''), '|','-' )), '')	
          ELSE '' END AS  NOTE_TEXT_2                                                                                                /*2580*/
		,'' AS NOTE_TITLE_3
		,'' AS NOTE_TEXT_3
		,'' AS NOTE_TITLE_4
		,'' AS NOTE_TEXT_4
		,'' AS NOTE_TITLE_5
		,'' AS NOTE_TEXT_5
		,'' AS NOTE_TITLE_6
		,'' AS NOTE_TEXT_6
		,'' AS NOTE_TITLE_7
		,'' AS NOTE_TEXT_7
		,'' AS NOTE_TITLE_8
		,'' AS NOTE_TEXT_8
		,'' AS NOTE_TITLE_9
		,'' AS NOTE_TEXT_9
		,'UTF-8 (32678)' AS ENCODING_CONCEPT_ID
		,'4182347' AS LANGUAGE_CONCEPT_ID
		,ISNULL(HExt.F00675, '') AS PROVIDER_ID
		,rsSource.uk AS VISIT_OCCURRENCE_ID
        ,'' AS VISIT_DETAIL_ID 
        ,'UNM_CNExTCases.dbo.Tumor.uk' AS NOTE_SOURCE_VALUE
        ,ISNULL(HSP.F00016, '') AS ACCESSION_NUMBER
		,ISNULL(HSP.F00006, '') AS MRN
		,CASE WHEN format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') is NULL THEN
            FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss')  
	     ELSE FORMAT(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') END
		 AS Modified_dtTm
  FROM UNM_CNExTCases.dbo.Tumor rsSource
  JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.UK = rsSource.FK1
  JOIN UNM_CNExTCases.dbo.FollowUp rsTarget ON rsTarget.uk = rsSource.uk
  JOIN UNM_CNExTCases.dbo.Hospital HSP on HSP.FK2=rsSource.UK
  JOIN UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK = HExt.UK
 where HSP.F00006 not in (999999998, 9999998, 999999, 9999)
   and HSP.F00006 >= 1000
   and HExt.F00084 >= @fromDate
UNION ALL
SELECT 'CNEXT TUMOR(OMOP_NOTES)' AS IDENTITY_CONTEXT
        ,rsSource.uk AS SOURCE_PK
        ,rsSource.uk AS NOTE_ID
        ,PAT.UK AS PERSON_ID
        ,rsSource.uk AS NOTE_EVENT_ID
		,'' AS NOTE_EVENT_FIELD_CONCEPT_ID
        ,case
		   when rsSource.F00029 = '00000000'
		   then ''
		   when right(rsSource.F00029, 4) = '9999'
		   then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		   when right(rsSource.F00029,2) = '99'
           then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		   else ISNULL(FORMAT(TRY_CAST(rsSource.F00029 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		 END AS NOTE_DATE 
        ,case
		   when rsSource.F00029 = '00000000'
		   then ''
		   when right(rsSource.F00029, 4) = '9999'
		   then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,4) + '0101' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		   when right(rsSource.F00029,2) = '99'
           then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,6) + '01' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		   else ISNULL(FORMAT(TRY_CAST(rsSource.F00029 AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		 END AS NOTE_DATETIME
		,'1791@32' AS NOTE_TYPE_CONCEPT_ID
		,'' AS NOTE_CLASS_CONCEPT_ID
		,CASE WHEN F00030 IS NOT NULL THEN 
	        'Text_Final_Dx' 
	      ELSE '' END AS NOTE_TITLE_1
		,CASE WHEN F00030 IS NOT NULL THEN 
 	        isNULL(RTRIM( REPLACE(REPLACE(REPLACE(CAST(F00030 as NVarchar(4000)), CHAR(13), ''), CHAR(10), ''), '|','-' )), '') 
		  ELSE '' END AS  NOTE_TEXT_1                                                                                                /*2680*/ 
		,CASE WHEN F00089 IS NOT NULL THEN 
	       'Text_Primary_Site' 
		 ELSE '' END AS NOTE_TITLE_2
		,CASE WHEN F00089 IS NOT NULL THEN 
    	    isNULL(RTRIM( REPLACE(REPLACE(REPLACE(CAST(F00089 as NVarchar(4000)), CHAR(13), ''), CHAR(10), ''), '|','-' )), '') 
         ELSE '' END AS  NOTE_TEXT_2                                                                                                /*2580*/
		,CASE WHEN F00090 IS NOT NULL THEN 
	       'Text_Histology' 
		  ELSE '' END AS NOTE_TITLE_3
		,CASE WHEN F00090 IS NOT NULL THEN 
			isNULL(RTRIM( REPLACE(REPLACE(REPLACE(CAST(F00090 as NVarchar(4000)), CHAR(13), ''), CHAR(10), ''), '|','-' )), '') 
		  ELSE '' END AS  NOTE_TEXT_3                                                                                                /*2590*/
		,CASE WHEN F01209 IS NOT NULL THEN 
	       'Text_Scopes' 
		  ELSE '' END AS NOTE_TITLE_4
		,CASE WHEN F01209 IS NOT NULL THEN 
    	    isNULL(RTRIM( REPLACE(REPLACE(REPLACE(CAST(F01209 as NVarchar(4000)), CHAR(13), ''), CHAR(10), ''), '|','-' )), '')
          ELSE '' END AS  NOTE_TEXT_4                                                                                                /*2540*/
		,CASE WHEN F01210 IS NOT NULL THEN 
	       'Text_Labs' 
	      ELSE '' END AS NOTE_TITLE_5
		,CASE WHEN F01210 IS NOT NULL THEN 
     	    isNULL(RTRIM( REPLACE(REPLACE(REPLACE(CAST(F01210 as NVarchar(4000)), CHAR(13), ''), CHAR(10), ''), '|','-' )), '')
          ELSE '' END AS  NOTE_TEXT_5                                                                                                /*2550*/
		,CASE WHEN F01211 IS NOT NULL THEN 
	       'Text_Physical_Exam' 
         ELSE '' END AS NOTE_TITLE_6
		,CASE WHEN F01211 IS NOT NULL THEN 
	        isNULL(RTRIM( REPLACE(REPLACE(REPLACE(CAST(F01211 as NVarchar(4000)), CHAR(13), ''), CHAR(10), ''), '|','-' )), '')
		 ELSE '' END AS  NOTE_TEXT_6                                                                                                /*2520*/
		,CASE WHEN F01212 IS NOT NULL THEN 
	       'Text_Xrays_Scans' 
		  ELSE '' END AS NOTE_TITLE_7
		,CASE WHEN F01212 IS NOT NULL THEN 
			isNULL(RTRIM( REPLACE(REPLACE(REPLACE(CAST(F01212 as NVarchar(4000)), CHAR(13), ''), CHAR(10), ''), '|','-' )), '')
		  ELSE '' END AS  NOTE_TEXT_7                                                                                                /*2530*/
		,CASE WHEN F01213 IS NOT NULL THEN 
	        'Text_Pathology' 
          ELSE '' END AS NOTE_TITLE_8
		,CASE WHEN F01213 IS NOT NULL THEN 
        	isNULL(RTRIM( REPLACE(REPLACE(REPLACE(CAST(F01213 as NVarchar(4000)), CHAR(13), ''), CHAR(10), ''), '|','-' )), '')
	      ELSE '' END AS  NOTE_TEXT_8                                                                                                /*2570*/
		,CASE WHEN F01214 IS NOT NULL THEN 
	        'Text_Operative_Findings' 
		  ELSE '' END AS NOTE_TITLE_9
		,CASE WHEN F01214 IS NOT NULL THEN 
     	    isNULL(RTRIM( REPLACE(REPLACE(REPLACE(CAST(F01214 as NVarchar(4000)), CHAR(13), ''), CHAR(10), ''), '|','-' )), '')
          ELSE '' END AS  NOTE_TEXT_9                                                                                                /*2560*/
        ,'UTF-8 (32678)' AS ENCODING_CONCEPT_ID
		,'4182347' AS LANGUAGE_CONCEPT_ID
		,ISNULL(HExt.F00675, '') AS PROVIDER_ID
		,rsSource.uk AS VISIT_OCCURRENCE_ID
        ,'' AS VISIT_DETAIL_ID 
        ,'UNM_CNExTCases.dbo.Tumor.uk' AS NOTE_SOURCE_VALUE
        ,ISNULL(HSP.F00016, '') AS ACCESSION_NUMBER
		,ISNULL(HSP.F00006, '') AS MRN
        ,CASE WHEN format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') is NULL THEN
            FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss')  
	     ELSE FORMAT(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') END
		 AS Modified_dtTm
  FROM UNM_CNExTCases.dbo.Tumor rsSource
  JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.UK = rsSource.FK1
  JOIN UNM_CNExTCases.dbo.Hospital HSP on HSP.FK2=rsSource.UK
  JOIN UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK = HExt.UK
  and HSP.F00006 not in (999999998, 9999998, 999999, 9999)
  and HSP.F00006 >= 1000
  and HExt.F00084 >= @fromDate
UNION all
SELECT 'CNEXT TREATMENT(OMOP_NOTES)' AS IDENTITY_CONTEXT
        ,rsSource.uk AS SOURCE_PK
        ,rsSource.uk AS NOTE_ID
        ,PAT.UK AS PERSON_ID
        ,rsSource.uk AS NOTE_EVENT_ID
		,'' AS NOTE_EVENT_FIELD_CONCEPT_ID
        ,case
		   when rsSource.F00029 = '00000000'
		   then ''
		   when right(rsSource.F00029, 4) = '9999'
		   then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		   when right(rsSource.F00029,2) = '99'
           then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		   else ISNULL(FORMAT(TRY_CAST(rsSource.F00029 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		 END AS NOTE_DATE 
        ,case
		   when rsSource.F00029 = '00000000'
		   then ''
		   when right(rsSource.F00029, 4) = '9999'
		   then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,4) + '0101' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		   when right(rsSource.F00029,2) = '99'
           then ISNULL(FORMAT(TRY_CAST(left(rsSource.F00029,6) + '01' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		   else ISNULL(FORMAT(TRY_CAST(rsSource.F00029 AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		 END AS NOTE_DATETIME
		,'1791@32' AS NOTE_TYPE_CONCEPT_ID
		,'' AS NOTE_CLASS_CONCEPT_ID
        ,CASE WHEN CAST(F01215 AS VARCHAR(100)) IS NOT NULL AND CAST(F01215 AS VARCHAR(100)) !=  ' ' THEN 
		         'Text_Radiation_Beam_Summary' 
		      ELSE '' END AS NOTE_TITLE_1
        ,CASE WHEN F01215 IS NOT NULL THEN 
	    	    isNULL(RTRIM( REPLACE(REPLACE(REPLACE(CAST(F01215 as NVarchar(4000)), CHAR(13), ''), CHAR(10), ''), '|','-' )), '')
	         ELSE '' END AS  NOTE_TEXT_1
        ,CASE WHEN F01216 IS NOT NULL AND CAST(F01216 AS VARCHAR(100)) !=  ' ' THEN 
		        'Text_Chemotherapy_Summary' 
		     ELSE '' END AS NOTE_TITLE_2
        ,CASE WHEN F01216 IS NOT NULL THEN 
    	    	isNULL(RTRIM( REPLACE(REPLACE(REPLACE(CAST(F01216 as NVarchar(4000)), CHAR(13), ''), CHAR(10), ''), '|','-' )), '')
		     ELSE '' END AS  NOTE_TEXT_2
        ,CASE WHEN F01217 IS NOT NULL  AND CAST(F01217 AS VARCHAR(100)) !=  ' ' THEN 
		        'Text_Hormone_Summary' 
		      ELSE '' END AS NOTE_TITLE_3
        ,CASE WHEN F01217 IS NOT NULL THEN 
		        isNULL(RTRIM( REPLACE(REPLACE(REPLACE(CAST(F01217 as NVarchar(4000)), CHAR(13), ''), CHAR(10), ''), '|','-' )), '')
	          ELSE '' END AS  NOTE_TEXT_3
        ,CASE WHEN F01218 IS NOT NULL  AND CAST(F01218 AS VARCHAR(100)) !=  ' ' THEN 
		        'Text_Immunotherapy_Summary' 
		      ELSE '' END AS NOTE_TITLE_4
        ,CASE WHEN F01218 IS NOT NULL THEN 
		       isNULL(RTRIM( REPLACE(REPLACE(REPLACE(CAST(F01218 as NVarchar(4000)), CHAR(13), ''), CHAR(10), ''), '|','-' )), '')
	          ELSE '' END AS  NOTE_TEXT_4
        ,CASE WHEN F01219 IS NOT NULL  AND CAST(F01219 AS VARCHAR(100)) !=  ' ' THEN 
		        'Text_Other_Therapy_Summary' 
		      ELSE '' END AS NOTE_TITLE_5
        ,CASE WHEN F01219 IS NOT NULL THEN 
       		   isNULL(RTRIM( REPLACE(REPLACE(REPLACE(CAST(F01219 as NVarchar(4000)), CHAR(13), ''), CHAR(10), ''), '|','-' )), '')
		      ELSE '' END AS  NOTE_TEXT_5
        ,CASE WHEN F01223 IS NOT NULL  AND CAST(F01223 AS VARCHAR(100)) !=  ' ' THEN 
		       'Text_Staging' 
		      ELSE '' END AS NOTE_TITLE_6
        ,CASE WHEN F01223 IS NOT NULL THEN 
       		    isNULL(RTRIM( REPLACE(REPLACE(REPLACE(CAST(F01223 as NVarchar(4000)), CHAR(13), ''), CHAR(10), ''), '|','-' )), '')
		      ELSE '' END AS  NOTE_TEXT_6
        ,CASE WHEN F01351 IS NOT NULL  AND CAST(F01351 AS VARCHAR(100)) !=  ' ' THEN 
		        'Text_Surgery_Summary' 
		      ELSE '' END AS NOTE_TITLE_7
        ,CASE WHEN F01351 IS NOT NULL THEN 
    		   isNULL(RTRIM( REPLACE(REPLACE(REPLACE(CAST(F01351 as NVarchar(4000)), CHAR(13), ''), CHAR(10), ''), '|','-' )), '') 
		      ELSE '' END AS  NOTE_TEXT_7
        ,CASE WHEN F01413 IS NOT NULL  AND CAST(F01413 AS VARCHAR(100)) !=  ' ' THEN 
		        'Text_Before_1998' 
		      ELSE '' END AS NOTE_TITLE_8
        ,CASE WHEN F01413 IS NOT NULL THEN 
        		isNULL(RTRIM( REPLACE(REPLACE(REPLACE(CAST(F01413 as NVarchar(4000)), CHAR(13), ''), CHAR(10), ''), '|','-' )), '')
	          ELSE '' END AS  NOTE_TEXT_8
        ,CASE WHEN F05952 IS NOT NULL  AND CAST(F05952 AS VARCHAR(100)) !=  ' ' THEN 
		       'Text_Radiation_Other_Summary' 
		      ELSE '' END AS NOTE_TITLE_9
        ,CASE WHEN F05952 IS NOT NULL THEN 
		       isNULL(RTRIM( REPLACE(REPLACE(REPLACE(CAST(F05952 as NVarchar(4000)), CHAR(13), ''), CHAR(10), ''), '|','-' )), '')
	        ELSE '' END AS  NOTE_TEXT_9
	    ,'UTF-8 (32678)' AS ENCODING_CONCEPT_ID
	    ,'4182347' AS LANGUAGE_CONCEPT_ID
        ,ISNULL(HExt.F00675, '') AS PROVIDER_ID
		,rsSource.uk AS VISIT_OCCURRENCE_ID
        ,'' AS VISIT_DETAIL_ID 
        ,'UNM_CNExTCases.dbo.Tumor.uk' AS NOTE_SOURCE_VALUE
        ,ISNULL(HSP.F00016, '') AS ACCESSION_NUMBER
		,ISNULL(HSP.F00006, '') AS MRN
       ,CASE WHEN format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') is NULL THEN
            FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss')  
	     ELSE FORMAT(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') END
		 AS Modified_dtTm
  FROM UNM_CNExTCases.dbo.Tumor rsSource
  JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.UK = rsSource.FK1
  JOIN UNM_CNExTCases.dbo.Treatment TRT on TRT.UK = rsSource.UK
  JOIN UNM_CNExTCases.dbo.Hospital HSP on HSP.FK2=rsSource.UK
  JOIN UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK = HExt.UK
 WHERE TRT.F00420 NOT IN ('00','09') 
   and HSP.F00006 not in (999999998, 9999998, 999999, 9999)
   and HSP.F00006 >= 1000
   and HExt.F00084 >= @fromDate
 ORDER BY NOTE_TYPE_CONCEPT_ID
           ,rsSource.UK DESC