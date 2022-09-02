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
   
SELECT 'IDENTITY_CONTEXT|SOURCE_PK|PROCEDURE_OCCURRENCE_ID|PERSON_ID|PROCEDURE_CONCEPT_ID|PROCEDURE_DATE|PROCEDURE_DATETIME|PROCEDURE_TYPE_CONCEPT_ID|MODIFIER_CONCEPT_ID|QUANTITY|PROVIDER_ID|VISIT_OCCURRENCE_ID|VISIT_DETAIL_ID|PROCEDURE_SOURCE_VALUE|PROCEDURE_SOURCE_CONCEPT_ID|MODIFIER_SOURCE_VALUE|MRN|CONDITION_CONCEPT_ID_SITE|Modified_DtTm';
SELECT  'CNEXT DIAGNOSIS/STAGE(OMOP_PROCEDURE_OCCURRENCE)' AS IDENTITY_CONTEXT                                                                           /*'UNMTR DX/STG RECORD'*/
         ,concat('DXS-', DXS.uk) AS SOURCE_PK
         ,concat('DXS-', DXS.uk) AS PROCEDURE_OCCURRENCE_ID
         ,PAT.uk AS PERSON_ID
         ,CASE
			WHEN DXS.F05084 <> '' 
			THEN '740@' + DXS.F05084
            ELSE ''
          END AS PROCEDURE_CONCEPT_ID
         ,case
		     when DXS.F05175 = '99999999'
		     then ''
		     when right(DXS.F05175, 4) = '9999'
		     then FORMAT(TRY_CAST(left(DXS.F05175,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss')
		     when right(DXS.F05175,2) = '99'
             then FORMAT(TRY_CAST(left(DXS.F05175,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss')
		     else ISNULL(FORMAT(TRY_CAST(DXS.F05175 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		    END AS PROCEDURE_DATE 
         ,case
		     when DXS.F05175 = '99999999'
		     then ''
		     when right(DXS.F05175, 4) = '9999'
		     then FORMAT(TRY_CAST(left(DXS.F05175,4) + '0101' AS DATETIME), 'yyyy-MM-dd HH:mm:ss')
		     when right(DXS.F05175,2) = '99'
             then FORMAT(TRY_CAST(left(DXS.F05175,6) + '01' AS DATETIME), 'yyyy-MM-dd HH:mm:ss')
		     else ISNULL(FORMAT(TRY_CAST(DXS.F05175 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
		    END AS PROCEDURE_DATETIME
    	 ,'1791@32' AS PROCEDURE_TYPE_CONCEPT_ID
         ,0 AS MODIFIER_CONCEPT_ID
         ,1 AS QUANTITY
         ,ISNULL(DXS.F05162, '') AS PROVIDER_ID
         ,rsSource.uk AS VISIT_OCCURRENCE_ID
         ,concat('DXS-', DXS.uk) AS VISIT_DETAIL_ID
         ,DXS.F05084 AS PROCEDURE_SOURCE_VALUE
         ,0 AS PROCEDURE_SOURCE_CONCEPT_ID
         ,0 AS MODIFIER_SOURCE_VALUE
		 ,ISNULL(HSP.F00006, '') AS MRN
		 ,ISNULL(STUFF(rsSource.F00152,4,0,'.'), '') AS CONDITION_CONCEPT_ID_SITE
	 ,CASE WHEN format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') is NULL
        then FORmat(GETDATE(), 'yyyy-MM-dd HH:mm:ss')  
	else format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') end
	AS modified_dtTm
  FROM UNM_CNExTCases.dbo.Tumor rsSource
  JOIN UNM_CNExTCases.dbo.DxStg DXS on DXS.fk2 = rsSource.uk
  JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.uk = rsSource.fk1
  JOIN UNM_CNExTCases.dbo.Hospital HSP on HSP.FK2 = rsSource.UK
  JOIN UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK = HExt.UK
 where DXS.F05084 NOT IN ('00','09')
   and HSP.F00006 not in (999999998, 9999998, 999999, 9999)
   and HSP.F00006 >= 1000
   and DXS.F05175 != '00000000'
   and HExt.F00084 >= @fromDate
UNION
SELECT 'CNEXT SURG(OMOP_PROCEDURE_OCCURRENCE)' AS IDENTITY_CONTEXT                                                                   /*'UNMTR SURGICAL RECORD'*/
        ,concat('SRG-', SRG.uk) AS SOURCE_PK
        ,concat('SRG-', SRG.uk) AS PROCEDURE_OCCURRENCE_ID
        ,PAT.uk AS PERSON_ID
        ,CASE                                  --no nulls, but empty space values to handle
			WHEN SRG.F03488 <> ''
			THEN '1290@'  + SRG.F03488
			ELSE ''
		 END AS PROCEDURE_CONCEPT_ID                                                                          /*670*/
         ,case
		     when SRG.F00434 = '99999999'
		     then ''
		     when right(SRG.F00434, 4) = '9999'
		     then FORMAT(TRY_CAST(left(SRG.F00434,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss')
		     when right(SRG.F00434,2) = '99'
             then FORMAT(TRY_CAST(left(SRG.F00434,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss')
		     else ISNULL(FORMAT(TRY_CAST(SRG.F00434 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		  END AS PROCEDURE_DATE
		  ,case
		     when SRG.F00434 = '99999999'
		     then ''
		     when right(SRG.F00434, 4) = '9999'
		     then FORMAT(TRY_CAST(left(SRG.F00434,4) + '0101' AS DATETIME), 'yyyy-MM-dd HH:mm:ss')
		     when right(SRG.F00434,2) = '99'
             then FORMAT(TRY_CAST(left(SRG.F00434,6) + '01' AS DATETIME), 'yyyy-MM-dd HH:mm:ss')
		     else ISNULL(FORMAT(TRY_CAST(SRG.F00434 AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		  END AS PROCEDURE_DATETIME
    	,'1791@32' AS PROCEDURE_TYPE_CONCEPT_ID
	    ,0 AS MODIFIER_CONCEPT_ID
		,1 AS QUANTITY
    	,ISNULL(SRG.F05161, '') AS PROVIDER_ID                                            /*2480*/
        ,rsSource.uk AS VISIT_OCCURRENCE_ID
        ,concat('SRG-', SRG.uk) AS VISIT_DETAIL_ID
        ,SRG.F03488 AS PROCEDURE_SOURCE_VALUE
	    ,0 AS PROCEDURE_SOURCE_CONCEPT_ID
	    ,0 AS MODIFIER_SOURCE_VALUE
		,ISNULL(HSP.F00006, '') AS MRN
		,ISNULL(STUFF(rsSource.F00152,4,0,'.'), '') AS CONDITION_CONCEPT_ID_SITE
		,CASE 
		    WHEN format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') is NULL
            then format(GETDATE(), 'yyyy-MM-dd HH:mm:ss')  
	        else format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss')
		 end AS modified_dtTm
   FROM UNM_CNExTCases.dbo.Tumor rsSource
   JOIN UNM_CNExTCases.dbo.Surg SRG ON SRG.fk2 = rsSource.uk
   JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.uk = rsSource.fk1
   JOIN UNM_CNExTCases.dbo.Hospital HSP ON HSP.fk2 = rsSource.uk
   JOIN UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK = HExt.UK
  WHERE SRG.F03488 > '00' 
    AND SRG.F03488 < '98'
	AND ((SRG.F00434 is not null AND SRG.F00434 != '' AND SRG.F00434 not in ('00000000','88888888'))
     or (SRG.F05169 is not null AND SRG.F05169 != '' AND SRG.F05169 not in ('00000000','88888888')))
	AND HSP.F00006 not in (999999998, 9999998, 999999, 9999)
    AND HSP.F00006 >= 1000
	and HExt.F00084 >= @fromDate
UNION
SELECT 'CNEXT RADIATION(OMOP_PROCEDURE_OCCURRENCE)' AS IDENTITY_CONTEXT                                                                  /*'UNMTR RADIATION RECORD'*/
        ,concat('RAD-', RAD.uk) AS SOURCE_PK
        ,concat('RAD-', RAD.uk) AS PROCEDURE_OCCURRENCE_ID
        ,PAT.uk AS PERSON_ID
       	,'1506@'  + RAD.F07799 AS PROCEDURE_CONCEPT_ID
		,case
		     when RAD.F05187 = '99999999'
		     then ''
		     when right(RAD.F05187, 4) = '9999'
		     then FORMAT(TRY_CAST(left(RAD.F05187,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss')
		     when right(RAD.F05187,2) = '99'
             then FORMAT(TRY_CAST(left(RAD.F05187,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss')
		     else ISNULL(FORMAT(TRY_CAST(RAD.F05187 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		  END AS PROCEDURE_DATE
		,case
		     when RAD.F05187 = '99999999'
		     then ''
		     when right(RAD.F05187, 4) = '9999'
		     then FORMAT(TRY_CAST(left(RAD.F05187,4) + '0101' AS DATETIME), 'yyyy-MM-dd HH:mm:ss')
		     when right(RAD.F05187,2) = '99'
             then FORMAT(TRY_CAST(left(RAD.F05187,6) + '01' AS DATETIME), 'yyyy-MM-dd HH:mm:ss')
		     else ISNULL(FORMAT(TRY_CAST(RAD.F05187 AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		  END AS PROCEDURE_DATETIME
    	,'1791@32' AS PROCEDURE_TYPE_CONCEPT_ID
	    ,0 AS MODIFIER_CONCEPT_ID
		,1 AS QUANTITY
    	,ISNULL(RAD.F05156, '') AS PROVIDER_ID                                             /*2480*/
        ,rsSource.uk AS VISIT_OCCURRENCE_ID
        ,concat('RAD-', RAD.uk) AS VISIT_DETAIL_ID
        ,RAD.F07799 AS PROCEDURE_SOURCE_VALUE    --nulls handled by predicate              /*740*/
	    ,0 AS PROCEDURE_SOURCE_CONCEPT_ID
	    ,0 AS MODIFIER_SOURCE_VALUE
		,ISNULL(HSP.F00006, '') AS MRN
		,ISNULL(STUFF(rsSource.F00152,4,0,'.'), '') AS CONDITION_CONCEPT_ID_SITE
	    ,CASE 
			WHEN format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') is NULL
            then format(GETDATE(), 'yyyy-MM-dd HH:mm:ss')  
	        else format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss')
		 end AS modified_dtTm
   FROM UNM_CNExTCases.dbo.Tumor rsSource
   JOIN UNM_CNExTCases.dbo.Radiation RAD ON RAD.fk2 = rsSource.uk
   JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.uk = rsSource.fk1
   JOIN UNM_CNExTCases.dbo.Hospital HSP ON HSP.fk2 = rsSource.uk
   JOIN UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK = HExt.UK
  WHERE RAD.F05257 > '000' 
    AND RAD.F07799 > '00'
    AND RAD.F07799 < '99'
	and ((RAD.F05187 is not NULL and RAD.F05187 != '' and RAD.F05187 not in ('00000000','88888888'))
	 or (RAD.F05212 is not NULL and RAD.F05212 != '' and RAD.F05212 not in ('00000000','88888888')))
	and HSP.F00006 not in (999999998, 9999998, 999999, 9999)
    and HSP.F00006 >= 1000
	and HExt.F00084 >= @fromDate
UNION
SELECT 'CNEXT OTHER(OMOP_PROCEDURE_OCCURRENCE)' AS IDENTITY_CONTEXT                                                                 /*'UNMTR OTHER RECORD'*/
        ,concat('OTH-', OTH.uk) SOURCE_PK
        ,concat('OTH-', OTH.uk) AS PROCEDURE_OCCURRENCE_ID
        ,PAT.uk AS PERSON_ID
        ,'730@'  + OTH.F05069 AS PROCEDURE_CONCEPT_ID
		,case
		     when OTH.F05195 = '99999999'
		     then ''
		     when right(OTH.F05195, 4) = '9999'
		     then FORMAT(TRY_CAST(left(OTH.F05195,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss')
		     when right(OTH.F05195,2) = '99'
             then FORMAT(TRY_CAST(left(OTH.F05195,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss')
		     else ISNULL(FORMAT(TRY_CAST(OTH.F05195 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		  END AS PROCEDURE_DATE
		  ,case
		     when OTH.F05195 = '99999999'
		     then ''
		     when right(OTH.F05195, 4) = '9999'
		     then FORMAT(TRY_CAST(left(OTH.F05195,4) + '0101' AS DATETIME), 'yyyy-MM-dd HH:mm:ss')
		     when right(OTH.F05195,2) = '99'
             then FORMAT(TRY_CAST(left(OTH.F05195,6) + '01' AS DATETIME), 'yyyy-MM-dd HH:mm:ss')
		     else ISNULL(FORMAT(TRY_CAST(OTH.F05195 AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		  END AS PROCEDURE_DATETIME        
    	,'1791@32' AS PROCEDURE_TYPE_CONCEPT_ID
	    ,0 AS MODIFIER_CONCEPT_ID
		,1 AS QUANTITY
    	,ISNULL(OTH.F05160, '') AS PROVIDER_ID                                      /*2460*/
        ,rsSource.uk AS VISIT_OCCURRENCE_ID
        ,concat('OTH-', OTH.uk) AS VISIT_DETAIL_ID
        ,OTH.F05069 AS PROCEDURE_SOURCE_VALUE   --nulls handled by predicate        /*740*/
	    ,0 AS PROCEDURE_SOURCE_CONCEPT_ID
	    ,0 AS MODIFIER_SOURCE_VALUE
		,ISNULL(HSP.F00006, '') AS MRN
		,ISNULL(STUFF(rsSource.F00152,4,0,'.'), '') AS CONDITION_CONCEPT_ID_SITE
        ,CASE 
			WHEN format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') is NULL
            then format(GETDATE(), 'yyyy-MM-dd HH:mm:ss')  
	        else format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss')
		 end AS modified_dtTm
   FROM UNM_CNExTCases.dbo.Tumor rsSource
   JOIN UNM_CNExTCases.dbo.Other OTH ON OTH.fk2 = rsSource.uk
   JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.uk = rsSource.fk1
   JOIN UNM_CNExTCases.dbo.Hospital HSP ON HSP.fk2 = rsSource.uk
   JOIN UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK = HExt.UK
  WHERE OTH.F05069 in ( '1','2','3','6')
    and OTH.F05195 is not null and OTH.F05195 != '' and OTH.F05195 not in ('00000000','88888888')
    and HSP.F00006 not in (999999998, 9999998, 999999, 9999)
    and HSP.F00006 >= 1000
	and HExt.F00084 >= @fromDate
UNION
SELECT 'CNEXT CHEMO(OMOP_PROCEDURE_OCCURRENCE)' AS IDENTITY_CONTEXT
        ,concat('CHM-', CHM.uk) AS SOURCE_PK
        ,concat('CHM-', CHM.uk) AS PROCEDURE_OCCURRENCE_ID
    	,PAT.uk AS PERSON_ID
    	,'700@' + CHM.F05037 AS PROCEDURE_CONCEPT_ID
	    ,case
		     when CHM.F05189 = '99999999'
		     then ''
		     when right(CHM.F05189, 4) = '9999'
		     then FORMAT(TRY_CAST(left(CHM.F05189,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss')
		     when right(CHM.F05189,2) = '99'
             then FORMAT(TRY_CAST(left(CHM.F05189,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss')
		     else ISNULL(FORMAT(TRY_CAST(CHM.F05189 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		  END AS PROCEDURE_DATE
		  ,case
		     when CHM.F05189 = '99999999'
		     then ''
		     when right(CHM.F05189, 4) = '9999'
		     then FORMAT(TRY_CAST(left(CHM.F05189,4) + '0101' AS DATETIME), 'yyyy-MM-dd HH:mm:ss')
		     when right(CHM.F05189,2) = '99'
             then FORMAT(TRY_CAST(left(CHM.F05189,6) + '01' AS DATETIME), 'yyyy-MM-dd HH:mm:ss')
		     else ISNULL(FORMAT(TRY_CAST(CHM.F05189 AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		  END AS PROCEDURE_DATETIME
        ,'1791@32' AS PROCEDURE_TYPE_CONCEPT_ID
		,0 AS MODIFIER_CONCEPT_ID
		,1 AS QUANTITY
    	,ISNULL(CHM.F05157, '') AS PROVIDER_ID 
		,rsSource.uk AS VISIT_OCCURRENCE_ID
        ,concat('CHM-', CHM.uk) AS VISIT_DETAIL_ID		
        ,CHM.F05037 AS PROCEDURE_SOURCE_VALUE
        ,0 AS PROCEDURE_SOURCE_CONCEPT_ID
	    ,0 AS MODIFIER_SOURCE_VALUE
	    ,ISNULL(HSP.F00006, '') AS MRN
		,ISNULL(STUFF(rsSource.F00152,4,0,'.'), '') AS CONDITION_CONCEPT_ID_SITE
		,CASE 
			WHEN format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') is NULL
            then format(GETDATE(), 'yyyy-MM-dd HH:mm:ss')  
	        else format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss')
		 end AS modified_dtTm
   FROM UNM_CNExTCases.dbo.Tumor rsSource
   JOIN UNM_CNExTCases.dbo.Chemo CHM ON CHM.fk2 = rsSource.uk
   JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.uk = rsSource.fk1
   JOIN UNM_CNExTCases.dbo.Hospital HSP ON rsSource.uk = HSP.fk2
   JOIN UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK = HExt.UK
WHERE CHM.F05037 IN ('01', '02', '03')
    AND CHM.F05669 > '00'
	and ((CHM.F05189 is not null and CHM.F05189 != '' and CHM.F05189 not in ('00000000','19000000','05  0532','07  0142','07  0252','07  0352','88888888'))
	 or (CHM.F05214 is not null and CHM.F05214 != '' and CHM.F05214 not in ('00000000','88888888')))
	and HSP.F00006 not in (999999998, 9999998, 999999, 9999)
    and HSP.F00006 >= 1000
	and HExt.F00084 >= @fromDate
UNION
SELECT 'CNEXT HORMONE(OMOP_PROCEDURE_OCCURRENCE)' AS IDENTITY_CONTEXT
        ,concat('HOR-', HOR.uk) AS SOURCE_PK
        ,concat('HOR-', HOR.uk) AS PROCEDURE_OCCURRENCE_ID
    	,PAT.uk AS PERSON_ID
    	,'710@' + HOR.F05063 AS PROCEDURE_CONCEPT_ID
	    ,case
		     when HOR.F05191 = '99999999'
		     then ''
		     when right(HOR.F05191, 4) = '9999'
		     then FORMAT(TRY_CAST(left(HOR.F05191,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss')
		     when right(HOR.F05191,2) = '99'
             then FORMAT(TRY_CAST(left(HOR.F05191,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss')
		     else ISNULL(FORMAT(TRY_CAST(HOR.F05191 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		  END AS PROCEDURE_DATE
		  ,case
		     when HOR.F05191 = '99999999'
		     then ''
		     when right(HOR.F05191, 4) = '9999'
		     then FORMAT(TRY_CAST(left(HOR.F05191,4) + '0101' AS DATETIME), 'yyyy-MM-dd HH:mm:ss')
		     when right(HOR.F05191,2) = '99'
             then FORMAT(TRY_CAST(left(HOR.F05191,6) + '01' AS DATETIME), 'yyyy-MM-dd HH:mm:ss')
		     else ISNULL(FORMAT(TRY_CAST(HOR.F05191 AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		  END AS PROCEDURE_DATETIME
    	,'1791@32' AS PROCEDURE_TYPE_CONCEPT_ID
		,0 AS MODIFIER_CONCEPT_ID
		,1 AS QUANTITY
    	,ISNULL(HOR.F05158, '') AS PROVIDER_ID
		,rsSource.uk AS VISIT_OCCURRENCE_ID
	    ,concat('HOR-', HOR.uk) AS VISIT_DETAIL_ID		
        ,HOR.F05063 AS PROCEDURE_SOURCE_VALUE
        ,0 AS PROCEDURE_SOURCE_CONCEPT_ID
	    ,0 AS MODIFIER_SOURCE_VALUE
		,ISNULL(HSP.F00006, '') AS MRN
		,ISNULL(STUFF(rsSource.F00152,4,0,'.'), '') AS CONDITION_CONCEPT_ID_SITE
		,CASE 
			WHEN format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') is NULL
            then format(GETDATE(), 'yyyy-MM-dd HH:mm:ss')  
	        else format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss')
		 end AS modified_dtTm
   FROM UNM_CNExTCases.dbo.Tumor rsSource
   JOIN UNM_CNExTCases.dbo.Hormone HOR ON HOR.fk2 = rsSource.uk
   JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.uk = rsSource.fk1
   JOIN UNM_CNExTCases.dbo.Hospital HSP ON rsSource.uk = HSP.fk2   
   JOIN UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK = HExt.UK
WHERE HOR.F05063 = '01'
  and ((HOR.F05191 is not null and HOR.F05191 != '' and HOR.F05191 not in ('00000000','88888888'))
   or (HOR.F05216 is not null and HOR.F05216 != '' and HOR.F05216 not in('00000000','88888888')))
  and HSP.F00006 not in (999999998, 9999998, 999999, 9999)
  and HSP.F00006 >= 1000
  and HExt.F00084 >= @fromDate
UNION
SELECT 'CNEXT IMMUNO(OMOP_PROCEDURE_OCCURRENCE)' AS IDENTITY_CONTEXT
        ,concat('BRM-', BRM.uk) AS SOURCE_PK
        ,concat('BRM-', BRM.uk) AS PROCEDURE_OCCURRENCE_ID
    	,PAT.uk AS PERSON_ID
		,'720@' + BRM.F05066  AS PROCEDURE_CONCEPT_ID
	    ,case
		     when BRM.F05193 = '99999999'
		     then ''
		     when right(BRM.F05193, 4) = '9999'
		     then FORMAT(TRY_CAST(left(BRM.F05193,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss')
		     when right(BRM.F05193,2) = '99'
             then FORMAT(TRY_CAST(left(BRM.F05193,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss')
		     else ISNULL(FORMAT(TRY_CAST(BRM.F05193 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		  END AS PROCEDURE_DATE
		  ,case
		     when BRM.F05193 = '99999999'
		     then ''
		     when right(BRM.F05193, 4) = '9999'
		     then FORMAT(TRY_CAST(left(BRM.F05193,4) + '0101' AS DATETIME), 'yyyy-MM-dd HH:mm:ss')
		     when right(BRM.F05193,2) = '99'
             then FORMAT(TRY_CAST(left(BRM.F05193,6) + '01' AS DATETIME), 'yyyy-MM-dd HH:mm:ss')
		     else ISNULL(FORMAT(TRY_CAST(BRM.F05193 AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		  END AS PROCEDURE_DATETIME
    	,'1791@32' AS PROCEDURE_TYPE_CONCEPT_ID
		,0 AS MODIFIER_CONCEPT_ID
		,1 AS QUANTITY
    	,ISNULL(BRM.F05159, '') AS PROVIDER_ID
		,rsSource.uk AS VISIT_OCCURRENCE_ID
	    ,concat('BRM-', BRM.uk) AS VISIT_DETAIL_ID		
    	,BRM.F05066 AS PROCEDURE_SOURCE_VALUE
        ,0 AS PROCEDURE_SOURCE_CONCEPT_ID
	    ,0 AS MODIFIER_SOURCE_VALUE	
	    ,ISNULL(HSP.F00006, '') AS MRN
		,ISNULL(STUFF(rsSource.F00152,4,0,'.'), '') AS CONDITION_CONCEPT_ID_SITE
		,CASE 
			WHEN format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') is NULL
            then format(GETDATE(), 'yyyy-MM-dd HH:mm:ss')  
	        else format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss')
		 end AS modified_dtTm
   FROM UNM_CNExTCases.dbo.Tumor rsSource
   JOIN UNM_CNExTCases.dbo.Immuno BRM ON BRM.fk2 = rsSource.uk
   JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.uk = rsSource.fk1
   JOIN UNM_CNExTCases.dbo.Hospital HSP ON rsSource.uk = HSP.fk2
   JOIN UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK = HExt.UK
WHERE BRM.F05066 = '01'
  and ((BRM.F05193 is not null and BRM.F05193 != '' and BRM.F05193 not in('00000000','88888888'))
   or (BRM.F05218 is not null and BRM.F05218 != '' and BRM.F05218 not in('00000000','88888888')))
  and HSP.F00006 not in (999999998, 9999998, 999999, 9999)
  and HSP.F00006 >= 1000
  and HExt.F00084 >= @fromDate
UNION
SELECT 'CNEXT HOSP(OMOP_PROCEDURE_OCCURRENCE)' AS IDENTITY_CONTEXT
        ,concat('HSP-', HSP.uk) AS SOURCE_PK
        ,concat('HSP-', HSP.uk) AS PROCEDURE_OCCURRENCE_ID
    	,PAT.uk AS PERSON_ID
		,ISNULL(HSG.F05522, '') AS PROCEDURE_CONCEPT_ID
	    ,case
		    when HExt.F00427 = '99999999'
		    then ''
		    when right(HExt.F00427,4) = '9999'
		    then ISNULL(FORMAT(TRY_CAST(left(HExt.F00427,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '') 
		    when right(HExt.F00427,2) = '99'
		    then ISNULL(FORMAT(TRY_CAST(left(HExt.F00427,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		    else ISNULL(FORMAT(TRY_CAST(HExt.F00427 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
	      end AS PROCEDURE_DATE
		,case
		    when HExt.F00427 = '99999999'
		    then ''
		    when right(HExt.F00427,4) = '9999'
		    then ISNULL(FORMAT(TRY_CAST(left(HExt.F00427,4) + '0101' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '') 
		    when right(HExt.F00427,2) = '99'
		    then ISNULL(FORMAT(TRY_CAST(left(HExt.F00427,6) + '01' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		    else ISNULL(FORMAT(TRY_CAST(HExt.F00427 AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
	     end ASPROCEDURE_DATETIME 
        ,'1791@32' AS PROCEDURE_TYPE_CONCEPT_ID
		,0 AS MODIFIER_CONCEPT_ID
		,1 AS QUANTITY
    	,ISNULL(HExt.F00675, '') AS PROVIDER_ID
		,rsSource.uk AS VISIT_OCCURRENCE_ID
	    ,concat('HSP-', HSP.uk) AS VISIT_DETAIL_ID		
    	,ISNULL(HExt.F03564, '') AS PROCEDURE_SOURCE_VALUE
        ,0 AS PROCEDURE_SOURCE_CONCEPT_ID
	    ,0 AS MODIFIER_SOURCE_VALUE	
	    ,ISNULL(HSP.F00006, '') AS MRN
		,ISNULL(STUFF(rsSource.F00152,4,0,'.'), '') AS CONDITION_CONCEPT_ID_SITE
		,CASE 
			WHEN format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') is NULL
            then format(GETDATE(), 'yyyy-MM-dd HH:mm:ss')  
	        else format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss')
		 end AS modified_dtTm
   FROM UNM_CNExTCases.dbo.Tumor rsSource
   JOIN UNM_CNExTCases.dbo.Hospital HSP ON rsSource.uk = HSP.fk2
   JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.uk = rsSource.fk1
   JOIN UNM_CNExTCases.dbo.HospSupp HSG ON HSG.UK = HSP.UK
   JOIN UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK = HExt.UK
  WHERE ((HExt.F00427 is not null and HExt.F00427 != '' and HExt.F00427 not in ('00000000','88888888'))
     or (HExt.F00128 is not null and HExt.F00128 != '' and HExt.F00128 not in('00000000','88888888')))
    and HSP.F00006 not in (999999998, 9999998, 999999, 9999)
    and HSP.F00006 >= 1000
	and HExt.F00084 >= @fromDate