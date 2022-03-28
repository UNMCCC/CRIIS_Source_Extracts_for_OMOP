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
LTV - 2/4/2022 - handled NULL values with the ISNULL function. Note: there are some provider_id's that contain all 9's. I don't know if
                 this is equivilant to an unknown value and whether or not it should be converted to a null.
LTV- 2/22/2022 - removed the 'AND CHM.F04755 IS NOT NULL'  conditon on the Chemo table per Mark.

*/
SET NOCOUNT ON;
SELECT 'IDENTITY_CONTEXT|SOURCE_PK|DRUG_EXPOSURE_ID|PERSON_ID|DRUG_CONCEPT_ID|DRUG_EXPOSURE_START_DATE|DRUG_EXPOSURE_START_DATETIME|DRUG_EXPOSURE_END_DATE|DRUG_EXPOSURE_END_DATETIME|VERBATIM_END_DATE|DRUG_TYPE_CONCEPT_ID|STOP_REASON|REFILLS|QUANTITY|DAYS_SUPPLY|SIG|ROUTE_CONCEPT_ID|LOT_NUMBER|PROVIDER_ID|VISIT_OCCURRENCE_ID|VISIT_DETAIL_ID|DRUG_SOURCE_VALUE|DRUG_SOURCE_CONCEPT_ID|ROUTE_SOURCE_VALUE|DOSE_UNIT_SOURCE_VALUE|MRN|Modified_DtTm';
SELECT  'CNEXT CHEMO(OMOP_DRUG_EXPOSURE)' AS IDENTITY_CONTEXT
       ,CHM.uk AS SOURCE_PK
       ,CHM.UK AS DRUG_EXPOSURE_ID
       ,PAT.UK AS PERSON_ID  /*20*/
       ,CHM.F05037 AS DRUG_CONCEPT_ID
	   ,case
		     when CHM.F05189 = '99999999'
			 then ''
			 when right(CHM.F05189,4) = '9999'
			 then ISNULL(FORMAT(TRY_CAST(left(CHM.F05189,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '') 
			 when right(CHM.F05189,2) = '99'
			 then ISNULL(FORMAT(TRY_CAST(left(CHM.F05189,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
			 else ISNULL(FORMAT(TRY_CAST(CHM.F05189 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		 end AS DRUG_EXPOSURE_START_DATE
		,case
		     when CHM.F05189 = '99999999'
			 then ''
			 when right(CHM.F05189,4) = '9999'
			 then ISNULL(FORMAT(TRY_CAST(left(CHM.F05189,4) + '0101' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '') 
			 when right(CHM.F05189,2) = '99'
			 then ISNULL(FORMAT(TRY_CAST(left(CHM.F05189,6) + '01' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
			 else ISNULL(FORMAT(TRY_CAST(CHM.F05189 AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		 end AS DRUG_EXPOSURE_START_DATETIME
		,case 
		      when CHM.F05214 = '99999999'
			  then ''
		      when right(CHM.F05214, 4) = '9999'
		      then ISNULL(FORMAT(TRY_CAST(left(CHM.F05214,4) + '0101' AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
			  when right(CHM.F05214, 2) = '99'
			  then ISNULL(FORMAT(TRY_CAST(left(CHM.F05214,6) + '01' AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
			  else ISNULL(FORMAT(TRY_CAST(CHM.F05214 AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
		 end AS DRUG_EXPOSURE_END_DATE
		,case 
		      when CHM.F05214 = '99999999'
			  then ''
		      when right(CHM.F05214, 4) = '9999'
		      then ISNULL(FORMAT(TRY_CAST(left(CHM.F05214,4) + '0101' AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
			  when right(CHM.F05214, 2) = '99'
			  then ISNULL(FORMAT(TRY_CAST(left(CHM.F05214,6) + '01' AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
			  else ISNULL(FORMAT(TRY_CAST(CHM.F05214 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
		 end AS DRUG_EXPOSURE_END_DATETIME
	    ,case 
		      when CHM.F05214 = '99999999'
			  then ''
		      when right(CHM.F05214, 4) = '9999'
		      then ISNULL(FORMAT(TRY_CAST(left(CHM.F05214,4) + '0101' AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
			  when right(CHM.F05214, 2) = '99'
			  then ISNULL(FORMAT(TRY_CAST(left(CHM.F05214,6) + '01' AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
			  else ISNULL(FORMAT(TRY_CAST(CHM.F05214 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
		 end AS VERBATIM_END_DATE
	   ,'EHR dispensing record' AS DRUG_TYPE_CONCEPT_ID
	   ,''  AS STOP_REASON
	   ,''  AS REFILLS
       ,ISNULL(CHM.F04760, '') AS QUANTITY
	   ,''  AS DAYS_SUPPLY
	   ,''  AS SIG
       ,''  AS ROUTE_CONCEPT_ID
	   ,''  AS LOT_NUMBER
       ,ISNULL(CHM.F05157, '') AS PROVIDER_ID                                          /*2460*/
       ,rsSource.uk AS VISIT_OCCURRENCE_ID
       ,CHM.UK AS VISIT_DETAIL_ID
	   ,0 AS DRUG_SOURCE_VALUE
	   ,'700@' + CHM.F05037 AS DRUG_SOURCE_CONCEPT_ID
       ,0 AS ROUTE_SOURCE_VALUE
       ,0 AS DOSE_UNIT_SOURCE_VALUE
	   ,ISNULL(HSP.F00006, '') AS MRN
       ,CASE WHEN format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') is NULL
       then FORmat(GETDATE(), 'yyyy-MM-dd HH:mm:ss')  
       else format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') end
       AS modified_dtTm
  FROM UNM_CNExTCases.dbo.Tumor rsSource
  JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.uk = rsSource.fk1
  JOIN UNM_CNExTCases.dbo.Chemo CHM on CHM.fk2 = rsSource.UK
  JOIN UNM_CNExTCases.dbo.Hospital HSP ON HSP.fk2 = rsSource.UK
  INNER JOIN  UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK = HExt.UK
 WHERE CHM.F05037 IN ('01', '02', '03')
   AND CHM.F05669 > '00'
   and ((CHM.F05189 is not null and CHM.F05189 != '' and CHM.F05189 not in ('00000000','19000000','05  0532','07  0142','07  0252','07  0352','88888888','99999999'))
	or (CHM.F05214 is not null and CHM.F05214 != '' and CHM.F05214 not in ('00000000','88888888','99999999')))
   and HSP.F00006 not in (999999998, 9999998,999999, 9999)
   and HSP.F00006 >= 1000
 UNION
 SELECT  'CNEXT HORMONE(OMOP_DRUG_EXPOSURE)' AS IDENTITY_CONTEXT
       ,HOR.uk AS SOURCE_PK
       ,HOR.UK AS DRUG_EXPOSURE_ID
       ,PAT.UK AS PERSON_ID  /*20*/
       ,HOR.F05063 AS DRUG_CONCEPT_ID	   ,case
		     when HOR.F05191 = '99999999'
			 then ''
			 when right(HOR.F05191,4) = '9999'
			 then ISNULL(FORMAT(TRY_CAST(left(HOR.F05191,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '') 
			 when right(HOR.F05191,2) = '99'
			 then ISNULL(FORMAT(TRY_CAST(left(HOR.F05191,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
			 else ISNULL(FORMAT(TRY_CAST(HOR.F05191 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		 end AS DRUG_EXPOSURE_START_DATE
		,case
		     when HOR.F05191 = '99999999'
			 then ''
			 when right(HOR.F05191,4) = '9999'
			 then ISNULL(FORMAT(TRY_CAST(left(HOR.F05191,4) + '0101' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '') 
			 when right(HOR.F05191,2) = '99'
			 then ISNULL(FORMAT(TRY_CAST(left(HOR.F05191,6) + '01' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
			 else ISNULL(FORMAT(TRY_CAST(HOR.F05191 AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		 end AS DRUG_EXPOSURE_START_DATETIME
		,case 
		      when HOR.F05216 = '99999999'
			  then ''
		      when right(HOR.F05216, 4) = '9999'
		      then ISNULL(FORMAT(TRY_CAST(left(HOR.F05216,4) + '0101' AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
			  when right(HOR.F05216, 2) = '99'
			  then ISNULL(FORMAT(TRY_CAST(left(HOR.F05216,6) + '01' AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
			  else ISNULL(FORMAT(TRY_CAST(HOR.F05216 AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
		 end AS DRUG_EXPOSURE_END_DATE
		,case 
		      when HOR.F05216 = '99999999'
			  then ''
		      when right(HOR.F05216, 4) = '9999'
		      then ISNULL(FORMAT(TRY_CAST(left(HOR.F05216,4) + '0101' AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
			  when right(HOR.F05216, 2) = '99'
			  then ISNULL(FORMAT(TRY_CAST(left(HOR.F05216,6) + '01' AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
			  else ISNULL(FORMAT(TRY_CAST(HOR.F05216 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
		 end AS DRUG_EXPOSURE_END_DATETIME
	   ,case 
		      when HOR.F05216 = '99999999'
			  then ''
		      when right(HOR.F05216, 4) = '9999'
		      then ISNULL(FORMAT(TRY_CAST(left(HOR.F05216,4) + '0101' AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
			  when right(HOR.F05216, 2) = '99'
			  then ISNULL(FORMAT(TRY_CAST(left(HOR.F05216,6) + '01' AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
			  else ISNULL(FORMAT(TRY_CAST(HOR.F05216 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
		 end AS VERBATIM_END_DATE
	   ,'EHR dispensing record' AS DRUG_TYPE_CONCEPT_ID
	   ,''  AS STOP_REASON
	   ,''  AS REFILLS
       ,ISNULL(HOR.F05670, '') AS QUANTITY
	   ,''  AS DAYS_SUPPLY
	   ,''  AS SIG
       ,''  AS ROUTE_CONCEPT_ID
	   ,''  AS LOT_NUMBER
       ,ISNULL(HOR.F05158, '') AS PROVIDER_ID                                          /*2460*/
       ,rsSource.uk AS VISIT_OCCURRENCE_ID
       ,HOR.UK AS VISIT_DETAIL_ID
	   ,0 AS DRUG_SOURCE_VALUE
	   ,'710@' + HOR.F05063 AS DRUG_SOURCE_CONCEPT_ID
       ,0 AS ROUTE_SOURCE_VALUE
       ,0 AS DOSE_UNIT_SOURCE_VALUE
	   ,ISNULL(HSP.F00006, '') AS MRN
       ,CASE WHEN format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') is NULL
       then FORmat(GETDATE(), 'yyyy-MM-dd HH:mm:ss')  
       else format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') end
       AS modified_dtTm
   FROM UNM_CNExTCases.dbo.Tumor rsSource
  JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.uk = rsSource.fk1
  JOIN UNM_CNExTCases.dbo.Hormone HOR on HOR.fk2 = rsSource.UK
  JOIN UNM_CNExTCases.dbo.Hospital HSP ON HSP.fk2 = rsSource.UK
  INNER JOIN  UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK = HExt.UK
 WHERE HOR.F05063 = '01'
    and ((HOR.F05191 is not null and HOR.F05191 != '' and HOR.F05191 not in ('00000000','88888888','99999999'))
     or (HOR.F05216 is not null and HOR.F05216 != '' and HOR.F05216 not in('00000000','88888888','99999999')))
	and HSP.F00006 not in (999999998, 9999998,999999, 9999)
    and HSP.F00006 >= 1000
UNION
SELECT  'CNEXT IMMUNO(OMOP_DRUG_EXPOSURE)' AS IDENTITY_CONTEXT
       ,BRM.uk AS SOURCE_PK
       ,BRM.uk AS DRUG_EXPOSURE_ID
       ,PAT.UK AS PERSON_ID  /*20*/
       ,BRM.F05066 AS DRUG_CONCEPT_ID
	   ,case
		     when BRM.F05193 = '99999999'
			 then ''
			 when right(BRM.F05193,4) = '9999'
			 then ISNULL(FORMAT(TRY_CAST(left(BRM.F05193,4) + '0101' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '') 
			 when right(BRM.F05193,2) = '99'
			 then ISNULL(FORMAT(TRY_CAST(left(BRM.F05193,6) + '01' AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
			 else ISNULL(FORMAT(TRY_CAST(BRM.F05193 AS DATE), 'yyyy-MM-dd HH:mm:ss'), '')
		 end AS DRUG_EXPOSURE_START_DATE
		,case
		     when BRM.F05193 = '99999999'
			 then ''
			 when right(BRM.F05193,4) = '9999'
			 then ISNULL(FORMAT(TRY_CAST(left(BRM.F05193,4) + '0101' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '') 
			 when right(BRM.F05193,2) = '99'
			 then ISNULL(FORMAT(TRY_CAST(left(BRM.F05193,6) + '01' AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
			 else ISNULL(FORMAT(TRY_CAST(BRM.F05193 AS DATETIME), 'yyyy-MM-dd HH:mm:ss'), '')
		 end AS DRUG_EXPOSURE_START_DATETIME
		,case 
		      when BRM.F05218 = '99999999'
			  then ''
		      when right(BRM.F05218, 4) = '9999'
		      then ISNULL(FORMAT(TRY_CAST(left(BRM.F05218,4) + '0101' AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
			  when right(BRM.F05218, 2) = '99'
			  then ISNULL(FORMAT(TRY_CAST(left(BRM.F05218,6) + '01' AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
			  else ISNULL(FORMAT(TRY_CAST(BRM.F05218 AS DATE),'yyyy-MM-dd HH:mm:ss'), '')
		 end AS DRUG_EXPOSURE_END_DATE
		,case 
		      when BRM.F05218 = '99999999'
			  then ''
		      when right(BRM.F05218, 4) = '9999'
		      then ISNULL(FORMAT(TRY_CAST(left(BRM.F05218,4) + '0101' AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
			  when right(BRM.F05218, 2) = '99'
			  then ISNULL(FORMAT(TRY_CAST(left(BRM.F05218,6) + '01' AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
			  else ISNULL(FORMAT(TRY_CAST(BRM.F05218 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
		 end AS DRUG_EXPOSURE_END_DATETIME
       ,case 
		      when BRM.F05218 = '99999999'
			  then ''
		      when right(BRM.F05218, 4) = '9999'
		      then ISNULL(FORMAT(TRY_CAST(left(BRM.F05218,4) + '0101' AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
			  when right(BRM.F05218, 2) = '99'
			  then ISNULL(FORMAT(TRY_CAST(left(BRM.F05218,6) + '01' AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
			  else ISNULL(FORMAT(TRY_CAST(BRM.F05218 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '')
		 end AS VERBATIM_END_DATE
	   ,'EHR dispensing record' AS DRUG_TYPE_CONCEPT_ID
	   ,''  AS STOP_REASON
	   ,''  AS REFILLS
       ,ISNULL(BRM.F05672, '') AS QUANTITY
	   ,''  AS DAYS_SUPPLY
	   ,''  AS SIG
       ,''  AS ROUTE_CONCEPT_ID
	   ,''  AS LOT_NUMBER
       ,ISNULL(BRM.F05159, '') AS PROVIDER_ID                                          /*2460*/
       ,rsSource.uk AS VISIT_OCCURRENCE_ID
       ,BRM.UK AS VISIT_DETAIL_ID
	   ,0 AS DRUG_SOURCE_VALUE
	   ,'720@' + BRM.F05066 AS DRUG_SOURCE_CONCEPT_ID
       ,0 AS ROUTE_SOURCE_VALUE
       ,0 AS DOSE_UNIT_SOURCE_VALUE
	   ,ISNULL(HSP.F00006, '') AS MRN
       ,CASE WHEN format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') is NULL
       then FORmat(GETDATE(), 'yyyy-MM-dd HH:mm:ss')  
       else format(TRY_CAST(HExt.F00084 as datetime),'yyyy-MM-dd HH:mm:ss') end
       AS modified_dtTm
  FROM UNM_CNExTCases.dbo.Tumor rsSource
  JOIN UNM_CNExTCases.dbo.Patient PAT on PAT.uk = rsSource.fk1
  JOIN UNM_CNExTCases.dbo.Immuno BRM ON BRM.fk2 = rsSource.uk
  JOIN UNM_CNExTCases.dbo.Hospital HSP ON HSP.fk2 = rsSource.UK
  INNER JOIN  UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK = HExt.UK
  WHERE BRM.F05066 = '01'
    and ((BRM.F05193 is not null and BRM.F05193 != '' and BRM.F05193 not in('00000000','88888888','99999999'))
     or (BRM.F05218 is not null and BRM.F05218 != '' and BRM.F05218 not in('00000000','88888888','99999999')))
	and HSP.F00006 not in (999999998, 9999998,999999, 9999)
    and HSP. F00006 >= 1000
ORDER BY 2 DESC