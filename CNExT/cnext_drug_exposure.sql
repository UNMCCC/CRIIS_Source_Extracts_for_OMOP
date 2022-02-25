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
       ,CHM.uk AS DRUG_EXPOSURE_ID
       ,(SELECT TOP 1 ISNULL(rsTarget.F00004, '') FROM UNM_CNExTCases.dbo.PatExtended rsTarget WHERE rsTarget.uk =  rsSource.fk1 Order By rsTarget.UK ASC) AS PERSON_ID  /*20*/
       ,CHM.F05037 AS DRUG_CONCEPT_ID  --nulls handled by predicate                                                  /*700*/
       ,ISNULL(FORMAT(TRY_CAST(CHM.F05189 AS DATE), 'yyyy-MM-dd'), '') AS DRUG_EXPOSURE_START_DATE                   /*1220*/
       ,ISNULL(FORMAT(TRY_CAST(CHM.F05189 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '') AS DRUG_EXPOSURE_START_DATETIME   /*1220*/
       ,ISNULL(FORMAT(TRY_CAST(CHM.F05214 AS DATE), 'yyyy-MM-dd'), '') AS DRUG_EXPOSURE_END_DATE                     /*1220*/
       ,ISNULL(FORMAT(TRY_CAST(CHM.F05214 AS DATETIME),'yyyy-MM-dd HH:mm:ss'), '') AS DRUG_EXPOSURE_END_DATETIME     /*1220*/
       ,ISNULL(FORMAT(TRY_CAST(CHM.F05214 AS DATE), 'yyyy-MM-dd'), '') AS VERBATIM_END_DATE                          /*3220*/
	   ,'EHR dispensing record' AS DRUG_TYPE_CONCEPT_ID
	   ,''  AS STOP_REASON
	   ,''  AS REFILLS
       ,ISNULL(CHM.F04760, '') AS QUANTITY
	   ,''  AS DAYS_SUPPLY
	   ,''  AS SIG
       ,''  AS ROUTE_CONCEPT_ID
	   ,''  AS LOT_NUMBER
       ,ISNULL(CHM.F05157, '') AS PROVIDER_ID                                          /*2460*/
       ,(SELECT TOP 1 rsTarget.UK FROM UNM_CNExTCases.dbo.Patient rsTarget WHERE rsTarget.uk =  rsSource.fk1 Order By rsTarget.UK ASC) AS VISIT_OCCURRENCE_ID
       ,rsSource.uk AS VISIT_DETAIL_ID
	   ,0 AS DRUG_SOURCE_VALUE
	   ,'700@' + CHM.F05037 AS DRUG_SOURCE_CONCEPT_ID  --nulls handled by predicate 
       ,0 AS ROUTE_SOURCE_VALUE
       ,0 AS DOSE_UNIT_SOURCE_VALUE
	   ,ISNULL(HSP.F00006, '') AS MRN
	   ,isNULL(format(TRY_CAST(HExt.F00084 as datetime),'yyyy-mm-dd HH:mm:ss'),'')  AS modified_dtTm
      -- ,(SELECT TOP 1 rsTarget.F00016 FROM UNM_CNExTCases.dbo.Hospital rsTarget WHERE rsTarget.fk2 = rsSource.UK  Order By  rsTarget.fk2 ASC) AS ACCESSION_NUMBER  /*550*/
      -- ,(SELECT TOP 1 rsTarget.F00006 FROM UNM_CNExTCases.dbo.Hospital rsTarget WHERE rsTarget.fk2 = rsSource.UK  Order By  rsTarget.fk2 ASC) AS MRN  
  FROM UNM_CNExTCases.dbo.Tumor rsSource
  JOIN UNM_CNExTCases.dbo.Chemo CHM on CHM.fk2 = rsSource.UK
  JOIN UNM_CNExTCases.dbo.Hospital HSP ON HSP.fk2 = rsSource.UK
  INNER JOIN  UNM_CNExTCases.dbo.HospExtended HExt on HSP.UK=HExt.UK
 WHERE CHM.F05037 IN ('01', '02', '03')
   AND CHM.F05669 > '00'
 ORDER BY 2 DESC