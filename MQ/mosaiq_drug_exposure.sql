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
*/
/* DAH NOTES
12/2/2021
1) Source: MosaiqAdmin.dbo.Ref_Patient_Drugs_Administered created by stored procedure MosaiqAdmin.dbo.sp_Ref_Patient_Drugs_Administered
	Data is extracted from Mosaiq tables dbo.PharmAdm (RXA), dbo.Drug (DRG), dbo.Orders (ORC)
	Also use MosaiqAdmin.dbo.Ref_CPTs_and_Activities 
2) Research
	Check with infusion nurses to clarify drugs with administerd amount (quantity) of zero
	Research:  Not all appts are statused as captured even though a drug was administered 
3)  Need RS21 help in mapping fields

12/16/21 -- Added mapping to Values for 2 standard vocabularies: RxNorm and CVX
01/10/22 -- added RxNorm_CodeType and CVX_CodeType

1/10/2022 -- added modified_dtTm  for incremental add
1/10/2022 -- using concatenation of Appt date and Mosaiq Patient ID as visit occurrence identifer
Addressed NULLS 01/12/2022

EXECUTION CHECK SUCESSFUL 01/12/2022
*/
SET NOCOUNT ON;
DECLARE @IncDate VARCHAR(8);
SET @IncDate = CONVERT(VARCHAR(8),DateAdd(month, -2, GETDATE()),112);
DECLARE @AllDates VARCHAR(8);
SET @AllDates = '20100101';
DECLARE @fromDate VARCHAR(8);
SET @fromDate = 
   CASE $(isInc)
     WHEN 'Y' THEN  @IncDate
     WHEN 'N' THEN  @AllDates
   END
   
SELECT 'IDENTITY_CONTEXT|SOURCE_PK|DRUG_EXPOSURE_ID|PERSON_ID|DRUG_CONCEPT_ID|DRUG_EXPOSURE_START_DATE|DRUG_EXPOSURE_START_DATETIME|DRUG_EXPOSURE_END_DATE|DRUG_EXPOSURE_END_DATETIME|VERBATIM_END_DATETIME|DRUG_TYPE_CONCEPT_ID|STOP_REASON|REFILLS|QUANTITY|DAYS_SUPPLY|SIG|ROUTE_CONCEPT_ID|LOT_NUMBER|PROVIDER_ID|VISIT_OCCURRENCE_ID|VISIT_DETAIL_ID|DRUG_SOURCE_VALUE|DRUG_SOURCE_CONCEPT_ID|ROUTE_SOURCE_VALUE|DOSE_UNIT_SOURCE_VALUE|Adm_units|drug_label|drug_generic_name|drug_type|RxNorm_CodeValue|RxNorm_CodeType|CVX_CodeValue|CVX_CodeType|modified_DtTm';
SELECT DISTINCT 'Mosaiq Ref_Patient_Drugs_Administered(OMOP_DRUG_EXPOSURE)' AS IDENTITY_CONTEXT
    ,rsource.RXA_SET_ID						AS SOURCE_PK
    ,rsource.RXA_SET_ID						AS DRUG_EXPOSURE_ID
    ,rsource.Pat_ID1						AS PERSON_ID
	,''										AS DRUG_CONCEPT_ID
    ,isNULL(FORMAT(rsource.Adm_Date,		'yyyy-MM-dd 00:00:00'),'')	AS DRUG_EXPOSURE_START_DATE
    ,isNULL(FORMAT(rsource.Adm_Start_DtTm,	'yyyy-MM-dd HH:mm:ss'),'')	AS DRUG_EXPOSURE_START_DATETIME
    ,isNULL(FORMAT(rsource.Adm_Date,		'yyyy-MM-dd 00:00:00'),'')	AS DRUG_EXPOSURE_END_DATE
    ,isNULL(FORMAT(rsource.Adm_End_DtTm,	'yyyy-MM-dd HH:mm:ss'),'')	AS DRUG_EXPOSURE_END_DATETIME
    ,isNULL(FORMAT(rsource.Adm_End_DtTm,	'yyyy-MM-dd HH:mm:ss'),'')	AS VERBATIM_END_DATETIME
	,'EHR Administered Drugs'				AS DRUG_TYPE_CONCEPT_ID
 	,''										AS STOP_REASON
 	,''										AS REFILLS
   ,isNULL(cast(rsource.Adm_Amount as varchar(30)),'')	 		AS QUANTITY  -- need units to describe quantity (added field)
 	,''										AS DAYS_SUPPLY
 	,''										AS SIG
    ,isNULL(rsource.Adm_Route,'')			AS ROUTE_CONCEPT_ID
	,''										AS LOT_NUMBER
    ,isNULL(rsource.Ordering_provider_id,'')		AS PROVIDER_ID   
 	,isNULL(rsource.ApptDt_PatID,'')		AS VISIT_OCCURRENCE_ID   -- for multiple visits
 	,''										AS VISIT_DETAIL_ID
 	,''										AS DRUG_SOURCE_VALUE
 	,''										AS DRUG_SOURCE_CONCEPT_ID
    ,isNULL(rsource.Adm_Route,'') 			AS ROUTE_SOURCE_VALUE
    ,isNULL(cast(rsource.Adm_Amount as varchar(30)),'')	 		AS DOSE_UNIT_SOURCE_VALUE
 	,isNULL(rsource.Adm_units,'')			AS Adm_Units			-- units associated with Quantity (mg, mcg, ml, etc)
	,isNULL(rsource.drug_label,'')			AS drug_label			-- Mosaiq Drug Label
	,isNULL(rsource.drug_generic_name,'')	AS drug_generic_name	-- Mosaiq Generic Name associated with Drug Label
	,isNULL(rsource.drug_type,'')			AS drug_type			-- Mosaiq Drug Classification
	,isNULL(rsource.RxNorm_CodeValue,'')	AS RxNorm_CodeValue		-- RXNorm Standard Vocab Value -- set in 93% of records
	,isNULL(rsource.RxNorm_CodeType,'')		AS RxNorm_CodeType		-- such as SBD, SCD...
	,isNULL(rsource.CVX_CodeValue,'')		AS CVX_CodeValue		-- CVX Standard Vocab Value Assigned to Drug  -- set in 1% of cases, a fraction of those do not also have RxNorm value
	,isNULL(rsource.CVX_CodeType,'')		AS CVX_CodeType			
	,isNULL(FORMAT(rsource.adm_date, 'yyyy-MM-dd HH:mm:ss'),'') AS modified_DtTm
FROM MosaiqAdmin.dbo.Ref_Patient_Drugs_Administered rsource
--INNER JOIN MosaiqAdmin.dbo.RS21_Patient_List_for_Security_Review pat on rsource.pat_id1 = pat.pat_id1 -- subset 
WHERE rSource.RXA_Set_ID is not null and rsource.Pat_id1 is not null
  and rsource.adm_date >= @fromDate
--WHERE rsource.Adm_Amount > 0   -- drug marked as adminstered but amount altered -- RESEARCH
;