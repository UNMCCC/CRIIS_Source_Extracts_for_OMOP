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
/* Debbie Healy 11/16/21
Data source is MosaiqAdmin.dbo.Ref_SchSet_Charges which is a table materialized daily and includes data derived from:
	Mosaiq.dbo.Charge
	Mosaiq.dbo.Charge_Audit
	Mosaiq.dbo.Schedule,
	Mosaiq.dbo.CPT
	Only valid visits, patients, and billing/diagnostic CPTs are included
	as of 11/18/21 - only 300 patient subset is included

Questions:
	1) Removed clause code_type = 0 because this value is not set accurately in Mosaiq. 
		  Is the intention is to exclude drug and supply CPT codes?  -- YES
	2) include E&M (Provider Office Visits) CPT codes in here or add them to Visit Detail or both -- INCLUDE IN HERE
	3) quantity?  Mosaiq.dbo.AggregateTotal is not set, but Units_days is -- SEE EXAMPLE
	4) Removing code to get Observation Orders (order_type 1) data:
		 --orders place by doctors to have a patient scheduled for anything:
					follow-up doc appts, labs,  chemo, radiation, scans...
					AND these are not CPT codes, these are the in-house alpha scheduling/ordering activitie
--
Using Charges to get Procedure Codes associated with each appt.  What about visits that don't have a charge?
12/20/21 -- Need to check Charge-Review Dt-- DONE
Addressed NULLS 01/12/22
EXECUTION CHECK SUCCESSFUL -- DAH 01/12/22
01/10/22 -- Need to check into identifying DEVICES

*/
SELECT "IDENTITY_CONTEXT|SOURCE_PK|PROCEDURE_OCCURRENCE_ID|PERSON_ID|PROCEDURE_CONCEPT_ID|PROCEDURE_DATE|PROCEDURE_DATE_DATETIME|PROCEDURE_TYPE_CONCEPT_ID|MODIFIER_CONCEPT_ID|QUANTITY|PROVIDER_ID|VISIT_OCCURRENCE_ID|VISIT_DETAIL_ID|PROCEDURE_SOURCE_VALUE|PROCEDURE_SOURCE_CONCEPT_ID|MODIFIER_SOURCE_VALUE";

SELECT distinct  'Mosaiq Ref_SchSet_Charges(OMOP_PROCEDURE_OCCURRENCE)' AS IDENTITY_CONTEXT
            ,chg.CHG_ID				AS SOURCE_PK
            ,chg.CHG_ID				AS PROCEDURE_OCCURRENCE_ID
            ,chg.Pat_ID1			AS PERSON_ID
            ,isNULL(chg.cpt_code,'')		AS PROCEDURE_CONCEPT_ID   -- AND code_type = 0 (from select top 1)
            ,isNULL(FORMAT(chg.Appt_DtTm,'yyyy-MM-dd HH:mm:ss')	,'')	AS PROCEDURE_DATE  -- should this be date only?
            ,isNULL(FORMAT(chg.Appt_DtTm,'yyyy-MM-dd HH:mm:ss')	,'')	AS PROCEDURE_DATE_DATETIME 
		 	,'EHR RECORD'						AS PROCEDURE_TYPE_CONCEPT_ID
			,isNULL(chg.Modifier1,'')			AS MODIFIER_CONCEPT_ID
			,isNULL(chg.days_units,'')			AS QUANTITY  -- compare with Adm quantity
			,isNULL(chg.Chg_Provider_Id,0)		AS PROVIDER_ID
	        ,isNULL(apptDt_PatID,'')			AS VISIT_OCCURRENCE_ID
	        ,isNULL(sch_set_id	,'')			AS VISIT_DETAIL_ID
			,isNULL(chg.cpt_code,'')			AS PROCEDURE_SOURCE_VALUE -- AND code_type = 0
		 	,''									AS PROCEDURE_SOURCE_CONCEPT_ID 
			,isNULL(chg.Modifier1,'')			AS MODIFIER_SOURCE_VALUE
FROM  MosaiqAdmin.dbo.Ref_SchSet_Charges chg
INNER JOIN MosaiqAdmin.dbo.Ref_CPTs_and_Activities cpt on chg.cpt_code = cpt.cpt_code 
INNER JOIN MosaiqAdmin.dbo.RS21_Patient_List_for_Security_Review pat on chg.pat_id1 = pat.pat_id1 -- subset 
WHERE chg.CHG_ID IS NOT NULL
AND   cpt.is_billing = 'Y'  
AND   cpt.is_drug_cpt = 'N'  -- 12/2/21 -- this will still include DEVICES -- need to figure out how to exclude those
;

