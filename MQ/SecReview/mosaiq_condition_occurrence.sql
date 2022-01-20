
-- mosaiq_condition_occurrence.sql
/*
Debbie Healy 10/14/21
What is the purpose of this table -- gets activity, diagnosis, appt dt, and FIN from Charge/Schedule
This may return multiple records for each sch_ID  
** Example sch_id = 7353914 is an infusion appt
** This returns 11 charge rows for various chemo administration  cpt-codes and Drug J-codes
** Plus each charge set has a "header" record associated with the scheduling activity ("4 Hr Infus") in this case.
** Do we just want to pick up the "header" record?  But what to do if sch_id/header disagree?

Debbie Healy 10/11/21

12/2/21 -- changed to extract data from  MosaiqAdmin.dbo.Ref_Patient_Diagnoses 

-- if not DX for the day -- perhaps get Order DX (?)

CONFIDENCE LEVEL -- MEDIUM

Addressed NULLS 01/12/2022
EXECUTION CHECK SUCCESSFUL 01/20/2022
1/10/2022 -- using concatenation of Appt date and Mosaiq Patient ID as  source_PK, Condition_OCcurrence_ID  
1/20/2022 added Modified DtTm
*/
SET NOCOUNT ON;
SELECT "IDENTITY_CONTEXT|SOURCE_PK|CONDITION_OCCURRENCE_ID|PERSON_ID|CONDITION_CONCEPT_ID|CONDITION_START_DATE|CONDITION_START_DATETIME|CONDITION_END_DATE|CONDITION_END_DATETIME|CONDITION_TYPE_CONCEPT_ID|CONDITION_STATUS_CONCEPT_ID|STOP_REASON|PROVIDER_ID|VISIT_OCCURRENCE_ID|VISIT_DETAIL_ID|CONDITION_SOURCE_VALUE|CONDITION_SOURCE_CONCEPT_ID|CONDITION_STATUS_SOURCE_VALUE";
SELECT DISTINCT   'MosaiqAdmin Ref_Patient_Diagnoses(OMOP_CONDITION_OCCURRENCE)' AS IDENTITY_CONTEXT  -- 1st Diag on charge
  		    ,DX.apptDt_PatID			AS SOURCE_PK  -- need to populate with GROUP_CHG_ID ?
            ,DX.apptDt_PatID			AS CONDITION_OCCURRENCE_ID  
            ,DX.Pat_ID1					AS PERSON_ID
			,DX.Diag_Code				AS CONDITION_CONCEPT_ID  -- DD has this typed as Number(22) -- but Diag isn't numeric -- diag was used in origional code
			,FORMAT(DX.appt_date,'yyyy-MM-dd 00:00:00') AS CONDITION_START_DATE
			,FORMAT(DX.appt_date,'yyyy-MM-dd HH:mm:ss') AS CONDITION_START_DATETIME   
			,NULL						AS CONDITION_END_DATE
			,NULL						AS CONDITION_END_DATETIME
		    ,NULL						AS CONDITION_TYPE_CONCEPT_ID -- what should this be? --original query set it to Account-status -- not sure what that is
            ,NULL						AS CONDITION_STATUS_CONCEPT_ID -- set to top 1 schedule activity in origional query
			,NULL						AS STOP_REASON 
            ,NULL						AS PROVIDER_ID    
	        ,DX.apptDt_PatID			AS VISIT_OCCURRENCE_ID 
            ,NULL						AS VISIT_DETAIL_ID				
			,DX.Diag_Code				AS CONDITION_SOURCE_VALUE		-- should this be ICD-code 
            ,'Outpatient'				AS CONDITION_SOURCE_CONCEPT_ID  -- why is this outpatient --see DD
            ,NULL						AS CONDITION_STATUS_SOURCE_VALUE  
			,FORMAT(run_date, 'yyyy-MM-dd HH:mm:ss') AS Modified_DtTm
FROM MosaiqAdmin.dbo.Ref_Patient_Diagnoses DX	
INNER JOIN MosaiqAdmin.dbo.RS21_Patient_List_for_Security_Review pat on DX.pat_id1 = pat.pat_id1 -- subset 
WHERE DX.Diag_Code is not null
  AND DX.Diag_Code  <> ' '
;
