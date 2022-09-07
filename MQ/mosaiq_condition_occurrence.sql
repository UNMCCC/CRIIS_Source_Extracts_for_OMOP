
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
1/10/2022 -- using concatenation of Appt date and Mosaiq Patient ID as  source_PK, Condition_Occurrence_ID  
1/20/2022 added Modified DtTm
3/17/2022 -- Since there are multiple diagnosis codes per patient per appt-dt, needed to add dx key (tpg_id) to create a unique key...
	-- WOULD IT BE BETTER TO CREATE A PK?
-- Cast tpg_id as char(5) -- current max tpg_id is 40111
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
   
SELECT 'IDENTITY_CONTEXT|SOURCE_PK|CONDITION_OCCURRENCE_ID|PERSON_ID|CONDITION_CONCEPT_ID|CONDITION_START_DATE|CONDITION_START_DATETIME|CONDITION_END_DATE|CONDITION_END_DATETIME|CONDITION_TYPE_CONCEPT_ID|CONDITION_STATUS_CONCEPT_ID|STOP_REASON|PROVIDER_ID|VISIT_OCCURRENCE_ID|VISIT_DETAIL_ID|CONDITION_SOURCE_VALUE|CONDITION_SOURCE_CONCEPT_ID|CONDITION_STATUS_SOURCE_VALUE|Modified_DtTm';
SELECT DISTINCT 'MosaiqAdmin Ref_Patient_Diagnoses(OMOP_CONDITION_OCCURRENCE)' AS IDENTITY_CONTEXT  -- 1st Diag on charge
  		    ,rtrim(DX.apptDt_PatID) + '-' + cast(DX.tpg_id  as char(5))		AS SOURCE_PK  -- need to populate with GROUP_CHG_ID ?
            ,rtrim(DX.apptDt_PatID) + '-' + cast(DX.tpg_id  as char(5))		AS CONDITION_OCCURRENCE_ID  
            ,DX.Pat_ID1						AS PERSON_ID
			,DX.Diag_Code					AS CONDITION_CONCEPT_ID  -- DD has this typed as Number(22) -- but Diag isn't numeric -- diag was used in origional code
			,isNULL(FORMAT(DX.appt_date,'yyyy-MM-dd 00:00:00'),'')	AS CONDITION_START_DATE
			,isNULL(FORMAT(DX.appt_date,'yyyy-MM-dd HH:mm:ss'),'')  AS CONDITION_START_DATETIME   
			,''								AS CONDITION_END_DATE
			,''								AS CONDITION_END_DATETIME
		    ,''								AS CONDITION_TYPE_CONCEPT_ID -- what should this be? --original query set it to Account-status -- not sure what that is
            ,''								AS CONDITION_STATUS_CONCEPT_ID -- set to top 1 schedule activity in origional query
			,''								AS STOP_REASON 
            ,''								AS PROVIDER_ID    
	        ,isNULL(DX.apptDt_PatID	,'')	AS VISIT_OCCURRENCE_ID 
            ,''						AS VISIT_DETAIL_ID				
			,DX.Diag_Code				AS CONDITION_SOURCE_VALUE		-- should this be ICD-code 
            ,'Outpatient'				AS CONDITION_SOURCE_CONCEPT_ID  -- why is this outpatient --see DD
            ,''						AS CONDITION_STATUS_SOURCE_VALUE  
			,isNULL(FORMAT(appt_date, 'yyyy-MM-dd HH:mm:ss'),'') AS Modified_DtTm
FROM MosaiqAdmin.dbo.Ref_Patient_Diagnoses DX	
WHERE DX.Diag_Code is not null
  AND DX.Diag_Code  <> ' '
  and appt_date >= @fromDate
;
