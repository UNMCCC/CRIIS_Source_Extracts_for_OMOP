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

10) Use date format 'YYYY-MM-DD HH24:MI:SS' per Kevin 11/9/2021 

11) Include record modified Date-Time at end of field list

*/
/* visit occurrence
Changes to Script by Debbie Healy 11/8/2021
DEFINITION:	A Visit Occurrence is a day in which a patient was seen at UNMCCC.
			The Patient may have multiple appts during that day (Doctor appt, procedure such as Biopsy or Infusion, Counseling).  
			Each appt will appear as a Visit_Detail.
			Order data will tie back to the Visit Occurrence Record since Orders are done by Appt Date and not by scheduled appointment
			Procedure Data will tie back to the Visit Detail Record 
			Diagnosis will tie back to the Visit Detail Record when extracted from Charge Data, but will take back to Visit Occurrence if extracted from Orders or Medical Tables.
Question:  Could this be built from the Visit Details extract since it is distilled from that data?

Question: EHR? data from the schedule - which is more of an intention than of what will happen with patient.  
		Example:  a patient may be scheduled for Chemo but may not receive it due to current condition.  However, the visit is still marked as such as long as vitals have been taken and a provider saw the patient.
		Example:  appts are not always statused correctly. Slight concern that a charge may be created for a non-statused schedule record (Coding is supposed to go back an status the appt but...?)
Question:  Sch_set_id vs. Sch_id? Sch_Set_ID represents the appt slip = Non-changing schedule identifer associated with many sch_ids.  
			A sch_id reflects each operational change to an appt slip (for example, statusing)
			Distinct SCH_SET_ID allows for Unique Occurrence record; Use of Sch_id will introduce duplicates if data is added based on edit-dtTm instead of a re-write
11/9/21 -- meeting -- decided to use sch_set_id -- check later to see if this is ok and we don't need sch_id

Removed  ,sch.Location AS CARE_SITE_ID  -- still not sure about this?  location, location, location...
Revised: ,5 as Care_Site_ID		  
Removed: ,sch.Staff_ID AS PROVIDER_ID; Reason:  Provider and Activity are on the Detail Record associated with each actual appt

Removed: ,sch.App_DtTm AS VISIT_START_DATETIME -- don't populate time for Occurrence but only in detail?
Added Distinct:  to get one Occurrence record even if patient has multiple appts in one day.  ISSUE:  if patient only has non-medical (administrative) appts, occurrence record will still be created.
Removed:  ,admin.In_FAC_ID ADMITTED_FROM_SOURCE_VALUE  - not meaninful and only set twice ever
Removed:  ,CASE WHEN (SELECT TOP 1 ch.IsInPatient FROM Mosaiq.dbo.Charge ch WHERE ch.Pat_ID1 = sch.Pat_ID1 ORDER BY sch.Pat_ID1 ASC) = 0 THEN (SELECT TOP 1 ad.Out_FAC_ID FROM Mosaiq.dbo.Admin ad WHERE ad.Pat_ID1 = sch.Pat_ID1 ORDER BY ad.Pat_ID1 ASC)

Changed SELECTION CONDITIONS
	Added INNER JOIN MosaiqAdmin.check_pat_id1 on sch.pat_id = check_pat_id1.pat_id1 and check_pat_id1.is_valid = 'Y'  -- to removed sample/test patients.  Looking for place in Mosaiq to store flag.
	Removed  LEFT OUTER JOIN dbo.Admin ADM WITH(NOLOCK) on SCH.Pat_Id1 = ADM.Pat_Id1; Reason: Admin data is static (or slowly/inconsistently changing) and doesn't correlate to the appointment
Changed: WHERE Conditions:
	Added: and sch.SchStatus_Hist_SD in (' C', ' D', 'E', 'FC', 'FD', 'FE','OC', 'OD', 'OE',  'SC', 'SD','SE')

 NULL for provider on occurrence record, but set this on detail record

 11/10/2021
 Revised: Extraction Start Date per Trux 2010-01-01
11/12/2021:  Changed Selection critieria to join schedule with MosaiqAdmin.dbo.Ref_SchSets which contains all sch_set_ids for valid patients and valid activities since 01/01/2010
CONFIDENCE LEVEL:  MEDIUM

EXECUTION CHECK SUCESSFUL 01/10/2022
1/10/2022 -- added modified_dtTm  for incremental add
1/10/2022 -- using concatenation of Appt date and Mosaiq Patient ID as visit occurrence identifer
2/18/2022 -- added max(run_date) and group by to get distinct modified_DtTm
2/18/2022 -- NOTE Visit_OCCURRENCE is really a DISTINCT apptDt_PatID from Visit_DETAIL -- this extract shouldn't even be required
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
   
SELECT 'IDENTITY_CONTEXT|SOURCE_PK|VISIT_OCCURRENCE_ID|PERSON_ID|VISIT_CONCEPT_ID|VISIT_START_DATE|VISIT_START_DATETIME|VISIT_END_DATE|VISIT_END_DATETIME|VISIT_TYPE_CONCEPT_ID|PROVIDER_ID|CARE_SITE_ID|VISIT_SOURCE_VALUE|VISIT_SOURCE_CONCEPT_ID|ADMITTED_FROM_CONCEPT_ID|ADMITTED_FROM_SOURCE_VALUE|DISCHARGE_TO_CONCEPT_ID|DISCHARGE_TO_SOURCE_VALUE|PRECEDING_VISIT_OCCURRENCE_ID|modified_dtTm';
SELECT	DISTINCT
		'MosaiqAdmin Ref_SchSets (OMOP_VISIT_OCCURRENCE)'	AS IDENTITY_CONTEXT
	   ,rsource.apptDt_PatID	AS SOURCE_PK					-- will have multiple Visit_Details for each apptDt_PatID
 	   ,rsource.apptDt_PatID	AS VISIT_OCCURRENCE_ID  
       ,rsource.Pat_ID1			AS PERSON_ID
	   ,'Outpatient Visit'		AS VISIT_CONCEPT_ID				 
	   ,isNULL(FORMAT(rsource.appt_date,'yyyy-MM-dd 00:00:00'),'')	AS VISIT_START_DATE		-- Date only 
	   ,''						AS VISIT_START_DATETIME			-- DateTime will be on the Visit Detail records
	   ,''						AS VISIT_END_DATE
	   ,''						AS VISIT_END_DATETIME
	   ,'EHR RECORD'			AS VISIT_TYPE_CONCEPT_ID	
	   ,''						AS PROVIDER_ID					-- Provider will be on the Visit Detail record
	   ,5						AS CARE_SITE_ID		
	   ,rsource.apptDt_PatID	AS VISIT_SOURCE_VALUE
	   ,''						AS VISIT_SOURCE_CONCEPT_ID
	   ,''						AS ADMITTED_FROM_CONCEPT_ID
 	   ,''						AS ADMITTED_FROM_SOURCE_VALUE
	   ,''						AS DISCHARGE_TO_CONCEPT_ID
	   ,''						AS DISCHARGE_TO_SOURCE_VALUE
	   ,''						AS PRECEDING_VISIT_OCCURRENCE_ID  -- Not tracked in MQ, but RS21 will calculate
	   ,MAX(isNULL(FORMAT(rsource.appt_date,'yyyy-MM-dd HH:mm:ss'),'')) as modified_DtTm
FROM MosaiqAdmin.dbo.Ref_SchSets  rsource -- need to fix dups in SP that creates this
--INNER JOIN MosaiqAdmin.dbo.RS21_Patient_List_for_Security_Review pat on Ref_SchSets.pat_id1 = pat.pat_id1 -- subset 
WHERE   rsource.Pat_ID1 IS NOT NULL
and rsource.appt_date >= @fromDate
GROUP BY rsource.apptDt_PatID, rsource.Pat_ID1, rsource.appt_date
;
