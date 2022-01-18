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
/*
Changes to Script by Debbie Healy 11/9/2021
DEFINITION:	A Visit Occurrence is a day in which a patient was seen at UNMCCC.
			The Patient may have multiple appts during that day (Doctor appt, procedure such as Biopsy or Infusion, Counseling).  
			Each appt will appear as a Visit_Detail.
			Order data will tie back to the Visit Occurrence Record since Orders are done by Appt Date and not by scheduled appointment
			Labs may be returned on a non-appt date -- think about this
			Procedure Data will tie back to the Visit Detail Record 
			Diagnosis will tie back to the Visit Detail Record when extracted from Charge Data, but will take back to Visit Occurrence if extracted from Orders or Medical Tables.
QUESTION: sch.Location AS CARE_SITE_ID,  Do we want this or not? Care_site?
Revised:  ADM.Attending_Md_Id AS PROVIDER_ID, -- changed to Sch.staff_id 
Removed   sch.Location AS CARE_SITE_ID  -- still not sure about this?  location, location, location...
Revised:  5 as Care_Site_ID		 
Removed:  sch.SCH_ID AS PRECEDING_VISIT_DETAIL_ID, -- NOT previous visit_id, just a different version of appt due to operational edit
Added Distinct:  to get one Occurrence record even if patient has multiple appts in one day.  ISSUE:  if patient only has non-medical (administrative) appts, occurrence record will still be created.
Removed:  admin.In_FAC_ID ADMITTED_FROM_SOURCE_VALUE  - not meaninful and only set twice ever
Removed:  CASE WHEN (SELECT TOP 1 ch.IsInPatient FROM Mosaiq.dbo.Charge ch WHERE ch.Pat_ID1 = sch.Pat_ID1 ORDER BY sch.Pat_ID1 ASC) = 0 THEN (SELECT TOP 1 ad.Out_FAC_ID FROM Mosaiq.dbo.Admin ad WHERE ad.Pat_ID1 = sch.Pat_ID1 ORDER BY ad.Pat_ID1 ASC)
 
Changed SELECTION CONDITIONS
	11/12/21 -- added join to MosaiqAdmin.Ref_SchSets which contains all sch_set_ids for valid patients and valid activites since 1/1/2021 and sequences appts in a day by appt time
	Removed  LEFT OUTER JOIN dbo.Admin ADM WITH(NOLOCK) on SCH.Pat_Id1 = ADM.Pat_Id1; Reason: Admin data is static (or slowly/inconsistently changing) and doesn't correlate to the appointment
-- DOES CPT CODE GET ADDED IN HERE?

12/20/21
1) FIN?
2) Set CARE_SITE_ID based on activity and location
/* Radiation Oncology machines will be scheduled to a Machine;  The Facility is 102='UNM CRTC II Radiation Oncology' AND the appt location is the machine name		
SELECT distinct 		
    loc.last_name as machine_name, 		
	loc.machine_type, 	
	case 	
		when loc.machine_type = 1 then 'Accelerator'
		when loc.machine_type = 2 then 'Simulator'
		when loc.machine_type = 3 then 'Kilovoltage'
		when loc.machine_type = 4 then 'HDR'
		when loc.machine_type = 5 then 'Cobalt'
		else 'other'
	end machine_type_desc	
from mosaiq.dbo.staff stf  		
where stf.type = 'location' 		
and machine_type <> 0  --  machine_type = 0 --> 'Not a Machine'		
*/		


EXECUTION CHECK SUCESSFUL -- DAH 01/12/2022
1/10/2022 -- added modified_dtTm  for incremental add
1/10/2022 -- using concatenation of Appt date and Mosaiq Patient ID as visit occurrence identifer
1/12/22 -- handled NULLS
*/
SET NOCOUNT ON;
SELECT "IDENTITY_CONTEXT|SOURCE_PK|VISIT_DETAIL_ID|PERSON_ID|VISIT_DETAIL_CONCEPT_ID|VISIT_DETAIL_START_DATE|VISIT_DETAIL_START_DATETIME|VISIT_DETAIL_END_DATE|VISIT_END_DETAIL_DATETIME|VISIT_DETAIL_TYPE_CONCEPT_ID|PROVIDER_ID|CARE_SITE_ID|VISIT_DETAIL_SOURCE_VALUE|VISIT_DETAIL_SOURCE_CONCEPT_ID|ADMITTED_FROM_SOURCE_VALUE|ADMITTED_FROM_CONCEPT_ID|DISCHARGE_TO_SOURCE_VALUE|DISCHARGE_TO_CONCEPT_ID|PRECEDING_VISIT_DETAIL_ID|VISIT_DETAIL_PARENT_ID|VISIT_OCCURRENCE_ID|modified_dtTm";
SELECT DISTINCT
	  'MOSAIQ MosaiqAdmin Ref_SchSets(OMOP_VISIT_DETAIL)' AS IDENTITY_CONTEXT,
       Ref_SchSets.SCH_SET_ID			AS SOURCE_PK,
       Ref_SchSets.SCH_SET_ID			AS VISIT_DETAIL_ID,
       Ref_SchSets.Pat_ID1				AS PERSON_ID,
       'Outpatient Visit'			AS VISIT_DETAIL_CONCEPT_ID,
       isNULL(FORMAT(Ref_SchSets.Appt_Date,'yyyy-MM-dd'),'') AS VISIT_DETAIL_START_DATE,
       isNULL(FORMAT(Ref_SchSets.appt_DtTm,'yyyy-MM-dd HH:mm:ss') ,'')	AS VISIT_DETAIL_START_DATETIME,
       ''							AS VISIT_DETAIL_END_DATE,
       ''							AS VISIT_END_DETAIL_DATETIME,
       'EHR RECORD'					AS VISIT_DETAIL_TYPE_CONCEPT_ID,
       Ref_SchSets.provider_id		AS PROVIDER_ID,  -- changed from Admin provider
       --sch.Location AS CARE_SITE_ID,  -- still uncertain as to how to set Care_site_id 01/10/22 -- differential between MO/CRTC? 
	   5							AS CARE_SITE_ID,  --?
       Ref_SchSets.Activity			AS VISIT_DETAIL_SOURCE_VALUE,  -- "Lauer New Patient Appt", "8-hour Chemo"  -- gets translated into CPT after Code Capture
       NULL							AS VISIT_DETAIL_SOURCE_CONCEPT_ID,
       NULL							AS ADMITTED_FROM_SOURCE_VALUE, -- changed
       NULL							AS ADMITTED_FROM_CONCEPT_ID,
       NULL							AS DISCHARGE_TO_SOURCE_VALUE,
       NULL							AS DISCHARGE_TO_CONCEPT_ID,
       NULL							AS PRECEDING_VISIT_DETAIL_ID, -- RS21 will programmatically set this
       ''							AS VISIT_DETAIL_PARENT_ID,      
       Ref_SchSets.apptDt_PatID		AS VISIT_OCCURRENCE_ID,  -- sch_set_ID of 1st patient appt of the day 
	   isNULL(FORMAT(Ref_SchSets.run_date,'yyyy-MM-dd HH:mm:ss'),'') as modified_dtTm 
FROM MosaiqAdmin.dbo.Ref_SchSets 
INNER JOIN MosaiqAdmin.dbo.RS21_Patient_List_for_Security_Review pat on Ref_SchSets.pat_id1 = pat.pat_id1 -- subset 
WHERE Ref_SchSets.Pat_ID1 IS NOT NULL  
;




