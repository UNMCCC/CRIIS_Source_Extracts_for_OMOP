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

Non-measurement observations/Assessments 
Questions:  
	1) Mapping:
		If the Response to the Assessment Question is returned in the field Observe.Obs_Choice 
			--> Get response from ObsDef where ObsDef.Obd_id = Observe.Obs_Choice
			--> Then - it gets complicated -- see data 
		If the Response is not in the Observe.Obs_choice field, 
			-- Response will be in the Obs_String or Obs_float field (and will be returned from the function Mosaiq.dbo.fn_GetResults)
			-- A Date response may represented in the float field, but the Results-function will return a date (Obs_results)
	    
	2) Patient Alerts -- pre-2.5 stored in Observations (and so included in this dataset) - post-2.5 in PatAlert (not included in this data set)
			-- Inigo/RS21 should we include Pat Alerts? 
	3)  Need to review Observations that fell into the UNKNOWN Bucket to see whether to include them

Notes:  Need help with mapping to OMOP
confidence level 60% -- non-standard assessments
Addressed NULLS 01/12/22
EXECUTION CHECK SUCCESSFUL -- DAH 01/12/2022  
Ask Inigo about setting unique key
-- 2/10/2022 INIGO:  I decided to present the Assessment Name, Assessment Item names, Sequence # of Item names within an Assessment, and patient responses in a variety of ways to see what works for RS21.
Assessment Name - in QUALIFIER_CONCEPT_ID
Item Label		- in VALUE_AS_CONCEPT_ID, OBS_DESC (field added by us), OBS_RESULTS (field added by us; concatenated response)
Patient response/Answer (obd.label)       - in VALUE_AS_NUMBER for numeric values (eg. Pain-level = 5)
Patient response/Answer (obd.description) - in VALUE_AS_STRING for character values (eg. Pain-level = 'Moderate', 'Able to perform daily activities with rest periods')
Patient response/Answer (obx.obs_float)   - in VALUE_AS DATETIME for date responses (stored in MQ as FLOAT and must be retrieved using MQ fn_GetObsResult() using obx_id as parameter
Sequence # of Assessment Item:			  - in QUALIFIER_SOURCE_VALUE, OBS_LABEL (field added by us; Concatenate Seq # & Item-label)
Sort output file as example of how data would look in assessment form: Assessment-Name, Patient-ID, Observation-DtTm, Label-Seq#
NOTE that only assessment items having responses for a given a given patient are returned.  The entire assessment is NOT represented for each patient.
Debbie
*/

SET NOCOUNT ON;
SELECT 'IDENTITY_CONTEXT|SOURCE_PK|OBSERVATION_ID|PERSON_ID|OBSERVATION_CONCEPT_ID|OBSERVATION_DATE|OBSERVATION_DATETIME|OBSERVATION_TYPE_CONCEPT_ID|VALUE_AS_NUMBER|VALUE_AS_STRING|VALUE_AS_CONCEPT_ID|QUALIFIER_CONCEPT_ID|UNIT_CONCEPT_ID|PROVIDER_ID|VISIT_OCCURRENCE_ID|VISIT_DETAIL_ID|OBSERVATION_SOURCE_VALUE|UNIT_SOURCE_VALUE|QUALIFIER_SOURCE_VALUE|OBSERVATION_EVENT_ID|OBS_EVENT_FIELD_CONCEPT_ID|VALUE_AS_DATETIME|OBS_DESC|OBS_LABEL|OBS_CHOICE_LABEL|OBS_CHOICE_DESC|OBS_RESULTS|Modified_DtTm';
SELECT 'MosaiqAdmin OBSERVE(OMOP_OBSERVATION)' AS IDENTITY_CONTEXT 
 	   ,obx.OBX_ID			AS SOURCE_PK 
 	   ,obx.OBX_ID			AS OBSERVATION_ID
       ,obx.pat_id1			AS PERSON_ID
	   ,''					AS OBSERVATION_CONCEPT_ID
	   ,isNULL(FORMAT(obr.Obs_DtTm,'yyyy-MM-dd 00:00:00'), '')  AS OBSERVATION_DATE
	   ,isNULL(FORMAT(obr.Obs_DtTm,'yyyy-MM-dd HH:mm:ss'), '')  AS OBSERVATION_DATETIME
	   ,'EHR Assessment'		AS OBSERVATION_TYPE_CONCEPT_ID   
	   ,ISNUMERIC(answer.label)			AS VALUE_AS_NUMBER		--ANSWER	to the Assessment Item  
	   ,isNULL(answer.description, '')	AS VALUE_AS_STRING	    --ANSWER	to the Assessment Item
	   ,ltrim(rtrim(Assessment.Item_Label))	AS VALUE_AS_CONCEPT_ID	  -- label of the Assessment Item (Items should appear in the order of View_Seq when presented as part of the Assessment as a whole)
	   ,Assessment.Assessment_Label	AS QUALIFIER_CONCEPT_ID		-- Name of Assessment -- there will be many Item_Labels associated with each Assessment Name 
	   ,''	AS UNIT_CONCEPT_ID
	   ,''	AS PROVIDER_ID  -- providers aren't associated with assessments 
	   ,''	AS VISIT_OCCURRENCE_ID	-- assessment not necessarily taken when patient has an appt (could be over phone; mail; email)
	   ,''	AS VISIT_DETAIL_ID  -- Not linked to a particular appointment
       ,isNULL(answer.description,'')   AS OBSERVATION_SOURCE_VALUE 
	   ,''	AS UNIT_SOURCE_VALUE
	   ,isNULL(Assessment.View_Seq, 0)  AS QUALIFIER_SOURCE_VALUE  
	   ,''	AS OBSERVATION_EVENT_ID
	   ,''	AS OBS_EVENT_FIELD_CONCEPT_ID  
	   ,isNULL(isDate(mosaiq.dbo.fn_GetObsResult(obx_id,1)),'')  AS VALUE_AS_DATETIME -- if response is a date, it is stored as a float and must be retrieved using MQ FN
	   ,ltrim(rtrim(Assessment.Assessment_Label)) AS OBS_DESC -- Using as Assessment-Name 
	   ,cast(Assessment.View_Seq as char(4)) + ': ' + Assessment.Item_Label		AS OBS_LABEL	 -- Seq#/Order of item in assessment plus item label/name 
	   ,Assessment.Item_Label					AS OBS_CHOICE_LABEL		
	   ,Assessment.Item_Desc					AS OBS_CHOICE_DESC	
	    --,ltrim(rtrim(Assessment.Assessment_Label)) + ': ' + ltrim(rtrim(Assessment.Item_Label)) + ' -- ' + ltrim(rtrim(answer.Description)) AS OBS_RESULTS-- can't use...if answer is a date must retrieve with function an it is time-consuming
	   ,''  AS OBS_RESULTS			 
	   ,Format(getDate(),'yyyy-MM-dd HH:mm:ss')			AS Modified_DtTm   -- need to change later 2/9/2022
from Mosaiq.dbo.observe obx
Inner join Mosaiq.dbo.ObsReq obr on obx.obr_set_id = obr.obr_set_id
inner join mosaiq.dbo.ident on obx.pat_id1 = ident.pat_id1
inner join Mosaiq.dbo.ObsDef answer on obx.obs_choice = answer.obd_id  -- to get the patient response/answer to the Assessment item/question
left join MosaiqAdmin.dbo.Ref_ObsDefs_Assessments Assessment on obx.obd_id = Assessment.Item_Obd_Id -- Assessment Item aka "Question"
Inner join MosaiqAdmin.dbo.Ref_Patients on obx.pat_id1 = Ref_Patients.pat_id1 and is_valid = 'Y'
WHERE obx.obx_id is not null 
and year(obr.obs_DtTm) >= '2010'
and (Assessment.Assessment_Label like '%TCP%' or Assessment.Assessment_Label = 'Pain Assessment') 
/*ORDER BY 
Assessment.Assessment_Label 
,obx.Pat_ID1
,obr.Obs_DtTm
,Assessment.View_Seq
*/
 ;
