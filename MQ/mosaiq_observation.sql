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
   
SELECT '|IDENTITY_CONTEXT|SOURCE_PK|OBSERVATION_ID|PERSON_ID|OBSERVATION_CONCEPT_ID|OBSERVATION_DATE|OBSERVATION_DATETIME|OBSERVATION_TYPE_CONCEPT_ID|VALUE_AS_NUMBER|VALUE_AS_STRING|VALUE_AS_CONCEPT_ID|QUALIFIER_CONCEPT_ID|UNIT_CONCEPT_ID|PROVIDER_ID|VISIT_OCCURRENCE_ID|VISIT_DETAIL_ID|OBSERVATION_SOURCE_VALUE|UNIT_SOURCE_VALUE|QUALIFIER_SOURCE_VALUE|OBSERVATION_EVENT_ID|OBS_EVENT_FIELD_CONCEPT_ID|VALUE_AS_DATETIME|ASSESSMENT_NAME|ASSESSMENT_SEQ|ASSESSMENT_QUESTION|ASSESSMENT_ANSWER|Modified_DtTm';


SELECT  'MosaiqAdmin OBSERVE(OMOP_OBSERVATION)' AS IDENTITY_CONTEXT 
 	   ,Assessment.OBX_ID	AS SOURCE_PK 
 	   ,Assessment.OBX_ID	AS OBSERVATION_ID
       ,Assessment.pat_id1	AS PERSON_ID
	   ,''					AS OBSERVATION_CONCEPT_ID
	   ,isNULL(FORMAT(Assessment.Obs_DtTm,'yyyy-MM-dd 00:00:00'), '')  AS OBSERVATION_DATE
	   ,isNULL(FORMAT(Assessment.Obs_DtTm,'yyyy-MM-dd HH:mm:ss'), '')  AS OBSERVATION_DATETIME
	   ,'EHR Assessment'	AS OBSERVATION_TYPE_CONCEPT_ID   
	   ,REPLACE(iSNULL(Assessment.Value_AS_NUMBER,''),0,'')	AS VALUE_AS_NUMBER		--ANSWER	to the Assessment Item  
	   ,isNULL(Assessment.VALUE_AS_STRING,'')	AS VALUE_AS_STRING	    --ANSWER	to the Assessment Item
	   ,''  AS VALUE_AS_CONCEPT_ID	-- Question  
	   ,''  AS QUALIFIER_CONCEPT_ID		-- Name of Assessment -- there will be many Item_Labels associated with each Assessment Name 
	   ,''	AS UNIT_CONCEPT_ID
	   ,''	AS PROVIDER_ID  -- providers aren't associated with assessments 
	   ,''	AS VISIT_OCCURRENCE_ID	-- assessment not necessarily taken when patient has an appt (could be over phone; mail; email)
	   ,''	AS VISIT_DETAIL_ID  -- Not linked to a particular appointment
       ,''  AS OBSERVATION_SOURCE_VALUE 
	   ,''	AS UNIT_SOURCE_VALUE
	   ,''  AS QUALIFIER_SOURCE_VALUE   -- label of the Assessment Item (Items should appear in the order of View_Seq when presented as part of the Assessment as a whole)
	   ,''	AS OBSERVATION_EVENT_ID
	   ,''	AS OBS_EVENT_FIELD_CONCEPT_ID  
	   ,''  AS VALUE_AS_DATETIME -- if response is a date, it is stored as a float and must be retrieved using MQ FN
	   ,isNULL(Assessment.Assessment_Label,'')	AS ASSESSMENT_NAME
	   ,isNULL(Assessment.View_Seq,'')	 AS ASSESSMENT_SEQ   -- Seq#/Order of QUESTIONS 
	   ,isNULL(Assessment.Item_label,'') AS ASSESSMENT_QUESTION		
	   ,Assessment.VALUE_AS_STRING		 AS ASSESSMENT_ANSWER	
	   ,isNULL(Format(Assessment.Modified_DtTm,'yyyy-MM-dd HH:mm:ss'),'') AS Modified_DtTm  
from MosaiqAdmin.dbo.Ref_Observation_Assessments Assessment
WHERE  year(Assessment.obs_DtTm) = '2019'
and (Assessment.Assessment_Label like '%TCP%' or Assessment.Assessment_Label = 'Pain Assessment') 
and Assessment.Modified_DtTm >= @fromDate
ORDER BY 
Assessment.Assessment_Label 
,Assessment.Pat_ID1
,Assessment.Obs_DtTm
,Assessment.View_Seq
 ;

