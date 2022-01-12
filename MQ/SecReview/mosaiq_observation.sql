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

EXECUTION CHECK SUCCESSFUL -- DAH 01/10/2022  
Ask Inigo about setting unique key
*/
SET NOCOUNT ON;
SELECT "IDENTITY_CONTEXT|SOURCE_PK|OBSERVATION_ID|PERSON_ID|OBSERVATION_CONCEPT_ID|OBSERVATION_DATE|OBSERVATION_DATETIME|OBSERVATION_TYPE_CONCEPT_ID|VALUE_AS_NUMBER|VALUE_AS_CONCEPT_ID|QUALIFIER_CONCEPT_ID|UNIT_CONCEPT_ID|PROVIDER_ID|VISIT_OCCURRENCE_ID|VISIT_DETAIL_ID|OBSERVATION_SOURCE_VALUE|OBSERVATION_SOURCE_CONCEPT_ID|UNIT_SOURCE_VALUE|QUALIFIER_SOURCE_VALUE|OBSERVATION_EVENT_ID|OBS_EVENT_FIELD_CONCEPT_ID|VALUE_AS_DATETIME|obs_desc|obs_label|obs_choice_label|obs_choice_desc|Obs_Results|obs_choice|obs_string|obs_float";
SELECT 'MOSAIQ ASSESSMENTS(OMOP_OBSERVATION)' AS IDENTITY_CONTEXT 
 	   ,rsource.OBX_ID AS SOURCE_PK 
 	   ,rsource.OBX_ID AS OBSERVATION_ID
       ,rsource.pat_id1 AS PERSON_ID
	   ,NULL AS OBSERVATION_CONCEPT_ID
	   ,FORMAT(rsource.Obs_DtTm,'yyyy-MM-dd 00:00:00') AS OBSERVATION_DATE
	   ,FORMAT(rsource.Obs_DtTm,'yyyy-MM-dd HH:mm:ss') AS OBSERVATION_DATETIME
	   ,'EHR assessment' AS OBSERVATION_TYPE_CONCEPT_ID  -- (?)
	   ,0 AS VALUE_AS_NUMBER		    -- results are a floating point value
	   ,0 AS VALUE_AS_CONCEPT_ID	    -- results are a choice in a drop-list (OBD_ID)
	   ,0 AS QUALIFIER_CONCEPT_ID
	   ,0 AS UNIT_CONCEPT_ID
	   ,0 AS PROVIDER_ID  -- providers don't place orders for assessments 
	   ,rsource.ApptDt_PatID	AS VISIT_OCCURRENCE_ID
	   ,0 AS VISIT_DETAIL_ID  -- Not linked to a particular appointment
       ,0 AS OBSERVATION_SOURCE_VALUE
	   ,0 as OBSERVATION_SOURCE_CONCEPT_ID
	   ,0 AS UNIT_SOURCE_VALUE	   
	   ,0 AS QUALIFIER_SOURCE_VALUE
	   ,0 as OBSERVATION_EVENT_ID
	   ,'MOSAIQ.dbo.Observe' AS OBS_EVENT_FIELD_CONCEPT_ID  -- as populated in original queries (?)
	   ,NULL AS VALUE_AS_DATETIME
	   ,rsource.obs_desc		-- Assessment Question (may be more or less detailed than obs_label)
	   ,rsource.obs_label		-- Assessment Question (may be more or less detailed than obs_label)
	   ,rsource.obs_choice_label -- contains the numeric response if observe.obs_choice is not null --> obs_choice_desc = ObsDef.Description where ObsDef.obd_id = Observe.Obs_choice
	   ,rsource.obs_choice_desc -- mapped textual response if observe.obs_choice is not null --> obs_choice_desc = ObsDef.Description where ObsDef.obd_id = Observe.Obs_choice
	   ,rsource.Obs_Results		-- Response as returned by mosaiq function fn_GetObsResult, if obs_choice is not populated
								-- equals obs_choice_label if obs_choice is populated
	   ,obs_choice -- Observe.obd_id that contains response to the assessment, if answer is not in string field
	   ,obs_string -- Observe.obs_string may contain response if obs_choice is not populated
	   ,obs_float -- Observe.obs_string may contain response if obs_choice is not populated	 (but a date value will be translated by the function)
  FROM MosaiqAdmin.dbo.Ref_Patient_Assessments rsource
  INNER JOIN MosaiqAdmin.dbo.Ref_Patients pat on rsource.pat_id1 = pat.pat_id1 and is_valid = 'Y'
  INNER JOIN MosaiqAdmin.dbo.RS21_Patient_list_for_Security_review subset on rsource.pat_id1 = subset.pat_id1
  ;